import os
import re
import subprocess
import tempfile
import polars as pl
import oracledb
from typing import Literal, List, Dict, Any

DBWriteMode = Literal["append", "replace", "upsert"]

# ── Polars → Oracle Type Mapping ──
POLARS_TO_ORACLE: dict[type, str] = {
    pl.Int8: "NUMBER(3)",
    pl.Int16: "NUMBER(5)",
    pl.Int32: "NUMBER(10)",
    pl.Int64: "NUMBER(19)",
    pl.Int128: "NUMBER(38)",
    pl.UInt8: "NUMBER(3)",
    pl.UInt16: "NUMBER(5)",
    pl.UInt32: "NUMBER(10)",
    pl.UInt64: "NUMBER(20)",
    pl.Float32: "BINARY_FLOAT",
    pl.Float64: "BINARY_DOUBLE",
    pl.Date: "DATE",
    pl.Time: "TIMESTAMP",
    pl.Datetime: "TIMESTAMP",
    pl.Duration: "INTERVAL DAY TO SECOND",
    pl.String: "VARCHAR2(255)",
    pl.Categorical: "VARCHAR2(255)",
    pl.Utf8: "VARCHAR2(255)",
    pl.Binary: "BLOB",
    pl.Boolean: "NUMBER(1)",
    pl.Null: "VARCHAR2(255)",
}


def _infer_dtype(dtype: pl.DataType) -> str:
    """Map Polars dtype ke Oracle SQL type."""
    # Handle Decimal
    if isinstance(dtype, pl.Decimal):
        p = dtype.precision or 18
        s = dtype.scale or 0
        return f"NUMBER({p},{s})"

    datatype = POLARS_TO_ORACLE.get(type(dtype))
    return datatype if datatype is not None else "VARCHAR2(255)"


def _is_table_exists(conn, table_name: str) -> bool:
    """Cek apakah tabel sudah ada di Oracle."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM user_tables
        WHERE table_name = :1
    """, [table_name.upper()])
    result = cursor.fetchone() is not None
    cursor.close()
    return result


def _ensure_table_exists(conn, df: pl.DataFrame, table_name: str,
                         key_columns: list = None, dtype_overrides: dict = None):
    """Auto-create tabel di Oracle jika belum ada."""
    if _is_table_exists(conn, table_name):
        return

    print(f"[DBDF] Table '{table_name}' not found. Auto-creating...")

    dtype_overrides = dtype_overrides or {}
    cols_def = []
    for col_name, dtype in df.schema.items():
        ora_type = dtype_overrides.get(col_name) or _infer_dtype(dtype)
        cols_def.append(f'{col_name} {ora_type}')
    cols_sql = ", ".join(cols_def)

    # Primary key
    pk_sql = ""
    if key_columns:
        if isinstance(key_columns, str):
            key_columns = [key_columns]
        pk_cols = ", ".join(key_columns)
        pk_sql = f", PRIMARY KEY ({pk_cols})"

    query = f'CREATE TABLE {table_name} ({cols_sql}{pk_sql})'
    print(f"[DBDF] Executing: {query}")
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()



def write_database(
    uri: str,
    df: pl.DataFrame,
    table_name: str,
    mode: DBWriteMode,
    chunk_size: int = 0,
    key_columns: List[str] = None,
    dtype_overrides: dict = None,
):
    """
    Main orchestration function.

    Responsibilities:
    - Validate input arguments.
    - Split the DataFrame into chunks (via _data_generator) to bound memory usage.
    - For each chunk: convert to CSV, generate a SQL*Loader control file, and load
      it into the staging table (first chunk truncates+inserts, subsequent chunks append).
    - Aggregate loading statistics across all chunks.
    - If any rows were loaded, move data from staging to the target table
      (using key_columns to MERGE when mode is "upsert").
    - Clean up all temporary files.
    - Return the aggregated loading result and statistics.

    key_columns: required when mode="upsert". List of column names that uniquely
    identify a row (typically the primary/unique key), used to decide whether a
    staging row matches an existing target row (UPDATE) or is new (INSERT).
    """

    if mode not in ["append", "replace", "upsert"]:
        raise ValueError(f"Invalid mode: {mode}")

    if mode == "upsert":
        if not key_columns:
            raise ValueError("key_columns is required when mode='upsert'")
        invalid_keys = set(key_columns) - set(df.columns)
        if invalid_keys:
            raise ValueError(f"key_columns not found in dataframe: {invalid_keys}")

    if df.is_empty():
        return {"status": "skipped", "reason": "DataFrame is empty"}

    staging_table = f"{table_name}_staging"
    conn = oracledb.connect(uri)
    cursor = conn.cursor()

    # DDL: Auto-create tabel target kalau belum ada
    _ensure_table_exists(conn, df, table_name, key_columns, dtype_overrides)


    total_stats = {
        "loaded_rows": 0,
        "rejected_rows": 0,
        "elapsed_time": "00:00:00",
        "chunks_processed": 0,
    }
    chunk_elapsed_times = []

    try:
        try:
            cursor.execute(f"TRUNCATE TABLE {staging_table}")
        except oracledb.DatabaseError:
            # Jika tabel staging belum ada, buat struktur tiruannya dari tabel target
            cursor.execute(f"CREATE TABLE {staging_table} AS SELECT * FROM {table_name} WHERE 1=0")

        for idx, chunk in enumerate(_data_generator(df, chunk_size)):
            is_append = idx > 0  # chunk pertama INSERT (staging kosong), sisanya APPEND
            chunk_files = []

            try:
                csv_path = dataframe_to_csv(chunk)
                chunk_files.append(csv_path)

                ctl_path = generate_control_file(chunk, staging_table, csv_path, append=is_append)
                chunk_files.append(ctl_path)

                loader_info = execute_sql_loader(uri, ctl_path)
                chunk_files.extend([loader_info["log_file"], loader_info["bad_file"], loader_info["discard_file"]])

                chunk_stats = parse_loader_log(loader_info["log_file"])

                total_stats["loaded_rows"] += chunk_stats.get("loaded_rows", 0)
                total_stats["rejected_rows"] += chunk_stats.get("rejected_rows", 0)
                total_stats["chunks_processed"] += 1
                chunk_elapsed_times.append(chunk_stats.get("elapsed_time", "00:00:00"))
            finally:
                # Hapus file chunk ini segera, jangan tunggu sampai semua chunk selesai
                # (atau sampai exception menggelembung ke atas). Untuk data jutaan baris
                # yang terpecah jadi banyak chunk, menunda cleanup bisa membuat puluhan
                # file CSV besar menumpuk di disk sekaligus.
                cleanup_temp_files(chunk_files)

        total_stats["elapsed_time_per_chunk"] = chunk_elapsed_times

        # Merge ke target sekali saja setelah semua chunk selesai dimuat ke staging
        if total_stats["loaded_rows"] > 0:
            merge_to_target(cursor, staging_table, table_name, mode, df.columns, key_columns)
            conn.commit()
            total_stats["status"] = "success"
        else:
            conn.rollback()
            total_stats["status"] = "failed_or_empty"

        return total_stats

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database write failed: {e}")
    finally:
        cursor.close()
        conn.close()


def dataframe_to_csv(df: pl.DataFrame):
    """
    Convert a Polars DataFrame (or a chunk of it) into a temporary CSV file.

    Responsibilities:
    - Export the dataframe to a temporary CSV.
    - Preserve column order.
    - Configure delimiter and null representation.
    - Use a fixed, explicit datetime format so it matches the format mask
      generated in generate_control_file() (avoids relying on Oracle's
      NLS_DATE_FORMAT / NLS_TIMESTAMP_FORMAT session defaults).
    - Return the generated CSV file path.
    """

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp_file:
        temp_path = temp_file.name

    df.write_csv(
        temp_path,
        separator=",",
        null_value="",
        datetime_format="%Y-%m-%d %H:%M:%S",
        date_format="%Y-%m-%d",
    )

    return temp_path


def generate_control_file(df, staging_table, csv_path, append: bool = False):
    """
    Generate a SQL*Loader control (.ctl) file for a single chunk.

    Responsibilities:
    - Read dataframe schema.
    - Map dataframe columns to Oracle columns, adding explicit DATE/TIMESTAMP
      format masks for Polars Date/Datetime columns so sqlldr doesn't depend
      on Oracle's session NLS format (which may not match what Polars wrote).
    - Configure SQL*Loader options.
    - Use INTO TABLE for the first chunk (staging table is empty/truncated) and
      APPEND INTO TABLE for subsequent chunks, so multiple loads accumulate
      correctly in the same staging table instead of failing on a non-empty table.
    - Return the generated control file path.
    """

    column_defs = []
    for col, dtype in zip(df.columns, df.dtypes):
        if dtype == pl.Date:
            column_defs.append(f'{col} DATE "YYYY-MM-DD"')
        elif isinstance(dtype, pl.Datetime) or dtype == pl.Datetime:
            column_defs.append(f'{col} TIMESTAMP "YYYY-MM-DD HH24:MI:SS"')
        else:
            column_defs.append(col)

    columns_str = ",\n    ".join(column_defs)
    normalized_csv_path = csv_path.replace('\\', '/')
    load_clause = "APPEND INTO TABLE" if append else "INTO TABLE"

    ctl_content = f"""LOAD DATA
        INFILE '{normalized_csv_path}'
        {load_clause} {staging_table}
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        TRAILING NULLCOLS
        (
            {columns_str}
        )
    """

    with tempfile.NamedTemporaryFile(mode="w", suffix=".ctl", delete=False) as temp_file:
        temp_file.write(ctl_content)
        return temp_file.name


def execute_sql_loader(uri, control_file):
    """
    Execute SQL*Loader.

    Responsibilities:
    - Build the sqlldr command.
    - Execute SQL*Loader using subprocess.
    - Generate log, bad, and discard files.
    - Return execution information.
    """
    base_path = control_file.rsplit('.', 1)[0]
    log_file = f"{base_path}.log"
    bad_file = f"{base_path}.bad"
    discard_file = f"{base_path}.dsc"

    cmd = [
        "sqlldr",
        f"userid={uri}",
        f"control={control_file}",
        f"log={log_file}",
        f"bad={bad_file}",
        f"discard={discard_file}",
        "errors=1000"  # Toleransi error baris sebelum dibatalkan
    ]

    subprocess.run(cmd, capture_output=True, text=True)

    return {
        "log_file": log_file,
        "bad_file": bad_file,
        "discard_file": discard_file
    }


def parse_loader_log(log_file):
    """
    Parse SQL*Loader execution log.

    Responsibilities:
    - Read the SQL*Loader log file.
    - Extract loaded row count.
    - Extract rejected row count.
    - Extract elapsed execution time.
    - Return a structured loading result for this chunk.
    """

    stats = {"loaded_rows": 0, "rejected_rows": 0, "elapsed_time": "00:00:00"}

    if not os.path.exists(log_file):
        return stats

    with open(log_file, 'r') as f:
        log_content = f.read()

    load_match = re.search(r'(\d+)\s+Rows\s+successfully\s+loaded.', log_content)
    if load_match:
        stats["loaded_rows"] = int(load_match.group(1))

    reject_match = re.search(r'(\d+)\s+Rows\s+not\s+loaded\s+due\s+to\s+data\s+errors.', log_content)
    if reject_match:
        stats["rejected_rows"] = int(reject_match.group(1))

    time_match = re.search(r'Elapsed time was:\s+(.*)', log_content)
    if time_match:
        stats["elapsed_time"] = time_match.group(1).strip()

    return stats


def merge_to_target(
    cursor,
    staging_table: str,
    target_table: str,
    mode: DBWriteMode,
    columns: List[str],
    key_columns: List[str] = None,
):
    if mode == "replace":
        cursor.execute(f"TRUNCATE TABLE {target_table}")
        cursor.execute(f"INSERT INTO {target_table} SELECT * FROM {staging_table}")

    elif mode == "append":
        cursor.execute(f"INSERT INTO {target_table} SELECT * FROM {staging_table}")

    elif mode == "upsert":
        if not key_columns:
            raise ValueError("key_columns is required for upsert mode")

        non_key_columns = [c for c in columns if c not in key_columns]

        on_clause = " AND ".join(f"tgt.{col} = src.{col}" for col in key_columns)
        insert_cols = ", ".join(columns)
        insert_values = ", ".join(f"src.{col}" for col in columns)

        merge_sql = f"MERGE INTO {target_table} tgt\n"
        merge_sql += f"USING {staging_table} src\n"
        merge_sql += f"ON ({on_clause})\n"

        # Hanya sertakan klausa UPDATE jika ada kolom non-key untuk diperbarui.
        # Jika semua kolom adalah key, tidak ada apa pun yang perlu di-update saat match.
        if non_key_columns:
            update_clause = ", ".join(f"tgt.{col} = src.{col}" for col in non_key_columns)
            merge_sql += f"WHEN MATCHED THEN\n    UPDATE SET {update_clause}\n"

        merge_sql += "WHEN NOT MATCHED THEN\n"
        merge_sql += f"    INSERT ({insert_cols})\n"
        merge_sql += f"    VALUES ({insert_values})"

        cursor.execute(merge_sql)


def cleanup_temp_files(files: List[str]):
    for file_path in files:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass


def _data_generator(df: pl.DataFrame, chunk_size: int):
    """
    Generator yang memecah dataframe menjadi beberapa chunk.

    Responsibilities:
    - Memecah df menjadi bagian-bagian berukuran chunk_size baris.
    - Yield setiap chunk satu per satu (lazy, hemat memori).
    - Jika chunk_size tidak diberikan atau >= jumlah baris df,
      yield seluruh df sebagai satu chunk saja.
    """
    if not chunk_size or chunk_size >= df.height:
        yield df
        return

    for offset in range(0, df.height, chunk_size):
        yield df.slice(offset, chunk_size)
