import time
import polars as pl
import oracledb
from typing import Literal, List, Dict, Any, Optional

DBWriteMode = Literal["append", "replace", "upsert"]


def write_database(
    uri: str,
    df: pl.DataFrame,
    schema_name: str,
    table_name: str,
    mode: str, 
    chunk_size: int = 0,
    key_columns: List[str] = None,
):
    function_start = time.perf_counter() 

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

    staging_table_name = f"{table_name}_staging"
    qualified_target = f"{schema_name}.{table_name}"
    qualified_staging = f"{schema_name}.{staging_table_name}"

    conn = oracledb.connect(uri)
    cursor = conn.cursor()

    total_stats = {
        "status": "in_progress",
        "loaded_rows": 0,
        "rejected_rows": 0,
        "chunks_processed": 0,
        "elapsed_time_seconds": 0.0,
        "chunk_loading_time_seconds": 0.0,
        "merge_time_seconds": 0.0,
    }
    chunk_process_time = []

    try:
        try:
            cursor.execute(f"TRUNCATE TABLE {qualified_staging}")
        except oracledb.DatabaseError:
            cursor.execute(
                f"CREATE TABLE {qualified_staging} AS SELECT * FROM {qualified_target} WHERE 1=0"
            )
            conn.commit()

        chunk_loading_start = time.perf_counter()

        # PROSES CHUNKING LANGSUNG DENGAN POLARS
        for idx, chunk in enumerate(_data_generator(df, chunk_size)):
            start_time = time.perf_counter()

            loaded, rejected = direct_path_insert(
                conn, chunk, schema_name, staging_table_name
            )

            conn.commit() 

            total_stats["loaded_rows"] += loaded
            total_stats["rejected_rows"] += rejected
            total_stats["chunks_processed"] += 1

            end_time = time.perf_counter()
            chunk_process_time.append(round(end_time - start_time, 4))

        # Simpan total waktu chunking
        chunk_loading_time = time.perf_counter() - chunk_loading_start
        total_stats["chunk_loading_time_seconds"] = round(chunk_loading_time, 4)
        total_stats["chunk_process_time"] = chunk_process_time

        merge_start = time.perf_counter() 
        
        if total_stats["loaded_rows"] > 0:
            merge_to_target(
                cursor, qualified_staging, qualified_target, mode, df.columns, key_columns
            )
            conn.commit()

            # Simpan durasi merge
            merge_time = time.perf_counter() - merge_start
            total_stats["merge_time_seconds"] = round(merge_time, 4)
            total_stats["status"] = "success"
        else:
            conn.rollback()
            total_stats["status"] = "failed_or_empty"

    except Exception as e:
        conn.rollback()
        total_stats["status"] = f"failed: {str(e)}"
        raise
    finally:
        total_elapsed = time.perf_counter() - function_start
        total_stats["elapsed_time_seconds"] = round(total_elapsed, 4)
        
        # Pastikan koneksi selalu ditutup
        cursor.close()
        conn.close()

    return total_stats


def direct_path_insert(
    conn,
    df_chunk: pl.DataFrame,
    schema_name: str,
    staging_table_name: str,
):
    if df_chunk.is_empty():
        return 0, 0

    columns = df_chunk.columns

    # TIDAK PERLU LAGI df_chunk.astype(object).where(...)
    # Polars + Arrow menangani tipe data dan NULLs secara native & zero-copy.

    try:
        conn.direct_path_load(
            schema_name=schema_name,
            table_name=staging_table_name,
            column_names=columns,
            data=df_chunk, # Langsung oper objek Polars DataFrame
        )
        return df_chunk.height, 0 # Gunakan .height untuk menghitung baris Polars
    except oracledb.DatabaseError as db_err:
        raise RuntimeError(f"Direct Path Load Error: {db_err}")


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

        if non_key_columns:
            update_clause = ", ".join(f"tgt.{col} = src.{col}" for col in non_key_columns)
            merge_sql += f"WHEN MATCHED THEN\n    UPDATE SET {update_clause}\n"

        merge_sql += "WHEN NOT MATCHED THEN\n"
        merge_sql += f"    INSERT ({insert_cols})\n"
        merge_sql += f"    VALUES ({insert_values})"

        cursor.execute(merge_sql)


def _data_generator(df: pl.DataFrame, chunk_size: int):
    # Menggunakan df.height yang merupakan standar Polars untuk jumlah baris (ekuivalen len(df))
    n_rows = df.height
    
    if not chunk_size or chunk_size >= n_rows:
        yield df
        return

    for offset in range(0, n_rows, chunk_size):
        yield df.slice(offset, chunk_size)