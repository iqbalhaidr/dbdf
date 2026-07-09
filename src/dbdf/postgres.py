import adbc_driver_postgresql.dbapi
import polars as pl
import math
from tqdm import tqdm

from .base import DatabaseAdapter

class PostgresAdapter(DatabaseAdapter):
    def __init__(self, connection_info: str | dict) -> None:
        super().__init__(connection_info=connection_info)
        self.connection_uri = self._build_connection_uri(connection_info)

    def _build_connection_uri(self, info: str | dict) -> str:
        if isinstance(info, str):
            return info
        elif isinstance(info, dict):
            user = info.get('user', '')
            password = info.get('password', '')
            host = info.get('host', 'localhost')
            port = info.get('port', 5432)
            database = info.get('database', '')
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"
        else:
            raise TypeError("connection_info harus berupa dictionary atau string URL koneksi.")


    def read_database(
        self, 
        query: str, 
        chunk_size: int = None
    ) -> pl.DataFrame:
        with adbc_driver_postgresql.dbapi.connect(self.connection_uri) as conn:
            return pl.read_database(query=query, connection=conn, iter_batches=False, batch_size=chunk_size)

    def write_database(
        self,
        df: pl.DataFrame,
        table_name: str,
        mode: str = "append",
        identifier: list[str] = None,
        if_table_not_exists: str = "fail",
        dtype_overrides: dict[type, str] = None,
        chunk_size: int = None,
        schema_name: str = None
    ) -> bool: 
        arrow_data = df.to_arrow()
        adbc_payload = arrow_data.to_reader(max_chunksize=chunk_size) if chunk_size is not None else arrow_data

        # Extract columns name
        columns = df.columns
        print("kontol")
        print(self.connection_info)

        with adbc_driver_postgresql.dbapi.connect(self.connection_uri) as conn:
            try:
                self._ensure_table_exists(conn, df, table_name, identifier, if_table_not_exists, dtype_overrides)
            except ValueError as e:
                print(e)

            match mode:
                case "append":
                    self._append(conn, adbc_payload, table_name)
                case "replace":
                    self._replace(conn, adbc_payload, table_name)
                case "upsert":
                    self._upsert(conn, adbc_payload, table_name, identifier, columns)
                
            conn.commit()

        return True
    
    def write_database_with_progress_bar(
        self,
        df: pl.DataFrame,
        table_name: str,
        mode: str = "append",
        identifier: list[str] = None,
        if_table_not_exists: str = "fail",
        dtype_overrides: dict[type, str] = None,
        chunk_size: int = None,
        schema_name: str = None
    ) -> bool: 
        columns = df.columns
        num_rows = df.height
        c_size = chunk_size if chunk_size else num_rows
        total_batches = math.ceil(num_rows / c_size)

        with adbc_driver_postgresql.dbapi.connect(self.connection_uri) as conn:
            try:
                self._ensure_table_exists(conn, df, table_name, identifier, if_table_not_exists, dtype_overrides)
            except ValueError as e:
                print(e)
                return False

            # Inisialisasi Progress Bar
            pbar = tqdm(df.iter_slices(n_rows=c_size), total=total_batches, desc=f"Writing to {table_name}")

            for batch_idx, df_chunk in enumerate(pbar):
                adbc_payload = df_chunk.to_arrow()
                
                # Cegah TRUNCATE berkali-kali pada mode replace
                current_mode = "append" if (mode == "replace" and batch_idx > 0) else mode

                try:
                    match current_mode:
                        case "append":
                            self._append(conn, adbc_payload, table_name)
                        case "replace":
                            self._replace(conn, adbc_payload, table_name)
                        case "upsert":
                            self._upsert(conn, adbc_payload, table_name, identifier, columns)
                    
                    # Commit per batch. Jika 1 batch gagal, batch sebelumnya tetap tersimpan.
                    conn.commit()

                except Exception as e:
                    start_row = batch_idx * c_size
                    end_row = min((batch_idx + 1) * c_size, num_rows)
                    print(f"\n[ERROR] Gagal di batch {batch_idx + 1}/{total_batches} (Baris {start_row} - {end_row}). Pesan: {e}")
                    conn.rollback() 
                    raise e # Hentikan eksekusi setelah error

        return True
    
    def _q(self, in_str: str):
        return f'"{in_str}"'

    # Append
    def _append(self, conn, adbc_payload, table_name, **kwargs):
        with conn.cursor() as cur:
            cur.adbc_ingest(table_name=table_name, data=adbc_payload, mode="append", **kwargs)

    # Truncate-Insert
    def _replace(self, conn, adbc_payload, table_name):
        # Truncate
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE {self._q(table_name)}')

        # Insert
        self._append(conn, adbc_payload, table_name)

    # Staging-Insert on Conflict
    # NOTE: identifier dapat berupa single/multiple attribute. asalkan sudah memiliki constraint unique, primary key, atau constraint "unique" lainnya
    def _upsert(self, conn, adbc_payload, table_name, identifier, columns):
        # Staging
        staging_table = f'{table_name}_staging'
        with conn.cursor() as cur:
            cur.execute(f'CREATE TEMP TABLE {self._q(staging_table)} (LIKE {self._q(table_name)})')
        self._append(conn, adbc_payload, staging_table, db_schema_name="pg_temp")

        # Insert on Conflict
        with conn.cursor() as cur:
            cols    = ", ".join(f'"{c}"' for c in columns)
            updates = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in identifier])
            conflicts = ", ".join(f'"{c}"' for c in identifier)
            action = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"

            QUERY_CONFLICT = f"""
                INSERT INTO {self._q(table_name)} ({cols})
                SELECT * FROM {self._q(staging_table)}
                ON CONFLICT ({conflicts})
                {action};
            """
            cur.execute(QUERY_CONFLICT)
    
    # Polars to PostgreSQL Type Mapping
    POLARS_TO_POSTGRES: dict[type, str] = {
        pl.Int8: "SMALLINT",
        pl.Int16: "SMALLINT",
        pl.Int32: "INTEGER",
        pl.Int64: "BIGINT",
        pl.Int128: "NUMERIC(39)",
        pl.UInt8: "SMALLINT",
        pl.UInt16: "INTEGER",
        pl.UInt32: "BIGINT",
        pl.UInt64: "NUMERIC(20)",
        pl.Float16: "REAL",
        pl.Float32: "REAL",
        pl.Float64: "DOUBLE PRECISION",
        pl.Date: "DATE",
        pl.Time: "TIME",
        pl.Datetime: "TIMESTAMP",
        pl.Duration: "INTERVAL",
        pl.String: "VARCHAR",
        pl.Categorical: "VARCHAR",
        pl.Utf8: "VARCHAR",
        pl.Binary: "BYTEA",
        pl.Boolean: "BOOLEAN",
        pl.Null: "VARCHAR",
    }

    def _infer_dtype(self, dtype: pl.DataType):
        datatype = self.POLARS_TO_POSTGRES.get(dtype)
        return datatype if datatype is not None else "VARCHAR"

    def _is_table_exists(self, conn, table_name) -> bool:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = {self._q(table_name)} 
                AND table_schema = current_schema()
            """)
            return cur.fetchone() is not None

    def _ensure_table_exists(self, conn, df, table_name, identifier, if_table_not_exists, dtype_overrides):
        if self._is_table_exists(conn, table_name):
            return
        
        if if_table_not_exists == "fail":
            raise ValueError("Table is not exists in database")
        
        print(f"[DBDF] Table {self._q(table_name)} not found. Auto-creating...")

        # Infer datatype
        dtype_overrides = dtype_overrides or {}
        cols_def = []
        for col_name, dtype in df.schema.items():
            pg_type = dtype_overrides.get(col_name) or self._infer_dtype(dtype)
            cols_def.append(f'{self._q(col_name)} {pg_type}')
        cols_sql = ", ".join(cols_def)

        # Infer primary key
        pk_sql = ""
        if identifier:
            pk_cols = ", ".join(f'"{c}"' for c in identifier)
            pk_sql = f", PRIMARY KEY ({pk_cols})"

        # Execute ddl
        QUERY_CREATE = f'CREATE TABLE {self._q(table_name)} ({cols_sql}{pk_sql});'
        print(f"[DBDF] Executing: {QUERY_CREATE}")
        with conn.cursor() as cur:
            cur.execute(QUERY_CREATE)

# Benchmark
'''
import time

t0 = time.perf_counter()
... [CODE]
t1 = time.perf_counter()
print(f"[PROCESS]={t1-t0:.2f}s")
'''