import oracledb
import polars as pl
import math
from tqdm import tqdm

from .base import DatabaseAdapter

class OracleAdapter(DatabaseAdapter):
    def __init__(self, connection_info: str | dict) -> None:
        super().__init__(connection_info=connection_info)

    def read_database(
        self, 
        query: str, 
        chunk_size: int = 100
    ) -> pl.DataFrame:
        username = self.connection_info["user"]
        password = self.connection_info["password"]
        dsn = self.connection_info["dsn"]

        with oracledb.connect(user=self._q(username), password=password, dsn=dsn) as conn:
            oracle_df = conn.fetch_df_all(statement=query, arraysize=chunk_size)
            return pl.from_arrow(oracle_df)

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
        schema = schema_name if schema_name is not None else self.connection_info["user"]
        columns = df.columns
        username = self.connection_info["user"]
        password = self.connection_info["password"]
        dsn = self.connection_info["dsn"]

        with oracledb.connect(user=self._q(username), password=password, dsn=dsn) as conn:
            self._ensure_table_exists(conn, schema, df, table_name, identifier, if_table_not_exists, dtype_overrides)
            
            match mode:
                case "append":
                    self._append(conn, schema, table_name, columns, df, chunk_size)
                case "replace":
                    self._replace(conn, schema, table_name, columns, df, chunk_size)
                case "upsert":
                    self._upsert(conn, schema, table_name, columns, df, chunk_size, identifier)

            conn.commit()

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
        schema = schema_name if schema_name is not None else self.connection_info["user"]
        columns = df.columns
        username = self.connection_info["user"]
        password = self.connection_info["password"]
        dsn = self.connection_info["dsn"]

        num_rows = df.height
        c_size = chunk_size if chunk_size else num_rows
        total_batches = math.ceil(num_rows / c_size)

        with oracledb.connect(user=self._q(username), password=password, dsn=dsn) as conn:
            self._ensure_table_exists(conn, schema, df, table_name, identifier, if_table_not_exists, dtype_overrides)
            
            pbar = tqdm(df.iter_slices(n_rows=c_size), total=total_batches, desc=f"Writing to {table_name}")

            for batch_idx, df_chunk in enumerate(pbar):
                current_mode = "append" if (mode == "replace" and batch_idx > 0) else mode

                try:
                    match current_mode:
                        case "append":
                            self._append(conn, schema, table_name, columns, df_chunk, c_size)
                        case "replace":
                            self._replace(conn, schema, table_name, columns, df_chunk, c_size)
                        case "upsert":
                            self._upsert(conn, schema, table_name, columns, df_chunk, c_size, identifier)

                    conn.commit()

                except Exception as e:
                    start_row = batch_idx * c_size
                    end_row = min((batch_idx + 1) * c_size, num_rows)
                    print(f"\n[ERROR] Gagal di batch {batch_idx + 1}/{total_batches} (Baris {start_row} - {end_row}). Pesan: {e}")
                    conn.rollback()
                    raise e
            
        return True

    def _q(self, in_str: str):
        return f'"{in_str}"'

    # Append
    def _append(self, conn, schema_name, table_name, column_names, df, batch_size):
        kwargs = {"batch_size": batch_size} if batch_size else {}
        conn.direct_path_load(
            schema_name=self._q(schema_name), 
            table_name=self._q(table_name), 
            column_names=[self._q(c) for c in column_names], 
            data=df, 
            **kwargs
        )

    # Truncate-Insert
    def _replace(self, conn, schema_name, table_name, column_names, df, batch_size):
        # Truncate
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE {self._q(schema_name)}.{self._q(table_name)}')

        # Insert
        self._append(conn, schema_name, table_name, column_names, df, batch_size)

    # Staging-Merge
    def _upsert(self, conn, schema_name, table_name, column_names, df, batch_size, identifier):
        # Staging
        staging_table = f"{table_name}_staging" 
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE {self._q(staging_table)} NOLOGGING AS SELECT * FROM {self._q(table_name)} WHERE 1=0')
        self._append(conn, schema_name, staging_table, column_names, df, batch_size)

        # Merge
        with conn.cursor() as cur:
            match_cond = " AND ".join([f't."{c}" = s."{c}"' for c in identifier])
            update_cols = [c for c in column_names if c not in identifier]
            update_set = ", ".join([f't."{c}" = s."{c}"' for c in update_cols])
            action = f"WHEN MATCHED THEN UPDATE SET {update_set}" if update_cols else ""
            insert_cols = ", ".join([f'"{c}"' for c in column_names])
            insert_vals = ", ".join([f's."{c}"' for c in column_names])

            QUERY_MERGE = f"""
                MERGE INTO {self._q(table_name)} t
                USING {self._q(staging_table)} s
                ON ({match_cond})
                {action}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_vals})
            """
            cur.execute(QUERY_MERGE)

        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE {self._q(staging_table)} PURGE')

    # Polars to Oracle Type Mapping
    POLARS_TO_ORACLE: dict[type, str] = {
        pl.Int8: "NUMBER(3)",
        pl.Int16: "NUMBER(5)",
        pl.Int32: "NUMBER(10)",
        pl.Int64: "NUMBER(19)",
        pl.UInt8: "NUMBER(3)",
        pl.UInt16: "NUMBER(5)",
        pl.UInt32: "NUMBER(10)",
        pl.UInt64: "NUMBER(20)",
        pl.Float16: "BINARY_FLOAT",
        pl.Float32: "BINARY_FLOAT",
        pl.Float64: "BINARY_DOUBLE",
        pl.Date: "DATE",
        pl.Time: "TIMESTAMP",
        pl.Datetime: "TIMESTAMP",
        pl.Duration: "INTERVAL DAY(9) TO SECOND(6)",
        pl.String: "VARCHAR2(4000)",
        pl.Categorical: "VARCHAR2(4000)",
        pl.Utf8: "VARCHAR2(4000)",
        pl.Binary: "BLOB",
        pl.Boolean: "NUMBER(1)",
        pl.Null: "VARCHAR2(4000)",
    }

    def _infer_dtype(self, dtype: pl.DataType) -> str:
        base_dtype = dtype.base_type() if hasattr(dtype, "base_type") else dtype
        datatype = self.POLARS_TO_ORACLE.get(base_dtype)
        return datatype if datatype is not None else "VARCHAR2(4000)"
        
    def _is_table_exists(self, conn, schema_name, table_name) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM ALL_TABLES WHERE OWNER = :owner AND TABLE_NAME = :table_name",
                owner=schema_name,
                table_name=table_name,
            )
            return cur.fetchone() is not None
        
    def _ensure_table_exists(self, conn, schema_name, df, table_name, identifier, if_table_not_exists, dtype_overrides):
        if self._is_table_exists(conn, schema_name, table_name):
            return
        
        if if_table_not_exists == "fail":
            raise ValueError("Table is not exists in database")
    
        print(f"[DBDF] Table {self._q(table_name)} not found. Auto-creating...")
    
        # Infer datatype
        dtype_overrides = dtype_overrides or {}
        cols_def = []
        for col_name, dtype in df.schema.items():
            ora_type = dtype_overrides.get(col_name) or self._infer_dtype(dtype)
            cols_def.append(f'{self._q(col_name)} {ora_type}')
        cols_sql = ", ".join(cols_def)
    
        # Infer primary key
        pk_sql = ""
        if identifier:
            pk_cols = ", ".join(self._q(c) for c in identifier)
            pk_sql = f", PRIMARY KEY ({pk_cols})"
    
        # Execute ddl
        QUERY_CREATE = f'CREATE TABLE {self._q(schema_name)}.{self._q(table_name)} ({cols_sql}{pk_sql})'
        print(f"[DBDF] Executing: {QUERY_CREATE}")
        with conn.cursor() as cur:
            cur.execute(QUERY_CREATE)
