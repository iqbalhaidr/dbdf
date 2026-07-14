import adbc_driver_postgresql.dbapi
import polars as pl
import pandas as pd
import pyarrow as pa
from tqdm import tqdm

from .base import DatabaseAdapter

class PostgresAdapter(DatabaseAdapter):
    def __init__(self, target: str | dict[str, str]) -> None:
        super().__init__(target=target)

    def read_database(
        self,
        *,
        query: str | None = None,
        chunk_size: int | None = None,
        progress_bar: bool = False,
        **kwargs
    ) -> pl.DataFrame:
        if progress_bar:
            with adbc_driver_postgresql.dbapi.connect(self.target) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    reader = cur.fetch_record_batch()
                    batches = []

                    with tqdm(desc="Reading PostgreSQL", unit=" rows") as pbar:
                        for batch in reader:
                            batches.append(batch)
                            pbar.update(len(batch))
                    
                    table = pa.Table.from_batches(batches)
                    return pl.from_arrow(table)
        else:
            return pl.read_database_uri(query=query, uri=self.target, **kwargs)

    # NOTE: identifier dapat berupa single/multiple attribute. asalkan sudah memiliki constraint unique, primary key, atau constraint "unique" lainnya
    def write_database(
        self, 
        data: pl.DataFrame | pd.DataFrame, 
        *,
        mode: str = "replace",
        identifiers: list[str] | None = None,
        chunk_size: int | None = None,
        overrides: dict[str, str] | None = None,
        progress_bar: bool = False,
        table_name: str,
        **kwargs
    ) -> None: 
        if isinstance(data, pd.DataFrame):
            data = pl.from_pandas(data=data)

        if overrides:
            cast_exprs = []
            for col_name, pg_type in overrides.items():
                base_type = pg_type.split("(")[0].strip().upper()
                target_dtype = self.POSTGRES_TO_POLARS.get(base_type)

                if target_dtype:
                    cast_exprs.append(pl.col(col_name).cast(target_dtype))
                else:
                    print(f"[DBDF] Warning: No auto-cast mapped for PostgreSQL type '{pg_type}'.")

            if cast_exprs:
                data = data.with_columns(cast_exprs)

        total_rows = data.height
        metadata = data.schema
        columns = data.columns
        table = data.to_arrow()

        if progress_bar:
            chunk_size = 100_000 if chunk_size is None else chunk_size
            def batcher():
                with tqdm(total=total_rows, desc=f"Writing {table_name}", unit=" rows") as pbar:
                    for batch in table.to_batches(max_chunksize=chunk_size):
                        yield batch
                        pbar.update(len(batch))

            data = pa.RecordBatchReader.from_batches(table.schema, batcher())
        else:
            data = table.to_reader(max_chunksize=chunk_size) if chunk_size is not None else data

        with adbc_driver_postgresql.dbapi.connect(self.target) as conn:
            self._ensure_table_exists(conn, metadata, table_name, identifiers, overrides)

            match mode:
                case "append":
                    self._append(conn, data, table_name, **kwargs)
                case "replace":
                    self._replace(conn, data, table_name, **kwargs)
                case "upsert":
                    self._upsert(conn, data, table_name, identifiers, columns)
                
            conn.commit()
    
    def _q(self, in_str: str):
        return f'"{in_str}"'

    # Append
    def _append(self, conn, data, table_name, **kwargs):
        with conn.cursor() as cur:
            cur.adbc_ingest(table_name=table_name, data=data, mode="append", **kwargs)

    # Truncate-Insert
    def _replace(self, conn, data, table_name, **kwargs):
        # Truncate
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE {self._q(table_name)}')

        # Insert
        self._append(conn, data, table_name, **kwargs)

    # Staging-Insert on Conflict
    def _upsert(self, conn, data, table_name, identifiers, columns):
        # Staging
        staging_table = f'{table_name}_staging'
        with conn.cursor() as cur:
            cur.execute(f'CREATE TEMP TABLE {self._q(staging_table)} (LIKE {self._q(table_name)})')
        self._append(conn, data, staging_table, db_schema_name="pg_temp")

        # Insert on Conflict
        with conn.cursor() as cur:
            cols    = ", ".join(f'{self._q(c)}' for c in columns)
            conflicts = ", ".join(f'{self._q(c)}' for c in identifiers)
            updates = ", ".join([f'{self._q(c)} = EXCLUDED.{self._q(c)}' for c in columns if c not in identifiers])
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

    POSTGRES_TO_POLARS: dict[str, type] = {
        "SMALLINT": pl.Int16,
        "INTEGER": pl.Int32,
        "INT": pl.Int32,
        "BIGINT": pl.Int64,
        "NUMERIC": pl.Float64, 
        "REAL": pl.Float32,
        "DOUBLE PRECISION": pl.Float64,
        "DATE": pl.Date,
        "TIME": pl.Time,
        "TIMESTAMP": pl.Datetime,
        "INTERVAL": pl.Duration,
        "VARCHAR": pl.String,
        "TEXT": pl.String,
        "CHAR": pl.String,
        "BYTEA": pl.Binary,
        "BOOLEAN": pl.Boolean,
    }

    def _infer_dtype(self, dtype: pl.DataType):
        datatype = self.POLARS_TO_POSTGRES.get(dtype)
        return datatype if datatype is not None else "VARCHAR"

    def _is_table_exists(self, conn, table_name) -> bool:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = '{table_name}' 
                AND table_schema = current_schema()
            """)
            return cur.fetchone() is not None

    def _ensure_table_exists(self, conn, metadata, table_name, identifiers, overrides):
        if self._is_table_exists(conn, table_name):
            return
        
        print(f"[DBDF] Table {self._q(table_name)} not found. Auto-creating...")

        # Infer datatype
        overrides = overrides or {}
        cols_def = []
        for col_name, dtype in metadata.items():
            pg_type = overrides.get(col_name) or self._infer_dtype(dtype)
            cols_def.append(f'{self._q(col_name)} {pg_type}')
        cols_sql = ", ".join(cols_def)

        # Infer primary key
        pk_sql = ""
        if identifiers:
            pk_cols = ", ".join(f'{self._q(c)}' for c in identifiers)
            pk_sql = f", PRIMARY KEY ({pk_cols})"

        # Execute ddl
        QUERY_CREATE = f'CREATE TABLE {self._q(table_name)} ({cols_sql}{pk_sql});'
        print(f"[DBDF] Executing: {QUERY_CREATE}")
        with conn.cursor() as cur:
            cur.execute(QUERY_CREATE)
