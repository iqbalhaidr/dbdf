import oracledb
import polars as pl
import pandas as pd
from tqdm import tqdm

from .base import DatabaseAdapter


class OracleAdapter(DatabaseAdapter):
    def __init__(self, target: str | dict) -> None:
        super().__init__(target=target)

    def read_database(
        self, *, query: str | None = None, chunk_size: int | None = None, progress_bar: bool = False, **kwargs
    ) -> pl.DataFrame:
        username = self.target["user"]
        password = self.target["pass"]
        dsn = self.target["dsn"]

        with oracledb.connect(user=self._q(username), password=password, dsn=dsn) as conn:
            if progress_bar:
                batch_size = chunk_size if chunk_size is not None else 100_000
                with conn.cursor() as cur:
                    cur.arraysize = batch_size
                    cur.execute(query)
                    batches = []

                    with tqdm(desc="Reading Oracle", unit=" rows") as pbar:
                        while True:
                            batch = cur.fetchmany(batch_size)
                            if not batch:
                                break
                            batches.extend(batch)
                            pbar.update(len(batch))

                    columns = [col[0] for col in cur.description]
                    return pl.DataFrame(data=batches, schema=columns)
            else:
                if chunk_size is not None:
                    kwargs["arraysize"] = chunk_size

                oracle_df = conn.fetch_df_all(statement=query, **kwargs)
                return pl.from_arrow(oracle_df)

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
        schema_name: str | None = None,
        **kwargs,
    ) -> None:
        if isinstance(data, pd.DataFrame):
            data = pl.from_pandas(data=data)

        if overrides:
            cast_exprs = []
            for col_name, ora_type in overrides.items():
                base_type = ora_type.split("(")[0].strip().upper()
                if base_type.startswith("INTERVAL"):
                    base_type = "INTERVAL"
                target_dtype = self.ORACLE_TO_POLARS.get(base_type)

                if target_dtype:
                    cast_exprs.append(pl.col(col_name).cast(target_dtype))
                else:
                    print(f"[DBDF] Warning: No auto-cast mapped for Oracle type '{ora_type}'.")

            if cast_exprs:
                data = data.with_columns(cast_exprs)

        username = self.target["user"]
        password = self.target["pass"]
        dsn = self.target["dsn"]
        schema = schema_name if schema_name is not None else self.target["user"]
        metadata = data.schema
        columns = data.columns

        with oracledb.connect(user=self._q(username), password=password, dsn=dsn) as conn:
            self._ensure_table_exists(conn, schema, metadata, table_name, identifiers, overrides)

            match mode:
                case "append":
                    self._append(conn, schema, table_name, columns, data, chunk_size, progress_bar, **kwargs)
                case "replace":
                    self._replace(conn, schema, table_name, columns, data, chunk_size, progress_bar, **kwargs)
                case "upsert":
                    self._upsert(
                        conn, schema, table_name, columns, data, chunk_size, identifiers, progress_bar, **kwargs
                    )
                case _:
                    raise ValueError(f"Arg mode={mode} invalid")

            conn.commit()

    def _q(self, in_str: str):
        return f'"{in_str}"'

    # Append
    def _append(self, conn, schema_name, table_name, column_names, data, batch_size, progress_bar, **kwargs):
        if batch_size is not None:
            kwargs["batch_size"] = batch_size

        if progress_bar:
            batch_size = 100_000 if batch_size is None else batch_size
            with tqdm(total=data.height, desc=f"Writing {table_name}", unit=" rows") as pbar:
                for chunk in data.iter_slices(n_rows=batch_size):
                    conn.direct_path_load(
                        schema_name=self._q(schema_name),
                        table_name=self._q(table_name),
                        column_names=[self._q(c) for c in column_names],
                        data=chunk,
                        **kwargs,
                    )
                    pbar.update(len(chunk))
        else:
            conn.direct_path_load(
                schema_name=self._q(schema_name),
                table_name=self._q(table_name),
                column_names=[self._q(c) for c in column_names],
                data=data,
                **kwargs,
            )

    # Truncate-Insert
    def _replace(self, conn, schema_name, table_name, column_names, data, batch_size, progress_bar, **kwargs):
        # Truncate
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {self._q(schema_name)}.{self._q(table_name)}")

        # Insert
        self._append(conn, schema_name, table_name, column_names, data, batch_size, progress_bar, **kwargs)

    # Staging-Merge
    def _upsert(
        self, conn, schema_name, table_name, column_names, data, batch_size, identifiers, progress_bar, **kwargs
    ):
        # Staging
        staging_table = f"{table_name}_staging"
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {self._q(staging_table)} NOLOGGING AS SELECT * FROM {self._q(table_name)} WHERE 1=0"
            )

        try:
            self._append(conn, schema_name, staging_table, column_names, data, batch_size, progress_bar, **kwargs)

            # Merge
            with conn.cursor() as cur:
                match_cond = " AND ".join([f"t.{self._q(c)} = s.{self._q(c)}" for c in identifiers])
                update_cols = [c for c in column_names if c not in identifiers]
                update_set = ", ".join([f"t.{self._q(c)} = s.{self._q(c)}" for c in update_cols])
                action = f"WHEN MATCHED THEN UPDATE SET {update_set}" if update_cols else ""
                insert_cols = ", ".join([f"{self._q(c)}" for c in column_names])
                insert_vals = ", ".join([f"s.{self._q(c)}" for c in column_names])

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
        finally:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE {self._q(staging_table)} PURGE")

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

    ORACLE_TO_POLARS: dict[str, type] = {
        "NUMBER": pl.Float64,
        "BINARY_FLOAT": pl.Float32,
        "BINARY_DOUBLE": pl.Float64,
        "FLOAT": pl.Float64,
        "DATE": pl.Datetime,
        "TIMESTAMP": pl.Datetime,
        "INTERVAL": pl.Duration,
        "VARCHAR2": pl.String,
        "NVARCHAR2": pl.String,
        "VARCHAR": pl.String,
        "CHAR": pl.String,
        "CLOB": pl.String,
        "BLOB": pl.Binary,
        "RAW": pl.Binary,
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

    def _ensure_table_exists(self, conn, schema_name, metadata, table_name, identifiers, overrides):
        if self._is_table_exists(conn, schema_name, table_name):
            return

        print(f"[DBDF] Table {self._q(table_name)} not found. Auto-creating...")

        # Infer datatype
        overrides = overrides or {}
        cols_def = []
        for col_name, dtype in metadata.items():
            ora_type = overrides.get(col_name) or self._infer_dtype(dtype)
            cols_def.append(f"{self._q(col_name)} {ora_type}")
        cols_sql = ", ".join(cols_def)

        # Infer primary key
        pk_sql = ""
        if identifiers:
            pk_cols = ", ".join(self._q(c) for c in identifiers)
            pk_sql = f", PRIMARY KEY ({pk_cols})"

        # Execute ddl
        QUERY_CREATE = f"CREATE TABLE {self._q(schema_name)}.{self._q(table_name)} ({cols_sql}{pk_sql})"
        print(f"[DBDF] Executing: {QUERY_CREATE}")
        with conn.cursor() as cur:
            cur.execute(QUERY_CREATE)
