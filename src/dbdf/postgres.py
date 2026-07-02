import adbc_driver_postgresql.dbapi
import polars as pl

from .ddl import generate_ddl

# TODO: kasus table belum ada di database (table baru)
# TODO: chunking
def write_database(uri, df, table_name, mode, identifier, dtype_overrides):
    with adbc_driver_postgresql.dbapi.connect(uri) as conn:
        _ensure_table_exists(conn, df, table_name, identifier, dtype_overrides)

        match mode:
            case "append":
                _append(conn, df, table_name)
            case "replace":
                _replace(conn, df, table_name)
            case "upsert":
                _upsert(conn, df, table_name, identifier)
            
        conn.commit()

def _append(conn, df, table_name):
    with conn.cursor() as cur:
        cur.adbc_ingest(
            table_name=f"{table_name}",
            data=df.to_arrow(),
            mode="append"
        )

# Truncate-Insert
def _replace(conn, df, table_name):
    with conn.cursor() as cur:
        # Truncate
        QUERY_TRUNCATE = f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY RESTRICT'
        cur.execute(QUERY_TRUNCATE)

        # Insert
        cur.adbc_ingest(
            table_name=f"{table_name}",
            data=df.to_arrow(),
            mode="append"
        )

# Staging-Insert on Conflict
# NOTE: identifier dapat berupa single/multiple attribute. asalkan sudah memiliki constraint unique, primary key, atau constraint "unique" lainnya
# TODO: optimasi (rps >= 10k/sec). kayanya udah, tapi butuh ditest lagi!
def _upsert(conn, df, table_name, identifier):
    if isinstance(identifier, str):
        identifier = [identifier]

    with conn.cursor() as cur:
        # Staging table
        QUERY_CREATE_STAGING = f'CREATE TEMP TABLE "{table_name}_staging" (LIKE "{table_name}")'
        cur.execute(QUERY_CREATE_STAGING)
        
        cur.adbc_ingest(
            table_name=f"{table_name}_staging",
            data=df.to_arrow(),
            mode="append",
            db_schema_name="pg_temp"
        )

        # Insert on Conflict
        cols    = ", ".join(f'"{c}"' for c in df.columns)
        updates = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c not in identifier])
        conflicts = ", ".join(f'"{c}"' for c in identifier)
        action = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"

        QUERY_UPSERT = f"""
            INSERT INTO "{table_name}" ({cols})
            SELECT * FROM "{table_name}_staging"
            ON CONFLICT ({conflicts})
            {action};
        """
        cur.execute(QUERY_UPSERT)
    
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

def _infer_dtype(dtype: pl.DataType):
    datatype = POLARS_TO_POSTGRES.get(dtype)
    return datatype if datatype is not None else "VARCHAR"

def _is_table_exists(conn, table_name) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}' 
            AND table_schema = current_schema();
        """)
        return cur.fetchone() is not None

def _ensure_table_exists(conn, df, table_name, identifier, dtype_overrides):
    if _is_table_exists(conn, table_name):
        return
    
    print(f"[DBDF] Table '{table_name}' not found. Auto-creating...")

    # Infer datatype
    dtype_overrides = dtype_overrides or {}
    cols_def = []
    for col_name, dtype in df.schema.items():
        pg_type = dtype_overrides.get(col_name) or _infer_dtype(dtype)
        cols_def.append(f'"{col_name}" {pg_type}')
    cols_sql = ", ".join(cols_def)

    # Infer primary key
    pk_sql = ""
    if identifier:
        if isinstance(identifier, str):
            identifier = [identifier]
        pk_cols = ", ".join(f'"{c}"' for c in identifier)
        pk_sql = f", PRIMARY KEY ({pk_cols})"

    # Execute ddl
    QUERY_CREATE = f'CREATE TABLE "{table_name}" ({cols_sql}{pk_sql});'
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