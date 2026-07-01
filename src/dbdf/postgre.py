import adbc_driver_postgresql.dbapi

def write_database(uri, df, table_name, mode, identifier):
    with adbc_driver_postgresql.dbapi.connect(uri) as conn:
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
def _upsert(conn, df, table_name, identifier):
    with conn.cursor() as cur:
        # Staging table
        QUERY_CREATE_STAGING = f'CREATE UNLOGGED TABLE "{table_name}_staging" (LIKE "{table_name}")'
        cur.execute(QUERY_CREATE_STAGING)
        
        cur.adbc_ingest(
            table_name=f"{table_name}_staging",
            data=df.to_arrow(),
            mode="append"
        )

        # Insert on Conflict
        cols    = ", ".join(f'"{c}"' for c in df.columns)
        updates = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != f"{identifier}"])

        QUERY_UPSERT = f"""
            INSERT INTO "{table_name}" ({cols})
            SELECT * FROM "{table_name}_staging"
            ON CONFLICT ("{identifier}")
            DO UPDATE SET {updates};
        """
        cur.execute(QUERY_UPSERT)

# Benchmark
'''
import time

t0 = time.perf_counter()
... [CODE]
t1 = time.perf_counter()
print(f"[PROCESS]={t1-t0:.2f}s")
'''