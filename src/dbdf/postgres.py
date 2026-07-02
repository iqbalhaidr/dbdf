import adbc_driver_postgresql.dbapi

# TODO: kasus table belum ada di database (table baru)
def write_database(uri, df, table_name, mode, identifier, chunk_size):
    arrow_data = df.to_arrow()
    if chunk_size is not None:
        adbc_payload = arrow_data.to_reader(max_chunksize=chunk_size)
    else:
        adbc_payload = arrow_data

    # Extract columns name
    columns = df.columns
    
    with adbc_driver_postgresql.dbapi.connect(uri) as conn:
        match mode:
            case "append":
                _append(conn, adbc_payload, table_name)
            case "replace":
                _replace(conn, adbc_payload, table_name)
            case "upsert":
                _upsert(conn, adbc_payload, table_name, identifier, columns)
            
        conn.commit()

def _append(conn, adbc_payload, table_name):
    with conn.cursor() as cur:
        cur.adbc_ingest(
            table_name=f"{table_name}",
            data=adbc_payload,
            mode="append"
        )

# Truncate-Insert
def _replace(conn, adbc_payload, table_name):
    with conn.cursor() as cur:
        # Truncate
        QUERY_TRUNCATE = f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY RESTRICT'
        cur.execute(QUERY_TRUNCATE)

        # Insert
        cur.adbc_ingest(
            table_name=f"{table_name}",
            data=adbc_payload,
            mode="append"
        )

# Staging-Insert on Conflict
# NOTE: identifier dapat berupa single/multiple attribute. asalkan sudah memiliki constraint unique, primary key, atau constraint "unique" lainnya
# TODO: optimasi (rps >= 10k/sec). kayanya udah, tapi butuh ditest lagi!
def _upsert(conn, adbc_payload, table_name, identifier, columns):
    if isinstance(identifier, str):
        identifier = [identifier]

    with conn.cursor() as cur:
        # Staging table
        QUERY_CREATE_STAGING = f'CREATE TEMP TABLE "{table_name}_staging" (LIKE "{table_name}")'
        cur.execute(QUERY_CREATE_STAGING)
        
        cur.adbc_ingest(
            table_name=f"{table_name}_staging",
            data=adbc_payload,
            mode="append",
            db_schema_name="pg_temp"
        )

        # Insert on Conflict
        cols    = ", ".join(f'"{c}"' for c in columns)
        updates = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in identifier])
        conflicts = ", ".join(f'"{c}"' for c in identifier)
        action = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"

        QUERY_UPSERT = f"""
            INSERT INTO "{table_name}" ({cols})
            SELECT * FROM "{table_name}_staging"
            ON CONFLICT ({conflicts})
            {action};
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