import adbc_driver_postgresql.dbapi
import polars as pl

from . import utils
from . import connections

def _copy_to_staging(conn, table_name, df):
    df.write_database(
        table_name=f"{table_name}_staging",
        connection=conn,
        if_table_exists="append",
        engine="adbc"
    )

def _upsert_from_staging(conn, table_name, primary_key, df):
    query = utils._upsert_query_builder(table_name, df.columns, primary_key)
    
    with conn.cursor() as cur:
        cur.execute(query)

def write_database(uri, df, table_name, mode, primary_key):
    conn = connections._connect_db(uri)

    utils._create_staging_table(conn, table_name)
    _copy_to_staging(conn, table_name, df)
    _upsert_from_staging(conn, table_name, primary_key, df)
    conn.commit()

    conn.close()