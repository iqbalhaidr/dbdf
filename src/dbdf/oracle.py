import polars as pl
import adbc_driver_postgresql.dbapi

def write_database(uri, df, table_name, mode):
    with adbc_driver_postgresql.dbapi.connect(uri) as conn:
        match mode:
            case "append":
                return
            case "replace":
                return
            case "upsert":
                return
            
        conn.commit()