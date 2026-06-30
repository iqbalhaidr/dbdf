import adbc_driver_postgresql.dbapi

def _connect_db(uri: str):
    conn = adbc_driver_postgresql.dbapi.connect(uri)
    return conn