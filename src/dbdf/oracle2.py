import oracledb
import polars as pl

def _q(name):
    """Wrap an identifier in double quotes (case-sensitive Oracle object name)."""
    return f'"{name}"'

def write_database(creds, df, table_name, mode, identifier, chunk_size, dtype_overrides):
    schema_name = creds["user"]

    with oracledb.connect(user=_q(creds["user"]), password=creds["password"], dsn=creds["dsn"]) as conn:
        _ensure_table_exists(conn, schema_name, df, table_name, identifier, dtype_overrides)
        match mode:
            case "append":
                _append(conn, schema_name, table_name, df.columns, df, chunk_size)
            case "replace":
                _replace(conn, schema_name, table_name, df.columns, df, chunk_size)
            case "upsert":
                _upsert(conn, schema_name, table_name, df.columns, df, chunk_size, identifier)

        conn.commit()

# NOTE: " untuk object, ' untuk literal string, '"app_user"' untuk bungkus object dalam string di python
# NOTE: tanpa "", semua diubah otomatis jadi capital, EMPLOYEE == "EMPLOYEE"
# FINAL: schema_name, table_name, column_names pass string biasa saja (standar) tidak perlu quote2 automatis 
def _append(conn, schema_name, table_name, column_names, df, batch_size):
    kwargs = {"batch_size": batch_size} if batch_size else {}
    conn.direct_path_load(
        schema_name=_q(schema_name), 
        table_name=_q(table_name), 
        column_names=[_q(c) for c in column_names], 
        data=df, 
        **kwargs
    )

# Truncate-Insert
def _replace(conn, schema_name, table_name, column_names, df, batch_size):
    with conn.cursor() as cur:
        # Truncate
        QUERY_TRUNCATE = f'TRUNCATE TABLE {_q(schema_name)}.{_q(table_name)}'
        cur.execute(QUERY_TRUNCATE)

    # Insert
    _append(conn, schema_name, table_name, column_names, df, batch_size)

# Staging-Merge
def _upsert(conn, schema_name, table_name, column_names, df, batch_size, identifier):
    if isinstance(identifier, str):
        identifier = [identifier]

    # Staging
    staging_table = f"{table_name}_STG" 
    with conn.cursor() as cur:
        QUERY_CREATE_STAGING = f'CREATE TABLE {_q(staging_table)} NOLOGGING AS SELECT * FROM {_q(table_name)} WHERE 1=0'
        cur.execute(QUERY_CREATE_STAGING)

    try:    
        # Load data to staging
        _append(conn, schema_name, staging_table, column_names, df, batch_size)

        # Merge
        with conn.cursor() as cur:
            match_cond = " AND ".join([f't."{c}" = s."{c}"' for c in identifier])
            update_cols = [c for c in column_names if c not in identifier]
            update_set = ", ".join([f't."{c}" = s."{c}"' for c in update_cols])
            action = f"WHEN MATCHED THEN UPDATE SET {update_set}" if update_cols else ""
            insert_cols = ", ".join([f'"{c}"' for c in column_names])
            insert_vals = ", ".join([f's."{c}"' for c in column_names])
            
            # Using Parallel Hint to force multi-core processing
            QUERY_MERGE = f"""
                MERGE INTO {_q(table_name)} t
                USING {_q(staging_table)} s
                ON ({match_cond})
                {action}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_vals})
            """
            cur.execute(QUERY_MERGE)
    finally:
        # Drop Staging
        with conn.cursor() as cur:
            QUERY_DROP_STAGING = f'DROP TABLE {_q(staging_table)} PURGE'
            cur.execute(QUERY_DROP_STAGING)

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

def _infer_dtype(dtype: pl.DataType) -> str:
    datatype = POLARS_TO_ORACLE.get(dtype)
    return datatype if datatype is not None else "VARCHAR2(4000)"

# def _is_table_exists(conn, schema_name, table_name) -> bool:
#     with conn.cursor() as cur:
#         cur.execute(f"SELECT 1 FROM ALL_TABLES WHERE OWNER = {_q(schema_name)} AND TABLE_NAME = {_q(table_name)}")
#         return cur.fetchone() is not None
    
def _is_table_exists(conn, schema_name, table_name) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ALL_TABLES WHERE OWNER = :owner AND TABLE_NAME = :table_name",
            owner=schema_name,
            table_name=table_name,
        )
        return cur.fetchone() is not None
    
def _ensure_table_exists(conn, schema_name, df, table_name, identifier, dtype_overrides):
    if _is_table_exists(conn, schema_name, table_name):
        return
 
    print(f"[DBDF] Table {_q(table_name)} not found. Auto-creating...")
 
    # Infer datatype
    dtype_overrides = dtype_overrides or {}
    cols_def = []
    for col_name, dtype in df.schema.items():
        ora_type = dtype_overrides.get(col_name) or _infer_dtype(dtype)
        cols_def.append(f'{_q(col_name)} {ora_type}')
    cols_sql = ", ".join(cols_def)
 
    # Infer primary key
    pk_sql = ""
    if identifier:
        ident = [identifier] if isinstance(identifier, str) else identifier
        pk_cols = ", ".join(_q(c) for c in ident)
        pk_sql = f", PRIMARY KEY ({pk_cols})"
 
    # Execute ddl
    QUERY_CREATE = f'CREATE TABLE {_q(schema_name)}.{_q(table_name)} ({cols_sql}{pk_sql})'
    print(f"[DBDF] Executing: {QUERY_CREATE}")
    with conn.cursor() as cur:
        cur.execute(QUERY_CREATE)
