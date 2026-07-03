import sys
from pathlib import Path
import polars as pl
import pandas as pd
import oracledb

# Ensure the src directory is in the python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

# from dbdf.core import write_database
from dbdf.oracle2 import write_database


def _append(conn, schema_name, table_name, column_names, df, batch_size):
    kwargs = {"batch_size": batch_size} if batch_size else {}
    conn.direct_path_load(
        schema_name=_q(schema_name), 
        table_name=_q(table_name), 
        column_names=[_q(c) for c in column_names], 
        data=df, 
        **kwargs
    )

def _replace(conn, schema_name, table_name, column_names, df, batch_size):
    with conn.cursor() as cur:
        # Truncate
        QUERY_TRUNCATE = f'TRUNCATE TABLE {_q(schema_name)}.{_q(table_name)}'
        cur.execute(QUERY_TRUNCATE)

    # Insert
    _append(conn, schema_name, table_name, column_names, df, batch_size)

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

# def _upsert(conn, schema_name, table_name, column_names, df, batch_size, identifier):
#     # if isinstance(identifier, str):
#     #     identifier = [identifier]

#     # Staging
#     staging_table = _q(f"{table_name}_STG") 
#     with conn.cursor() as cur:
#         QUERY_CREATE_STAGING = f'CREATE TABLE {staging_table} NOLOGGING AS SELECT * FROM {_q(table_name)} WHERE 1=0'
#         # cur.execute(QUERY_CREATE_STAGING)

#     print(staging_table)
#     print(QUERY_CREATE_STAGING)

    # try:    
    #     # Load data to staging
    #     _append(conn, schema_name, staging_table, column_names, df, batch_size)

    #     # Merge
    #     with conn.cursor() as cur:
    #         match_cond = " AND ".join([f't."{c}" = s."{c}"' for c in identifier])
    #         update_cols = [c for c in column_names if c not in identifier]
    #         update_set = ", ".join([f't."{c}" = s."{c}"' for c in update_cols])
    #         action = f"WHEN MATCHED THEN UPDATE SET {update_set}" if update_cols else ""
    #         insert_cols = ", ".join([f'"{c}"' for c in column_names])
    #         insert_vals = ", ".join([f's."{c}"' for c in column_names])
            
    #         # Using Parallel Hint to force multi-core processing
    #         QUERY_MERGE = f"""
    #             MERGE INTO {table_name} t
    #             USING {staging_table} s
    #             ON ({match_cond})
    #             {action}
    #             WHEN NOT MATCHED THEN
    #                 INSERT ({insert_cols})
    #                 VALUES ({insert_vals})
    #         """
    #         cur.execute(QUERY_MERGE)
    # finally:
    #     # Drop Staging
    #     with conn.cursor() as cur:
    #         cur.execute(f'DROP TABLE {staging_table} PURGE')

def _q(name):
    """Wrap an identifier in double quotes (case-sensitive Oracle object name)."""
    return f'"{name}"'

def main():
    # 1. Oracle Lite Default Credentials
    # Note: python-oracledb prefers a single DSN string. If you pass 'port' 
    # alongside a DSN, newer versions throw a "mutually exclusive" error.
    creds = {
        "user": "APP_USER",
        "password": "oracle",
        "dsn": "localhost:1521/FREEPDB1" 
    }

    df_append = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "VALUE": [10.5, 20.0, 30.0]
    })

    df_upsert = pl.DataFrame({
        "id": [4, 5, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "VALUE": [10.5, 20.0, 35.0]
    })
    
    table_name = "Test"
    print(table_name)
    schema_name = "APP_USER"
    print(schema_name)
    column_names = [c for c in df_append.columns]
    print(schema_name)

    staging_table = f"{table_name}_STG" 
    QUERY_CREATE_STAGING = f'CREATE TABLE {_q(staging_table)} NOLOGGING AS SELECT * FROM {_q(table_name)} WHERE 1=0'
    print(staging_table)
    print(QUERY_CREATE_STAGING)

    # 2. Test Append Mode
    print("Testing APPEND mode...")

    QUERY_TRUNCATE = f'TRUNCATE TABLE {_q(schema_name)}.{_q(table_name)}'
    print(QUERY_TRUNCATE)
    
    identifier = ["id"]
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
    print(QUERY_MERGE)

    QUERY_DROP_STAGING = f'DROP TABLE {_q(staging_table)} PURGE'
    print(QUERY_DROP_STAGING)

    QUERY_CHECK = f"SELECT 1 FROM ALL_TABLES WHERE OWNER = {_q(schema_name)} AND TABLE_NAME = {_q(table_name)}"
    print(QUERY_CHECK)

    cols_def = []
    for col_name, dtype in df_append.schema.items():
        ora_type = "VARCHAR"
        cols_def.append(f'{_q(col_name)} {ora_type}')
    cols_sql = ", ".join(cols_def)
    print(cols_sql)

    ident = [identifier] if isinstance(identifier, str) else identifier
    pk_cols = ", ".join(_q(c) for c in ident)
    pk_sql = f", PRIMARY KEY ({pk_cols})"
    print(pk_sql)

    QUERY_CREATE = f'CREATE TABLE {_q(schema_name)}.{_q(table_name)} ({cols_sql}{pk_sql})'
    print(QUERY_CREATE)

    # with oracledb.connect(user=(creds["user"]), password=creds["password"], dsn=creds["dsn"]) as conn:
    #     _upsert(
    #         conn=conn,
    #         df=df_upsert,
    #         table_name=table_name,
    #         column_names=column_names,
    #         schema_name=schema_name,
    #         batch_size=None,
    #         identifier=identifier
    #     )
    
    df_create = pl.DataFrame({
        "id": [4, 5, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "VALUE": [10.5, 20.0, 35.0]
    })

    write_database(
        creds=creds,
        df=df_create,
        table_name="Test233",
        mode="replace",
        identifier=["id"],
        chunk_size=10,
        dtype_overrides={"name": "VARCHAR2(101)"}
    )
    print("Append successful.")

if __name__ == "__main__":
    main()