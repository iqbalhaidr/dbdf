import sys
from pathlib import Path
import polars as pl
import pandas as pd
import oracledb

# Ensure the src directory is in the python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from dbdf.core import write_database

def setup_test_table(creds, table_name):
    """Manually creates the table since oracle2.py lacks auto-creation."""
    print(f"Creating table {table_name}...")
    with oracledb.connect(
        user=creds["user"], 
        password=creds["password"], 
        dsn=creds["dsn"]
    ) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f'DROP TABLE {table_name} PURGE')
            except oracledb.DatabaseError:
                pass # Table doesn't exist yet
            
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    ID NUMBER(10,0) PRIMARY KEY,
                    NAME VARCHAR2(100),
                    VALUE BINARY_DOUBLE
                )
            """)

def main():
    # 1. Oracle Lite Default Credentials
    # Note: python-oracledb prefers a single DSN string. If you pass 'port' 
    # alongside a DSN, newer versions throw a "mutually exclusive" error.
    creds = {
        "user": "APP_USER",
        "password": "oracle",
        "dsn": "localhost:1521/FREEPDB1" 
    }
    
    table_name = "EMPLOYEES"
    
    # Pre-create the table
    setup_test_table(creds, table_name)

    # 2. Test Append Mode
    print("Testing APPEND mode...")
    df_append = pd.DataFrame({
        "ID": [1, 2, 3],
        "NAME": ["Alice", "Bob", "Charlie"],
        "VALUE": [10.5, 20.0, 30.0]
    })
    
    write_database(
        uri="oracle://dummy", # core.py requires a URI for the scheme parsing
        creds=creds,
        df=df_append,
        table_name=table_name,
        mode="append",
        db_type="oracle",
        chunk_size=100
    )
    print("Append successful.")

    # 3. Test Upsert Mode
    print("Testing UPSERT mode...")
    df_upsert = pd.DataFrame({
        "ID": [2, 3, 4], # 2 and 3 exist (update), 4 is new (insert)
        "NAME": ["Bob Updated", "Charlie", "David"],
        "VALUE": [99.9, 30.0, 40.5]
    })
    
    write_database(
        uri="oracle://dummy",
        creds=creds,
        df=df_upsert,
        table_name=table_name,
        mode="upsert",
        identifier=["ID"],
        db_type="oracle",
        chunk_size=100
    )
    print("Upsert successful.")

if __name__ == "__main__":
    main()