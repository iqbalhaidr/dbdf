# NOTE:
'''
login postgres:
psql -h localhost -p 5432 -U postgres
pass: postgres
CREATE DATABASE "db1";
\l
\dt
\d "Override"

login oracle:
docker exec -it oracle sqlplus PDBADMIN/oracle@FREEPDB1
SHOW USER;
SHOW CON_NAME;
SELECT TABLE_NAME FROM USER_TABLES;

-- Table Info
SELECT column_name, data_type, data_length 
FROM user_tab_columns 
WHERE table_name = 'Override';
'''

def _df_info(df):
    total_rows = df.height
    sample_row = df.head(1)

    print("\n================== DATAFRAME INFO ==================")
    print(f"Total Rows: {total_rows:,}")
    print("\nSample Row:")
    print(sample_row)

    print("\n=================== SCHEMA INFO ===================")
    print(f"{'Attribute Name':<20} | {'Datatype':<15}")
    print("-" * 40)

    for col_name, col_type in df.schema.items():
        print(f"{col_name:<20} | {str(col_type):<15}")

    print("===================================================\n")

import sys
import os
import time
import polars as pl
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from dbdf.core import write_database
from dbdf.core import read_database

POSTGRESQL = "postgres://postgres:postgres@localhost:5432/db1"
ORACLE = {"user": "PDBADMIN", "pass": "oracle", "dsn": "localhost:1521/FREEPDB1"}
CSV = "test/seed.csv"
PARQUET = "test/seed.parquet"
CSV_ORIGINAL = "test/seed.original.csv"
CSV_UPSERT = "test/seed.upsert.csv"

df1 = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [85.5, 90.0, 78.5],
    "status": ["active", "active", "inactive"]
})

df2 = pl.DataFrame({
    "id": [1, 3, 4],
    "name": ["Alice", "Charlie", "Diana"],
    "score": [95.0, 78.5, 88.0],
    "status": ["active", "active", "active"] 
})

start_time = time.perf_counter()
print(f"Started: {datetime.now()}")

# Function here

# df = read_database(db_type="postgresql", target=POSTGRESQL, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=True)
# df = read_database(db_type="oracle", target=ORACLE, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=True)
# df = read_database(db_type="csv", target=CSV, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=True)
# df = read_database(db_type="parquet", target=PARQUET, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=True)

# df = read_database(db_type="postgresql", target=POSTGRESQL, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=False)
# df = read_database(db_type="oracle", target=ORACLE, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=False)
# df = read_database(db_type="csv", target=CSV, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=False)
# df = read_database(db_type="parquet", target=PARQUET, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=False)

# _df_info(df)

# write_database(db_type="postgresql", target=POSTGRESQL, data=df, mode="replace", identifiers=None, chunk_size=100_000, overrides=None, progress_bar=True, table_name="Data")
# write_database(db_type="oracle", target=ORACLE, data=df, mode="replace", identifiers=None, chunk_size=100_000, overrides=None, progress_bar=True, table_name="Data")
# write_database(db_type="csv", target=CSV, data=df, chunk_size=100_000, progress_bar=True)
# write_database(db_type="parquet", target=PARQUET, data=df, chunk_size=100_000, progress_bar=True)

# write_database(db_type="postgresql", target=POSTGRESQL, data=df, mode="replace", identifiers=None, chunk_size=100_000, overrides=None, progress_bar=False, table_name="Data")
# write_database(db_type="oracle", target=ORACLE, data=df, mode="replace", identifiers=None, chunk_size=100_000, overrides=None, progress_bar=False, table_name="Data")
# write_database(db_type="csv", target=CSV, data=df, chunk_size=100_000, progress_bar=False)
# write_database(db_type="parquet", target=PARQUET, data=df, chunk_size=100_000, progress_bar=False)

# Upsert Kecil
# write_database(db_type="postgresql", target=POSTGRESQL, data=df1, mode="replace", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=True, table_name="Upsert")
# write_database(db_type="postgresql", target=POSTGRESQL, data=df2, mode="upsert", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=True, table_name="Upsert")
# write_database(db_type="postgresql", target=POSTGRESQL, data=df2, mode="upsert", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=False, table_name="Upsert")

# write_database(db_type="oracle", target=ORACLE, data=df1, mode="replace", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=True, table_name="Upsert")
# write_database(db_type="oracle", target=ORACLE, data=df2, mode="upsert", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=True, table_name="Upsert")
# write_database(db_type="oracle", target=ORACLE, data=df2, mode="upsert", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=False, table_name="Upsert")

# Upsert Besar
# df = read_database(db_type="csv", target=CSV_ORIGINAL, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=True)
# write_database(db_type="postgresql", target=POSTGRESQL, data=df, mode="replace", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=True, table_name="Upsert_Besar")
# df = read_database(db_type="csv", target=CSV_UPSERT, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=True)
# write_database(db_type="postgresql", target=POSTGRESQL, data=df, mode="upsert", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=True, table_name="Upsert_Besar")

df = read_database(db_type="csv", target=CSV_ORIGINAL, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=True)
write_database(db_type="oracle", target=ORACLE, data=df, mode="replace", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=True, table_name="Upsert_Besar")
# df = read_database(db_type="csv", target=CSV_UPSERT, query='SELECT * FROM "Data"', chunk_size=100_000, progress_bar=True)
# write_database(db_type="oracle", target=ORACLE, data=df, mode="upsert", identifiers=["id"], chunk_size=100_000, overrides=None, progress_bar=True, table_name="Upsert_Besar")

# Override
# write_database(db_type="postgresql", target=POSTGRESQL, data=df1, mode="replace", identifiers=["id"], chunk_size=100_000, overrides={"id": "VARCHAR"}, progress_bar=True, table_name="Override")
# write_database(db_type="oracle", target=ORACLE, data=df1, mode="replace", identifiers=["id"], chunk_size=100_000, overrides={"id": "VARCHAR2(255)"}, progress_bar=True, table_name="Override")

end_time = time.perf_counter()
execution_time = end_time - start_time
print(f"time: {execution_time:.6f}s")


