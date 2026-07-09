import sys
import os
import time

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from dbdf.core import write_database
from dbdf.core import read_database
from dbdf.core import write_database_with_progress_bar

df = read_database(db_type="csv", connection_info="test/seed.csv", query=None, chunk_size=8192)
write_database(db_type="parquet", connection_info="test/seed.parquet", df=df, table_name="")


t0 = time.perf_counter()
write_database(db_type="postgresql", connection_info={"host": "localhost", "port": 5432, "user": "postgres", "password": "pass123", "database": "test"}, df=df, table_name="test_table", chunk_size=100000, if_table_not_exists="create")
t1 = time.perf_counter()
print(f"[PROCESS Write Postgres Without Progress Bar]={t1-t0:.2f}s")

t2 = time.perf_counter()
write_database_with_progress_bar(db_type="postgresql", connection_info={"host": "localhost", "port": 5432, "user": "postgres", "password": "pass123", "database": "test"}, df=df, table_name="test_table", chunk_size=100000, if_table_not_exists="create")
t1 = time.perf_counter()
t3 = time.perf_counter()
print(f"[PROCESS Write Postgres With Progress Bar]={t3-t2:.2f}s")

oracle_conn_info = {
    "user": "system",                  
    "password": "oracle",             
    "dsn": "localhost:1521/FREEPDB1"  
}

# t0 = time.perf_counter()
# write_database(
#     db_type="oracle", 
#     connection_info=oracle_conn_info, 
#     df=df, 
#     table_name="test_table", 
#     chunk_size=100000
# )
# t1 = time.perf_counter()
# print(f"[PROCESS Write Oracle Without Progress Bar]={t1-t0:.2f}s")

# t2 = time.perf_counter()
# write_database_with_progress_bar(
#     db_type="oracle", 
#     connection_info=oracle_conn_info, 
#     df=df, 
#     table_name="test_table", 
#     chunk_size=100000
# )
# t3 = time.perf_counter()
# print(f"[PROCESS Write Oracle With Progress Bar]={t3-t2:.2f}s")



