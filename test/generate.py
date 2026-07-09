import os
import duckdb

FILENAME = "test/seed.csv"
SF = 0.5

con = duckdb.connect()
con.execute("INSTALL tpch; LOAD tpch;")
con.execute(f"CALL dbgen(sf={SF});")
con.execute(f"COPY lineitem TO '{FILENAME}' (HEADER, DELIMITER ',');")

print("\n================== FILE ANALYSIS ==================")
size = os.path.getsize(FILENAME) / (1024 * 1024)
print(f"Size: {size:.2f} MB")
row = con.execute("SELECT COUNT(*) FROM lineitem").fetchone()[0]
print(f"Row: {row:,} rows")

print("\n=================== SCHEMA INFO ===================")
print(f"{'Attribute Name':<20} | {'Datatype':<15}")
print("-" * 40)
schema = con.execute("PRAGMA table_info('lineitem');").fetchall()
for column in schema:
    print(f"{column[1]:<20} | {column[2]:<15}")

print("===================================================\n")
