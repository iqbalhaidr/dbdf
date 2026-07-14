import os
import duckdb

FILENAME = "test/seed.csv"
SF = 0.5

# Ensure output directory exists
os.makedirs(os.path.dirname(FILENAME), exist_ok=True)

con = duckdb.connect()
con.execute("INSTALL tpch; LOAD tpch;")
con.execute(f"CALL dbgen(sf={SF});")

con.execute("""
    CREATE TEMP VIEW wide_benchmark_view AS 
    SELECT 
        l.*,
        o.o_custkey, o.o_orderstatus, o.o_totalprice, o.o_orderdate, 
        o.o_orderpriority, o.o_clerk, o.o_shippriority, o.o_comment,
        p.p_name, p.p_mfgr, p.p_brand, p.p_type, p.p_size, 
        p.p_container, p.p_retailprice, p.p_comment AS p_comment_ext
    FROM lineitem l
    JOIN orders o ON l.l_orderkey = o.o_orderkey
    JOIN part p ON l.l_partkey = p.p_partkey
""")

con.execute(f"COPY (SELECT * FROM wide_benchmark_view) TO '{FILENAME}' (HEADER, DELIMITER ',');")

print("\n================== FILE ANALYSIS ==================")
size = os.path.getsize(FILENAME) / (1024 * 1024)
print(f"Size: {size:.2f} MB")
row = con.execute("SELECT COUNT(*) FROM wide_benchmark_view").fetchone()[0]
print(f"Row: {row:,} rows")

print("\n=================== SCHEMA INFO ===================")
print(f"{'Attribute Name':<20} | {'Datatype':<15}")
print("-" * 40)
schema = con.execute("PRAGMA table_info('wide_benchmark_view');").fetchall()
for column in schema:
    print(f"{column[1]:<20} | {column[2]:<15}")

print("-" * 40)
print(f"Total Columns: {len(schema)}")
print("===================================================\n")