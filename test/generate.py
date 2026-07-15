import os
import duckdb

SEED_FILE = "test/seed.csv"
UPSERT_FILE = "test/seed.upsert.csv"
SF = 0.5

# Ensure output directory exists
os.makedirs(os.path.dirname(SEED_FILE), exist_ok=True)

con = duckdb.connect()
con.execute("INSTALL tpch; LOAD tpch;")
con.execute(f"CALL dbgen(sf={SF});")

# 1. Create the base view with an 'id' column using ROW_NUMBER()
con.execute("""
    CREATE TEMP VIEW wide_benchmark_view AS 
    SELECT 
        ROW_NUMBER() OVER()::BIGINT AS id,
        l.*,
        o.o_custkey, o.o_orderstatus, o.o_totalprice, o.o_orderdate, 
        o.o_orderpriority, o.o_clerk, o.o_shippriority, o.o_comment,
        p.p_name, p.p_mfgr, p.p_brand, p.p_type, p.p_size, 
        p.p_container, p.p_retailprice, p.p_comment AS p_comment_ext
    FROM lineitem l
    JOIN orders o ON l.l_orderkey = o.o_orderkey
    JOIN part p ON l.l_partkey = p.p_partkey
""")

# Get the total row count to calculate the 50/50 split
total_rows = con.execute("SELECT COUNT(*) FROM wide_benchmark_view").fetchone()[0]
half_rows = total_rows // 2

# 2. Create the Upsert view (50% updates, 50% inserts)
con.execute(f"""
    CREATE TEMP VIEW upsert_view AS 
    
    -- UPDATES (50%): Keep original IDs, but modify a column to simulate an update
    SELECT * REPLACE (o_totalprice * 1.1 AS o_totalprice)
    FROM wide_benchmark_view
    WHERE id <= {half_rows}
    
    UNION ALL
    
    -- INSERTS (50%): Shift the ID by total_rows to guarantee they are completely new records
    SELECT * REPLACE (id + {total_rows} AS id)
    FROM wide_benchmark_view
    WHERE id > {half_rows}
""")

# 3. Export both files
con.execute(f"COPY (SELECT * FROM wide_benchmark_view) TO '{SEED_FILE}' (HEADER, DELIMITER ',');")
con.execute(f"COPY (SELECT * FROM upsert_view) TO '{UPSERT_FILE}' (HEADER, DELIMITER ',');")


# ================== PRINT ANALYSIS ==================
print("\n================== FILE ANALYSIS ==================")
seed_size = os.path.getsize(SEED_FILE) / (1024 * 1024)
upsert_size = os.path.getsize(UPSERT_FILE) / (1024 * 1024)

print(f"Seed File:   {SEED_FILE} | Size: {seed_size:.2f} MB | Rows: {total_rows:,}")
print(f"Upsert File: {UPSERT_FILE} | Size: {upsert_size:.2f} MB | Rows: {total_rows:,}")

print("\n=================== SCHEMA INFO ===================")
print(f"{'Attribute Name':<20} | {'Datatype':<15}")
print("-" * 40)
schema = con.execute("PRAGMA table_info('wide_benchmark_view');").fetchall()
for column in schema:
    print(f"{column[1]:<20} | {column[2]:<15}")

print("-" * 40)
print(f"Total Columns: {len(schema)}")
print("===================================================\n")