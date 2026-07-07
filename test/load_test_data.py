"""
Load Test Data into PostgreSQL
-------------------------------
Step 1: Load data.parquet          → seeds the main 'data' table
Step 2: Load data_staging.parquet  → runs the upsert via your write_database()
"""

import polars as pl
import adbc_driver_postgresql.dbapi as pg
import sys
import os
import time

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

import src.dbdf.core as core  # your existing core.py

DB_URI      = "postgresql://postgres:postgres@localhost:5432/mydb2"
TABLE_NAME  = "data3"
IDENTIFIER = ['id']

# ─────────────────────────────────────────────────────────────────────────
# STEP 1 — Seed main table from parquet (run once)
# ─────────────────────────────────────────────────────────────────────────

def seed_main_table():
    print("[1/2] Seeding main 'data' table from batched parquets ...")

    # Polars scans the directory lazily and streams it via Arrow without overloading RAM
    df = pl.read_parquet("test/seed/*.parquet")
    print(f"  Loaded total of {len(df):,} rows from batched files")

    with pg.connect(DB_URI) as conn:
        with conn.cursor() as cur:
            # .to_arrow() works perfectly with multi-file inputs
            cur.adbc_ingest(TABLE_NAME, df.to_arrow(), mode="replace")

            if isinstance(IDENTIFIER, str):
                constraint_cols = f'"{IDENTIFIER}"'
            else:
                constraint_cols = ", ".join(f'"{c}"' for c in IDENTIFIER)

            print(f"  Adding Primary Key constraint to column '{IDENTIFIER}'...")
            cur.execute(f'ALTER TABLE "{TABLE_NAME}" ADD CONSTRAINT unique_id2 UNIQUE ({constraint_cols});')
        conn.commit()

    print(f"  Done — {len(df):,} rows inserted into '{TABLE_NAME}'")

# ─────────────────────────────────────────────────────────────────────────
# STEP 2 — Run upsert from staging parquet using your write_database()
# ─────────────────────────────────────────────────────────────────────────
def run_staging_upsert():
    print("\n[2/2] Running upsert from data_staging.parquet ...")

    df = pl.read_parquet("test/new_data.parquet")
    print(f"  Loaded {len(df):,} rows from staging parquet")
    print(f"  IDs range: {df['id'].min():,} – {df['id'].max():,}")

    start_time = time.perf_counter()
    core.write_database(
        uri         = DB_URI,
        df          = df,
        table_name  = TABLE_NAME,
        mode        = "upsert",
        identifier  = IDENTIFIER,
        chunk_size  = 200_000,
        creds=None,
        db_type="postgresql"

    )
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Waktu eksekusi: {elapsed_time:.6f} detik")
    print("  Done — upsert complete")


# ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # seed_main_table()
    run_staging_upsert()
