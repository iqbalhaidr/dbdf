"""
Load Test Data into Oracle via write_database()
-------------------------------------------------
Adaptasi dari load_test_data.py (yang aslinya untuk PostgreSQL + adbc)
supaya jalan dengan write_database.py berbasis Oracle + SQL*Loader.

Step 1: Seed tabel 'data' dari test/seed/*.parquet   (mode="replace"/"append")
Step 2: Upsert dari test/new_data.parquet             (mode="upsert")

Jalankan:
    ORACLE_URI="testuser/TestPass123@localhost:1521/XEPDB1" python3 load_test_data_oracle.py
"""

import os
import glob
import time
import polars as pl

from write_database import write_database

ORACLE_URI = os.environ.get("ORACLE_URI", "testuser/TestPass123@localhost:1521/XEPDB1")
TABLE_NAME = "data"
KEY_COLUMNS = ["id"]  # ganti ke ["id", "email"] kalau mau composite key
CHUNK_SIZE = 200_000   # ukuran batch per load ke SQL*Loader
SCHEMA_NAME = "testuser"


# ─────────────────────────────────────────────────────────────────────────
# STEP 1 — Seed tabel utama dari parquet batch (test/seed/*.parquet)
# ─────────────────────────────────────────────────────────────────────────
def seed_main_table():
    print("[1/2] Seeding tabel 'data' dari test/seed/*.parquet ...")

    files = sorted(glob.glob("test/seed/*.parquet"))
    if not files:
        raise FileNotFoundError(
            "Tidak ada file di test/seed/*.parquet — jalankan generate_test_data.py dulu."
        )

    total_loaded = 0
    for idx, path in enumerate(files):
        # Baca satu file batch saja ke memori (bukan seluruh direktori sekaligus),
        # supaya RAM yang dipakai terbatas seukuran satu file, bukan total semua batch.
        df = pl.read_parquet(path)
        print(f"  Batch {idx}: {path} ({len(df):,} baris)")

        mode = "replace" if idx == 0 else "append"
        result = write_database(
            ORACLE_URI, df, SCHEMA_NAME, TABLE_NAME,
            mode=mode,
            chunk_size=CHUNK_SIZE,
        )
        print(f"    -> {result}")
        total_loaded += result.get("loaded_rows", 0)

        del df

    print(f"  Selesai — total {total_loaded:,} baris di-seed ke '{TABLE_NAME}'")


# ─────────────────────────────────────────────────────────────────────────
# STEP 2 — Upsert dari staging parquet
# ─────────────────────────────────────────────────────────────────────────
def run_staging_upsert():
    print("\n[2/2] Menjalankan upsert dari test/new_data.parquet ...")

    df = pl.read_parquet("test/new_data.parquet")
    print(f"  Loaded {len(df):,} baris dari staging parquet")
    print(f"  Rentang id: {df['id'].min():,} - {df['id'].max():,}")

    start_time = time.perf_counter()
    result = write_database(
        uri=ORACLE_URI,
        schema_name=SCHEMA_NAME,
        df=df,
        table_name=TABLE_NAME,
        mode="upsert",
        key_columns=KEY_COLUMNS,
        chunk_size=CHUNK_SIZE,
    )
    elapsed = time.perf_counter() - start_time

    print(f"  Hasil: {result}")
    print(f"  Waktu eksekusi: {elapsed:.2f} detik")


if __name__ == "__main__":
    seed_main_table()
    run_staging_upsert()
