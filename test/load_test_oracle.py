"""
Load Test Data into Oracle
-------------------------------
Step 1: Inisialisasi awal tabel 'data_test' menggunakan file dari test/seed/
Step 2: Uji performa operasi append, replace, dan upsert menggunakan test/new_data.parquet
"""

import polars as pl
import sys
import os
import time
import glob

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

import src.dbdf.core as core

# Konfigurasi kredensial Oracle
ORACLE_CREDS = {
    "user": "APP_USER",
    "password": "oracle",
    "dsn": "localhost:1521/FREEPDB1"
}

TABLE_NAME  = "DATA_TEST2"
IDENTIFIER  = ['id', 'email']
CHUNK_SIZE  = 100_000

# ─────────────────────────────────────────────────────────────────────────
# STEP 1 — Seed main table (Hanya dijalankan sekali di awal)
# ─────────────────────────────────────────────────────────────────────────
def seed_main_table():
    print(f"\n[1/2] Seeding awal tabel '{TABLE_NAME}' dari test/seed/*.parquet ...")
    
    parquet_files = sorted(glob.glob("test/seed/*.parquet"))
    if not parquet_files:
        print("  [ERROR] File seed parquet tidak ditemukan!")
        return

    first_batch = True
    total_rows = 0
    t0 = time.perf_counter()
    
    for file in parquet_files:
        df = pl.read_parquet(file)
        mode = "replace" if first_batch else "append"
        
        core.write_database(
            uri="APP_USER/oracle@localhost:1521/FREEPDB1",
            creds=ORACLE_CREDS,
            df=df,
            table_name=TABLE_NAME,
            mode=mode,
            identifier=IDENTIFIER,
            chunk_size=CHUNK_SIZE,
            db_type="oracle"
        )
        
        first_batch = False
        total_rows += len(df)
        print(f"  -> Inserted {len(df):,} baris dari {os.path.basename(file)}. (Total: {total_rows:,})")

    t1 = time.perf_counter()
    print(f"  [SEED SELESAI] {total_rows:,} baris dimasukkan dalam {t1-t0:.2f} detik.\n")

# ─────────────────────────────────────────────────────────────────────────
# STEP 2 — Fungsi Uji Performa (Append, Replace, Upsert)
# ─────────────────────────────────────────────────────────────────────────
def test_mode(mode: str):
    print(f"--- Menguji mode: {mode.upper()} ---")
    
    # Load test data
    df = pl.read_parquet("test/new_data.parquet")
    print(f"  Data uji termuat: {len(df):,} baris.")
    
    start_time = time.perf_counter()
    core.write_database(
        uri=None,
        creds=ORACLE_CREDS,
        df=df,
        table_name=TABLE_NAME,
        mode=mode,
        identifier=IDENTIFIER,
        chunk_size=CHUNK_SIZE,
        db_type="oracle"
    )
    end_time = time.perf_counter()
    
    elapsed_time = end_time - start_time
    print(f"  [HASIL] Waktu eksekusi {mode.upper()}: {elapsed_time:.6f} detik\n")


# ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Lakukan seeding (Beri komentar jika database sudah memiliki data seed)
    # seed_main_table()

    # 2. Uji operasi ke database menggunakan new_data.parquet
    # PERHATIAN: 
    # - 'append' akan menambah data dan mungkin menyebabkan constraint error jika ada ID ganda dan tabel sudah dipasangi Primary Key.
    # - 'upsert' akan memperbarui data yang ada dan menambah yang baru (aman dari duplicate error).
    # - 'replace' akan menghapus/truncate seluruh data seed Anda!
    
    print("[2/2] Memulai Test Performa menggunakan new_data.parquet...\n")
    
    # Jalankan yang ingin Anda uji satu per satu:
    
    # test_mode("append")
    test_mode("upsert")
    
    # Pastikan Anda siap kehilangan data seed jika menjalankan ini:
    # test_mode("replace")