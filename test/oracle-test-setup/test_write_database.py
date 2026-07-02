"""
Test harness end-to-end untuk write_database().

Jalankan lewat docker-compose (lihat README.md), atau langsung jika sudah
punya Oracle + sqlldr terpasang lokal:

    ORACLE_URI="testuser/TestPass123@localhost:1521/XEPDB1" python3 test_write_database.py
"""

import os
import time
import polars as pl

from write_database import write_database

ORACLE_URI = os.environ.get("ORACLE_URI", "testuser/TestPass123@localhost:1521/XEPDB1")
TABLE = "customers"


def make_sample_df(n: int, start_id: int = 1) -> pl.DataFrame:
    return pl.DataFrame({
        "customer_id": list(range(start_id, start_id + n)),
        "name": [f"Customer {i}" for i in range(start_id, start_id + n)],
        "email": [f"user{i}@example.com" for i in range(start_id, start_id + n)],
    })


def test_append():
    print("\n=== TEST: append (chunked, chunk_size=100) ===")
    df = make_sample_df(1000000, start_id=1)
    result = write_database(ORACLE_URI, df, TABLE, mode="append", chunk_size=100000)
    print("Hasil:", result)
    assert result["status"] == "success"
    assert result["loaded_rows"] == 1000000
    assert result["chunks_processed"] == 10


def test_replace():
    print("\n=== TEST: replace (chunk_size=0, tanpa batching) ===")
    df = make_sample_df(300, start_id=1)
    result = write_database(ORACLE_URI, df, TABLE, mode="replace", chunk_size=0)
    print("Hasil:", result)
    assert result["status"] == "success"
    assert result["loaded_rows"] == 300
    assert result["chunks_processed"] == 1  # chunk_size=0 -> 1 chunk saja


def test_upsert():
    print("\n=== TEST: upsert (sebagian update, sebagian insert baru) ===")
    df_update = make_sample_df(100, start_id=1)     # id 1..100 -> sudah ada dari test_replace, jadi UPDATE
    df_new = make_sample_df(50, start_id=1000)       # id 1000..1049 -> baru, jadi INSERT
    df = pl.concat([df_update, df_new])
    result = write_database(
        ORACLE_URI, df, TABLE,
        mode="upsert",
        key_columns=["customer_id"],
        chunk_size=50,
    )
    print("Hasil:", result)
    assert result["status"] == "success"


def test_empty_dataframe():
    print("\n=== TEST: dataframe kosong (harus di-skip, bukan error) ===")
    df = pl.DataFrame({"customer_id": [], "name": [], "email": []})
    result = write_database(ORACLE_URI, df, TABLE, mode="append")
    print("Hasil:", result)
    assert result["status"] == "skipped"


def test_invalid_mode():
    print("\n=== TEST: mode tidak valid (harus raise ValueError) ===")
    df = make_sample_df(10)
    try:
        write_database(ORACLE_URI, df, TABLE, mode="delete_all")
        raise AssertionError("Seharusnya ValueError dilempar")
    except ValueError as e:
        print("OK, error tertangkap seperti yang diharapkan:", e)


def test_upsert_without_key_columns():
    print("\n=== TEST: upsert tanpa key_columns (harus raise ValueError) ===")
    df = make_sample_df(10)
    try:
        write_database(ORACLE_URI, df, TABLE, mode="upsert")
        raise AssertionError("Seharusnya ValueError dilempar")
    except ValueError as e:
        print("OK, error tertangkap seperti yang diharapkan:", e)


if __name__ == "__main__":
    print(f"Menyambung ke: {ORACLE_URI}")
    time.sleep(3)  # jeda kecil jaga-jaga listener baru siap

    test_append()
    test_replace()
    test_upsert()
    test_empty_dataframe()
    test_invalid_mode()
    test_upsert_without_key_columns()

    print("\nSemua test selesai tanpa error.")
