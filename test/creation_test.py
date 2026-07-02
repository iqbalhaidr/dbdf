import polars as pl
from datetime import date
import sys
import os
import adbc_driver_postgresql.dbapi as pg

# Ensure we can import your dbdf module
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from src.dbdf import core

DB_URI = "postgresql://postgres:postgres@localhost:5432/mydb2"
TABLE_NAME = "test_inventory2"

def run_creation_test():
    print(f"\n--- Testing Auto-Create for table: '{TABLE_NAME}' ---")

    # 1. Create a dummy DataFrame with mixed types
    df = pl.DataFrame({
        "item_id": [1, 2, 3],
        "sku": ["SKU-100", "SKU-200", "SKU-300"],
        "price": [19.99, 25.50, 9.99],
        "restock_date": [date(2024, 1, 1), date(2024, 6, 15), date(2024, 2, 20)],
        "is_active": [True, True, False]
    })

    df = df.with_columns(
        pl.col("price").cast(pl.Decimal(precision=10, scale=2))
    )

    print("\n[1] Polars Inferred Schema:")
    for col, dtype in df.schema.items():
        print(f"  - {col}: {dtype}")

    # 2. Define human overrides
    # We want 'price' to be strictly NUMERIC(10,2) to avoid floating point issues.
    # We want 'sku' to be restricted to exactly 20 characters.
    overrides = {
        "price": "NUMERIC(10,2)",
        "sku": "VARCHAR(20)"
    }
    print(f"\n[2] Applying Overrides: {overrides}")

    # 3. Fire the pipeline!
    print("\n[3] Running write_database()...")
    core.write_database(
        uri=DB_URI,
        df=df,
        table_name=TABLE_NAME,
        mode="append",
        chunk_size=100_000,
        dtype_overrides=overrides
    )
    print("  Done! Table created and data upserted.")

if __name__ == "__main__":
    run_creation_test()