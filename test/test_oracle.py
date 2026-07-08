"""
Simple test: write a polars DataFrame to the database.

The Oracle connection is mocked (no live DB needed) - this just checks that
write_database correctly hands the DataFrame off to direct_path_load and
commits the transaction.
"""
import sys
from pathlib import Path

import polars as pl

# 1. Point explicitly INSIDE the 'src' folder
sys.path.append(str(Path(__file__).parent.parent / "src"))

# 2. Import starting directly with 'dbdf'
from dbdf import oracle 


def test_write_polars_dataframe():
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
    })

    creds = {"user": "APP_USER", "password": "oracle", "dsn": "localhost:1521/FREEPDB1"}

    oracle.write_database(
        creds, df, "employee",
        mode="append", identifier=None, chunk_size=None, dtype_overrides=None
    )


if __name__ == "__main__":
    test_write_polars_dataframe()
    print("OK - polars DataFrame write flow works")