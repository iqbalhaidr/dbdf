# DBDF (DataBase DataFrame)

Library for high-performance database reading and writing from-to dataframe, powered by Polars.

## Overview
DBDF provides a unified, highly optimized interface to read from and write to various data sources (PostgreSQL, Oracle, CSV, Parquet). By leveraging the Polars engine under the hood, it ensures fast execution and minimal memory overhead while maintaining a simple, consistent API across different database dialects.

## Installation

Install directly from the GitHub repository using `pip` (Recommended using venv):

```bash
pip install git+https://github.com/iqbalhaidr/dbdf.git
```

## Supported Targets
- **PostgreSQL** (`"postgresql"`)
- **Oracle DB** (`"oracle"`)
- **CSV Files** (`"csv"`)
- **Parquet Files** (`"parquet"`)

## Quick Start

### Reading Data
Read data into a Polars DataFrame using `read_database`.

```python
from dbdf.core import read_database

# PostgreSQL Example
POSTGRES_URI = "postgres://username:password@localhost:5432/db1"
df = read_database(
    db_type="postgresql",
    target=POSTGRES_URI,
    query='SELECT * FROM "Data"',
    chunk_size=100_000,
    progress_bar=True
)

# Local CSV Example
df_csv = read_database(
    db_type="csv",
    target="data/seed.csv",
    chunk_size=100_000
)
```

### Writing Data
Write a Polars (or Pandas) DataFrame to a target using `write_database`. It supports `replace`, `append`, and `upsert` modes.

```python
from dbdf.core import write_database
import polars as pl

df = pl.DataFrame({
    "id": [1, 2, 3],
    "status": ["active", "active", "inactive"]
})

# Upserting into PostgreSQL
write_database(
    db_type="postgresql",
    target=POSTGRES_URI,
    data=df,
    mode="upsert",
    identifiers=["id"],
    table_name="UsersTarget",
    progress_bar=True
)

# Writing to Oracle (with type overrides)
ORACLE_CREDS = {"user": "admin", "pass": "secret", "dsn": "localhost:1521/FREEPDB1"}
write_database(
    db_type="oracle",
    target=ORACLE_CREDS,
    data=df,
    mode="replace",
    table_name="UsersTarget",
    overrides={"id": "NUMBER(10)"}
)
```

## Contributor
| Name |
|------|
| Ferdinand Gabe Tua Sinaga |
| Muhammad Iqbal Haidar |
| Rafa Abdussalam Danadyaksa |