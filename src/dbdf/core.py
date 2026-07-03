import polars as pl
import pandas as pd
from urllib.parse import urlparse
from pathlib import Path
from typing import Optional, Union, Iterator

from . import postgres
from . import oracle
from . import files


def read_sql(uri: str, query: str) -> pl.DataFrame:
    # TODO: teruskan chunk_size ke postgres.read_database/oracle.read_database setelah TODO masing-masing selesai
    scheme = urlparse(uri).scheme
    match scheme:
        case "postgresql":
            return postgres.read_database(uri, query)
        case "oracle":
            return oracle.read_database(uri, query)
        case _:
            raise ValueError(f"Skema tidak didukung: {scheme}")


def read_file(path: str, columns: Optional[list[str]] = None) -> pl.DataFrame:
    # TODO: teruskan chunk_size ke files.read_parquet/files.read_csv setelah TODO masing-masing selesai
    suffix = Path(path).suffix.lower()
    if suffix == ".parquet":
        return files.read_parquet(path, columns=columns)
    if suffix == ".csv":
        return files.read_csv(path, columns=columns)
    raise ValueError(f"Format file tidak didukung: {suffix}")

def write_database(
    uri: str,
    df: pl.DataFrame | pd.DataFrame,
    table_name: str,
    mode: str = "append",
    identifier: str | list[str] = None,
    dtype_overrides: dict[type, str] = None,
    chunk_size: int = None
):
    # Convert to polars if df is pandas
    if isinstance(df, pd.DataFrame):
        df = pl.from_pandas(df, include_index=False)

    scheme = urlparse(uri).scheme
    match scheme:
        case "postgresql":
            postgres.write_database(uri, df, table_name, mode, identifier, chunk_size, dtype_overrides)
        case "oracle":
            oracle.write_database(uri, df, table_name, mode, chunk_size, identifier, dtype_overrides)