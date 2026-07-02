import polars as pl
from typing import Optional

def read_parquet(
    path: str,
    columns: Optional[list[str]] = None,
) -> pl.DataFrame:
    # TODO: chunking untuk parquet
    lazy = pl.scan_parquet(path)
    if columns:
        lazy = lazy.select(columns)
    return lazy.collect()

def read_csv(
    path: str,
    columns: Optional[list[str]] = None,
    **read_csv_kwargs,
) -> pl.DataFrame:
    # TODO: chunking untuk csv 
    return pl.read_csv(path, columns=columns, **read_csv_kwargs)