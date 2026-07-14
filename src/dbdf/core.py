import polars as pl
import pandas as pd

from .postgresql import PostgresAdapter
from .oracle import OracleAdapter
from .csv import CsvAdapter
from .parquet import ParquetAdapter

def read_database(
    db_type: str,
    target: str | dict[str, str],
    *,
    query: str | None = None,
    chunk_size: int | None = None,
    progress_bar: bool = False,
    **kwargs
) -> pl.DataFrame:
    adapter = _get_adapter(db_type, target)
    return adapter.read_database(
        query=query, 
        chunk_size=chunk_size, 
        progress_bar=progress_bar,
        **kwargs
    )

def write_database(
    db_type: str,
    target: str | dict[str, str],
    data: pl.DataFrame | pd.DataFrame, 
    *,
    mode: str = "replace",
    identifiers: list[str] | None = None,
    chunk_size: int | None = None,
    overrides: dict[str, str] | None = None,
    progress_bar: bool = False,
    **kwargs
) -> None:
    adapter = _get_adapter(db_type, target)
    adapter.write_database(
        data, 
        mode=mode, 
        identifiers=identifiers,
        chunk_size=chunk_size, 
        overrides=overrides,
        progress_bar=progress_bar,
        **kwargs
    )

ADAPTERS = {
    "postgresql": PostgresAdapter,
    "oracle": OracleAdapter,
    "csv": CsvAdapter,
    "parquet": ParquetAdapter
}

def _get_adapter(db_type: str, target: str | dict[str, str]):
    if db_type not in ADAPTERS:
        raise ValueError(f"Arg db_type={db_type} invalid")
    return ADAPTERS[db_type](target=target)
