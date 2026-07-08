import polars as pl
import pandas as pd

from .postgres import PostgresAdapter
from .oracle import OracleAdapter

def write_database(
    db_type: str,
    connection_info: str | dict,
    df: pl.DataFrame | pd.DataFrame,
    table_name: str,
    mode: str = "append",
    identifier: list[str] = None,
    if_table_not_exists: str = "fail", # "fail", "create"
    dtype_overrides: dict[type, str] = None,
    chunk_size: int = None,
    schema_name: str = None
):
    # Convert to polars if df is pandas
    if isinstance(df, pd.DataFrame):
        df = pl.from_pandas(df, include_index=False)
    
    adapter = _get_adapter(db_type, connection_info)
    adapter.write_database(df, table_name, mode, identifier, if_table_not_exists, dtype_overrides, chunk_size, schema_name)

ADAPTERS = {
    "postgresql": PostgresAdapter,
    "oracle": OracleAdapter
}

def _get_adapter(db_type: str, connection_info: str | dict):
    if db_type not in ADAPTERS:
        raise ValueError(f"Arg db_type={db_type} invalid")
    return ADAPTERS[db_type](connection_info)