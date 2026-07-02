import polars as pl
import pandas as pd
from urllib.parse import urlparse

from . import postgres

def write_database(
    uri: str,
    df: pl.DataFrame | pd.DataFrame,
    table_name: str,
    mode: str = "append",
    identifier: str = None,
    dtype_overrides: dict[type, str] = None
):
    # Convert to polars if df is pandas
    if isinstance(df, pd.DataFrame):
        df = pl.from_pandas(df, include_index=False)

    scheme = urlparse(uri).scheme
    match scheme:
        case "postgresql":
            postgres.write_database(uri, df, table_name, mode, identifier, dtype_overrides)
        case "oracle":
            return