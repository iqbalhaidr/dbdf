import polars as pl
from urllib.parse import urlparse

from . import postgres

def write_database(
    uri: str,
    df: pl.DataFrame,
    table_name: str,
    mode: str = "append",
    identifier: str = None
):
    scheme = urlparse(uri).scheme
    match scheme:
        case "postgresql":
            postgres.write_database(uri, df, table_name, mode, identifier)
        case "oracle":
            return