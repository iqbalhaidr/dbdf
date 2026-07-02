import polars as pl
import pandas as pd
from typing import Union

DataFrameType = Union[pl.DataFrame, pd.DataFrame]

def ensure_polars(df: DataFrameType) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        return df
    elif isinstance(df, pd.DataFrame):
        return pl.from_pandas(df, include_index=True)
    else:
        raise TypeError(f"Expected pl.DataFrame or pd.DataFrame, got {type(df).__name__}")
    
def to_records(df: pl.DataFrame) -> list[tuple]:
    return [tuple(row) for row in df.iter_rows()]