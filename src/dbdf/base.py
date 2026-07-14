from abc import ABC, abstractmethod
import polars as pl
import pandas as pd

class DatabaseAdapter(ABC):
    def __init__(self, target: str | dict[str, str]) -> None:
        self.target = target

    @abstractmethod
    def read_database(
        self,
        *,
        query: str | None = None,
        chunk_size: int | None = None,
        progress_bar: bool = False,
        **kwargs
    ) -> pl.DataFrame: ...

    @abstractmethod
    def write_database(
        self, 
        data: pl.DataFrame | pd.DataFrame, 
        *,
        mode: str = "replace",
        identifiers: list[str] | None = None,
        chunk_size: int | None = None,
        overrides: dict[str, str] | None = None,
        progress_bar: bool = False,
        **kwargs
    ) -> None: ...
    