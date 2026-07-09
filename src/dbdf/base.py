from abc import ABC, abstractmethod
import polars as pl

class DatabaseAdapter(ABC):
    def __init__(self, connection_info: str | dict) -> None:
        self.connection_info = connection_info

    @abstractmethod
    def write_database(
        self,
        df: pl.DataFrame,
        table_name: str,
        mode: str = "append",
        identifier: list[str] = None,
        if_table_not_exists: str = "fail",
        dtype_overrides: dict[type, str] = None,
        chunk_size: int = None,
        schema_name: str = None
    ) -> bool: ...
    
    @abstractmethod
    def write_database_with_progress_bar(
        self,
        df: pl.DataFrame,
        table_name: str,
        mode: str = "append",
        identifier: list[str] = None,
        if_table_not_exists: str = "fail",
        dtype_overrides: dict[type, str] = None,
        chunk_size: int = None,
        schema_name: str = None
    ) -> bool: ...

    @abstractmethod
    def read_database(
        query: str,
        chunk_size: int = None
    ) -> pl.DataFrame: ...