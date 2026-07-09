import polars as pl

from .base import DatabaseAdapter

class ParquetAdapter(DatabaseAdapter):
    def __init__(self, connection_info: str | dict) -> None:
        super().__init__(connection_info=connection_info)

    def read_database(
        self,
        query: str = None, 
        chunk_size: int = None,
        **kwargs
    ) -> pl.DataFrame:
        return pl.read_parquet(source=self.connection_info)

    def write_database(
        self,
        df: pl.DataFrame,
        table_name: str = None,
        mode: str = "append",
        identifier: list[str] = None,
        if_table_not_exists: str = "fail",
        dtype_overrides: dict[type, str] = None,
        chunk_size: int = None,
        schema_name: str = None,
        **kwargs
    ) -> None: 
        df.write_parquet(file=self.connection_info)