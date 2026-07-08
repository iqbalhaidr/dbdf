import polars as pl

from .base import DatabaseAdapter

class ParquetAdapter(DatabaseAdapter):
    def __init__(self, connection_info: str | dict) -> None:
        super().__init__(connection_info=connection_info)

    def read_database(
        self
    ) -> pl.DataFrame:
        return pl.read_parquet(source=self.connection_info)

    def write_database(
        self,
        df: pl.DataFrame
    ) -> None: 
        df.write_parquet(file=self.connection_info)