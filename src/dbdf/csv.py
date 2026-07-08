import polars as pl

from .base import DatabaseAdapter

class CsvAdapter(DatabaseAdapter):
    def __init__(self, connection_info: str | dict) -> None:
        super().__init__(connection_info=connection_info)

    def read_database(
        self,
        chunk_size: int = 8192
    ) -> pl.DataFrame:
        return pl.read_csv(source=self.connection_info, batch_size=chunk_size)

    def write_database(
        self,
        df: pl.DataFrame,
        chunk_size: int = 1024,
    ) -> None: 
        df.write_csv(file=self.connection_info, batch_size=chunk_size)