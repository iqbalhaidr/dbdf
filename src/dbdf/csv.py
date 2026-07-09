import polars as pl

from .base import DatabaseAdapter

class CsvAdapter(DatabaseAdapter):
    def __init__(self, connection_info: str | dict) -> None:
        super().__init__(connection_info=connection_info)

    def read_database(
        self,
        query: str = None,
        chunk_size: int = 8192,
        **kwargs
    ) -> pl.DataFrame:
        return pl.read_csv(source=self.connection_info, batch_size=chunk_size)

    def write_database(
        self,
        df: pl.DataFrame,
        table_name: str = None,
        mode: str = "append",
        identifier: list[str] = None,
        if_table_not_exists: str = "fail",
        dtype_overrides: dict[type, str] = None,
        chunk_size: int = 1024,
        schema_name: str = None,
        **kwargs
    ) -> None: 
        df.write_csv(file=self.connection_info, batch_size=chunk_size)
        return True