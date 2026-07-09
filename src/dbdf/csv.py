import polars as pl
import tqdm

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
    
    def write_database_with_progress_bar(
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
        total_rows = df.height
        num_chunks = (total_rows + chunk_size - 1) // chunk_size

        with tqdm(total=num_chunks, desc="Writing CSV", unit="chunk") as pbar:
            for i in range(0, total_rows, chunk_size):
                chunk_df = df.slice(i, min(chunk_size, total_rows - i))
                chunk_df.write_csv(file=self.connection_info, batch_size=chunk_size)
                pbar.update(1)