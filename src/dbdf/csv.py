import polars as pl
import pandas as pd
from tqdm import tqdm

from .base import DatabaseAdapter

class CsvAdapter(DatabaseAdapter):
    def __init__(self, target: str | dict[str, str]) -> None:
        super().__init__(target=target)

    def read_database(
        self,
        *,
        query: str | None = None,
        chunk_size: int | None = None,
        progress_bar: bool = False,
        **kwargs
    ) -> pl.DataFrame:
        if progress_bar:
            chunk_size = 100_000 if chunk_size is None else chunk_size
            batcher = pl.scan_csv(source=self.target, **kwargs).collect_batches(chunk_size=chunk_size)
            batches = []

            with tqdm(desc=f"Reading {self.target}", unit=" rows") as pbar:
                for batch in batcher:
                    batches.append(batch)
                    pbar.update(len(batch))

            return pl.concat(batches)
        else:
            if chunk_size is not None:
                kwargs["batch_size"] = chunk_size

            return pl.read_csv(source=self.target, **kwargs)

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
    ) -> None:
        if isinstance(data, pd.DataFrame):
            data = pl.from_pandas(data=data)

        if progress_bar:
            chunk_size = 100_000 if chunk_size is None else chunk_size
            total_rows = data.height

            with tqdm(total=total_rows, desc=f"Writing {self.target}", unit=" rows") as pbar:
                for start_idx in range(0, total_rows, chunk_size):
                    chunk = data.slice(start_idx, chunk_size)
                    is_first_chunk = (start_idx == 0)
                    if not is_first_chunk:
                        kwargs["include_header"] = False

                    kwargs["batch_size"] = chunk_size
                    chunk.write_csv(file=self.target, **kwargs)
                    pbar.update(len(chunk))
        else:
            if chunk_size is not None:
                kwargs["batch_size"] = chunk_size

            data.write_csv(file=self.target, **kwargs)
