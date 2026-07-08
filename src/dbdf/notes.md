from typing import Iterator, Optional, Union
import pyarrow as pa

def read_database(uri: str, query: str) -> pl.DataFrame:
    # TODO: chunking pakai cur.fetch_record_batch() (streaming Arrow RecordBatchReader)
    with adbc_driver_postgresql.dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            table = cur.fetch_arrow_table()
    return pl.from_arrow(table)

# CORE.py
def read_sql(uri: str, query: str) -> pl.DataFrame:
    # TODO: teruskan chunk_size ke postgres.read_database/oracle.read_database setelah TODO masing-masing selesai
    scheme = urlparse(uri).scheme
    match scheme:
        case "postgresql":
            return postgres.read_database(uri, query)
        case "oracle":
            return oracle.read_database(uri, query)
        case _:
            raise ValueError(f"Skema tidak didukung: {scheme}")


def read_file(path: str, columns: Optional[list[str]] = None) -> pl.DataFrame:
    # TODO: teruskan chunk_size ke files.read_parquet/files.read_csv setelah TODO masing-masing selesai
    suffix = Path(path).suffix.lower()
    if suffix == ".parquet":
        return files.read_parquet(path, columns=columns)
    if suffix == ".csv":
        return files.read_csv(path, columns=columns)
    raise ValueError(f"Format file tidak didukung: {suffix}")
