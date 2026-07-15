import polars as pl
import pandas as pd

from .postgresql import PostgresAdapter
from .oracle import OracleAdapter
from .csv import CsvAdapter
from .parquet import ParquetAdapter


def read_database(
    db_type: str,
    target: str | dict[str, str],
    *,
    query: str | None = None,
    chunk_size: int | None = None,
    progress_bar: bool = False,
    **kwargs,
) -> pl.DataFrame:
    """
    Reads data from a specified database or file format into a Polars DataFrame.

    Args:
        db_type (str): The target database or file format. Supported values are "postgresql", "oracle", "csv", and "parquet".
        target (str | dict[str, str]): The connection URI, file path, or dictionary containing connection credentials (e.g., {"user": "...", "pass": "...", "dsn": "..."} for Oracle).
        query (str | None, optional): The SQL query to execute. Required for relational databases. Defaults to None.
        chunk_size (int | None, optional): The number of rows to fetch per batch. If `progress_bar` is True, this defaults to 100,000. Defaults to None.
        progress_bar (bool, optional): If True, displays a tqdm progress bar during the read operation. Defaults to False.
        **kwargs: Additional keyword arguments passed directly to the underlying reader or database adapter.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the fetched data.

    Raises:
        ValueError: If the provided `db_type` is not mapped to a valid adapter.

    Examples:
        Read from a PostgreSQL database using a connection URI:
        >>> POSTGRESQL = "postgres://postgres:postgres@localhost:5432/db1"
        >>> df = read_database(
        ...     db_type="postgresql",
        ...     target=POSTGRESQL,
        ...     query='SELECT * FROM "Data"',
        ...     chunk_size=100_000,
        ...     progress_bar=True
        ... )

        Read directly from a local CSV file:
        >>> df = read_database(
        ...     db_type="csv",
        ...     target="test/seed.csv",
        ...     chunk_size=100_000
        ... )
    """
    adapter = _get_adapter(db_type, target)
    return adapter.read_database(query=query, chunk_size=chunk_size, progress_bar=progress_bar, **kwargs)


def write_database(
    db_type: str,
    target: str | dict[str, str],
    data: pl.DataFrame | pd.DataFrame,
    *,
    mode: str = "replace",
    identifiers: list[str] | None = None,
    chunk_size: int | None = None,
    overrides: dict[str, str] | None = None,
    progress_bar: bool = False,
    **kwargs,
) -> None:
    """
    Writes a DataFrame to a specified database or file format.

    If a Pandas DataFrame is provided, it is automatically converted to Polars before writing.

    Args:
        db_type (str): The target database or file format. Supported values are "postgresql", "oracle", "csv", and "parquet".
        target (str | dict[str, str]): The connection URI, file path, or dictionary containing connection credentials.
        data (pl.DataFrame | pd.DataFrame): The DataFrame to write to the target destination.
        mode (str, optional): The write behavior. Supported values are "replace" (truncate and insert), "append", and "upsert". Defaults to "replace".
        identifiers (list[str] | None, optional): A list of column names acting as unique keys for matching records. Required if `mode` is "upsert". Defaults to None.
        chunk_size (int | None, optional): The number of rows to write per batch. If `progress_bar` is True, this defaults to 100,000. Defaults to None.
        overrides (dict[str, str] | None, optional): A dictionary mapping column names to exact database data types (e.g., {"col1": "VARCHAR(255)"}) to override automatic type inference during table creation. Defaults to None.
        progress_bar (bool, optional): If True, displays a tqdm progress bar during the write operation. Defaults to False.
        **kwargs: Additional database-specific keyword arguments. 
            - table_name (str): The name of the target table. Required for PostgreSQL and Oracle adapters.
            - schema_name (str, optional): The target schema name. Used exclusively by the Oracle adapter.
            - Other keyword arguments are passed directly to underlying tools (e.g., `include_header` for CSVs).

    Returns:
        None

    Raises:
        ValueError: If the provided `db_type` or `mode` is not valid.

    Examples:
        Upsert a Polars DataFrame into a PostgreSQL table using an identifier:
        >>> import polars as pl
        >>> df = pl.DataFrame({"id": [1, 3, 4], "status": ["active", "active", "active"]})
        >>> write_database(
        ...     db_type="postgresql",
        ...     target="postgres://postgres:postgres@localhost:5432/db1",
        ...     data=df,
        ...     mode="upsert",
        ...     identifiers=["id"],
        ...     table_name="Upsert"
        ... )

        Write to an Oracle database while overriding a specific column's data type:
        >>> import polars as pl
        >>> df = pl.DataFrame({"id": [1, 3, 4], "status": ["active", "active", "active"]})
        >>> ORACLE = {"user": "PDBADMIN", "pass": "oracle", "dsn": "localhost:1521/FREEPDB1"}
        >>> write_database(
        ...     db_type="oracle",
        ...     target=ORACLE,
        ...     data=df,
        ...     mode="replace",
        ...     overrides={"id": "VARCHAR2(255)"},
        ...     table_name="Override"
        ... )
    """
    adapter = _get_adapter(db_type, target)
    adapter.write_database(
        data,
        mode=mode,
        identifiers=identifiers,
        chunk_size=chunk_size,
        overrides=overrides,
        progress_bar=progress_bar,
        **kwargs,
    )


ADAPTERS = {"postgresql": PostgresAdapter, "oracle": OracleAdapter, "csv": CsvAdapter, "parquet": ParquetAdapter}


def _get_adapter(db_type: str, target: str | dict[str, str]):
    if db_type not in ADAPTERS:
        raise ValueError(f"Arg db_type={db_type} invalid")
    return ADAPTERS[db_type](target=target)
