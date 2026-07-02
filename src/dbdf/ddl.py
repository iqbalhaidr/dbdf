import polars as pl
import pandas as pd
from typing import Literal, Optional, Union
import logging

from .converters.dataframe import ensure_polars
from .type_mapping.polars_to_sql import map_polars_dtype_to_sql

logger = logging.getLogger('df_db_io')

def _quote(identifier: str, dialect: str) -> str:
    return f'"{identifier}"'

def generate_ddl(
    dataframe: Union[pl.DataFrame, pd.DataFrame],
    table_name: str,
    dialect: Literal["postgresql", "oracle"] = "postgresql",
    dtype_overrides: Optional[dict[str, str]] = None,
    primary_key: Optional[list[str]] = None,
    nullable_columns: Optional[list[str]] = None,
    if_not_exists: bool = True,
    sample_size: int = 10_000,
) -> str:
    df = ensure_polars(dataframe)
    dtype_overrides = dtype_overrides or {}
    primary_key = primary_key or []

    not_null_cols = set(primary_key)
    if nullable_columns is not None:
        not_null_cols = set(df.columns) - set(nullable_columns)

    sample_df = df.head(min(sample_size, len(df)))

    column_defs = []
    for col_name in df.columns:
        if col_name in dtype_overrides:
            sql_type = dtype_overrides[col_name]
            logger.info(f"Column '{col_name}': override → {sql_type}")
        else:
            sample_col = sample_df.get_column(col_name)
            sql_type = map_polars_dtype_to_sql(
                dtype=df.schema[col_name],
                dialect=dialect,
                col_name=col_name,
                sample_values=sample_col,
            )
        nullable = "" if col_name not in not_null_cols else " NOT NULL"
        column_defs.append(f"    {_quote(col_name, dialect)}  {sql_type}{nullable}")

    exists_clause = "IF NOT EXISTS " if if_not_exists and dialect == 'postgresql' else ""
    qualified_table = _quote(table_name, dialect)

    ddl_lines = [f"CREATE TABLE {exists_clause}{qualified_table} ("]
    ddl_lines.append(",\n".join(column_defs))
    if primary_key:
        pk_cols = ", ".join(_quote(c, dialect) for c in primary_key)
        ddl_lines.append(f",\n    PRIMARY KEY ({pk_cols})")
    ddl_lines.append(");")

    ddl = "\n".join(ddl_lines)
    logger.info(f"DDL for '{table_name}' ({dialect}): {len(df.columns)} columns")
    return ddl