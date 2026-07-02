import polars as pl
from typing import Optional
import logging

import math

logger = logging.getLogger('df_db_io')

# Polars to PostgreSQL
POLARS_TO_POSTGRES: dict[type, str] = {
    pl.Int8: "SMALLINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INTEGER",
    pl.Int64: "BIGINT",
    pl.UInt8: "SMALLINT",
    pl.UInt16: "INTEGER",
    pl.UInt32: "BIGINT",
    pl.UInt64: "NUMERIC(20)",
    pl.Float32: "REAL",
    pl.Float64: "DOUBLE PRECISION",
    pl.Boolean: "BOOLEAN",
    pl.Date: "DATE",
    pl.Time: "TIME",
    pl.Datetime: "TIMESTAMP",
    pl.Duration: "INTERVAL",
    pl.Binary: "BYTEA",
    pl.Null: "VARCHAR",
    pl.Utf8: "VARCHAR",
    pl.String: "VARCHAR",
    pl.Categorical: "VARCHAR",
}

# Polars to Oracle
POLARS_TO_ORACLE: dict[type, str] = {
    pl.Int8: "NUMBER(3)",
    pl.Int16: "NUMBER(5)",
    pl.Int32: "NUMBER(10)",
    pl.Int64: "NUMBER(19)",
    pl.UInt8: "NUMBER(3)",
    pl.UInt16: "NUMBER(5)",
    pl.UInt32: "NUMBER(10)",
    pl.UInt64: "NUMBER(20)",
    pl.Float32: "BINARY_FLOAT",
    pl.Float64: "BINARY_DOUBLE",
    pl.Boolean: "NUMBER(1)",
    pl.Date: "DATE",
    pl.Time: "TIMESTAMP",
    pl.Datetime: "TIMESTAMP",
    pl.Duration: "INTERVAL DAY TO SECOND",
    pl.Binary: "BLOB",
    pl.Null: "VARCHAR2(255)",
    pl.Utf8: "VARCHAR2(255)",
    pl.String: "VARCHAR2(255)",
    pl.Categorical: "VARCHAR2(255)",
}


def map_polars_dtype_to_sql(
    dtype: pl.DataType,
    dialect: str,
    col_name: str = "",
    sample_values: Optional[pl.Series] = None,
) -> str:
    mapping = POLARS_TO_POSTGRES if dialect == 'postgresql' else POLARS_TO_ORACLE
    dtype_class = type(dtype)
    base_sql_type = mapping.get(dtype_class)

    # Decimal
    if isinstance(dtype, pl.Decimal):
        p = dtype.precision or 18
        s = dtype.scale or 0
        return f"NUMERIC({p},{s})" if dialect == 'postgresql' else f"NUMBER({p},{s})"

    if base_sql_type is None:
        logger.warning(f"Column '{col_name}': Unknown type {dtype}, default VARCHAR")
        return "VARCHAR" if dialect == 'postgresql' else "VARCHAR2(255)"

    # Korektif 1: String -> deteksi panjang -> VARCHAR(N)
    if dtype_class in (pl.Utf8, pl.String, pl.Categorical) and sample_values is not None:
        non_null = sample_values.drop_nulls()
        if len(non_null) > 0:
            max_len = non_null.str.len_chars().max()
            if max_len is not None:
                padded = math.ceil((max_len + 20) / 10) * 10
                if dialect == 'postgresql':
                    corrected = f"VARCHAR({padded})"
                else:
                    corrected = f"VARCHAR2({min(padded, 4000)})"
                logger.info(f"Column '{col_name}': max_len={max_len} → {corrected}")
                return corrected

    # Korektif 2: Kolom semua NULL
    if sample_values is not None and sample_values.null_count() == len(sample_values):
        logger.warning(f"Column '{col_name}': All NULL, default VARCHAR")
        return "VARCHAR" if dialect == 'postgresql' else "VARCHAR2(255)"

    # Korektif 3: Float yang sebenarnya integer
    if dtype_class in (pl.Float64, pl.Float32) and sample_values is not None:
        non_null = sample_values.drop_nulls()
        if len(non_null) > 0:
            try:
                is_int = (non_null == non_null.cast(pl.Int64).cast(pl.Float64)).all()
                if is_int:
                    logger.info(f"Column '{col_name}': Float tapi isinya integer, suggest BIGINT")
            except Exception:
                pass

    return base_sql_type