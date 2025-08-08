import polars as pl

from typing import Callable

from fastdataframe.core.annotation import ColumnInfo


type cast_function_type = Callable[
    [pl.DataType, pl.DataType, str, ColumnInfo],
    (pl.Expr),
]


def simple_cast(
    src: pl.DataType, tgt: pl.DataType, col_name: str, column_info: ColumnInfo
) -> pl.Expr:
    return pl.col(col_name).cast(tgt, strict=True)


def str_to_bool(
    src: pl.DataType, tgt: pl.DataType, col_name: str, column_info: ColumnInfo
) -> pl.Expr:
    return pl.col(col_name).replace_strict(
        {column_info.bool_false_string: False, column_info.bool_true_string: True},
        return_dtype=tgt,
    )


def str_to_date(
    src: pl.DataType, tgt: pl.DataType, col_name: str, column_info: ColumnInfo
) -> pl.Expr:
    return pl.col(col_name).str.to_date(column_info.date_format, strict=True)


def str_to_datetime(
    src: pl.DataType, tgt: pl.DataType, col_name: str, column_info: ColumnInfo
) -> pl.Expr:
    if column_info.date_format:
        return pl.col(col_name).str.to_datetime(column_info.date_format, strict=True)
    else:
        return pl.col(col_name).str.to_datetime(strict=True)


def str_to_time(
    src: pl.DataType, tgt: pl.DataType, col_name: str, column_info: ColumnInfo
) -> pl.Expr:
    if column_info.date_format:
        return pl.col(col_name).str.to_time(column_info.date_format, strict=True)
    else:
        return pl.col(col_name).str.to_time(strict=True)


def str_to_duration(
    src: pl.DataType, tgt: pl.DataType, col_name: str, column_info: ColumnInfo
) -> pl.Expr:
    return pl.col(col_name).str.to_duration(strict=True)


def str_to_numeric_with_trim(
    src: pl.DataType, tgt: pl.DataType, col_name: str, column_info: ColumnInfo
) -> pl.Expr:
    return pl.col(col_name).str.strip_chars().cast(tgt, strict=True)


def str_to_categorical(
    src: pl.DataType, tgt: pl.DataType, col_name: str, column_info: ColumnInfo
) -> pl.Expr:
    return pl.col(col_name).cast(tgt, strict=True)


def str_to_decimal(
    src: pl.DataType, tgt: pl.DataType, col_name: str, column_info: ColumnInfo
) -> pl.Expr:
    return pl.col(col_name).cast(tgt, strict=True)


custom_cast_functions: dict[
    tuple[type[pl.DataType], type[pl.DataType]], cast_function_type
] = {
    # Existing functions
    (pl.Int64, pl.Float64): simple_cast,
    (pl.String, pl.Boolean): str_to_bool,
    (pl.String, pl.Date): str_to_date,
    
    # String to numeric types (with trimming for whitespace handling)
    (pl.String, pl.Int8): str_to_numeric_with_trim,
    (pl.String, pl.Int16): str_to_numeric_with_trim,
    (pl.String, pl.Int32): str_to_numeric_with_trim,
    (pl.String, pl.Int64): str_to_numeric_with_trim,
    (pl.String, pl.Int128): str_to_numeric_with_trim,
    (pl.String, pl.UInt8): str_to_numeric_with_trim,
    (pl.String, pl.UInt16): str_to_numeric_with_trim,
    (pl.String, pl.UInt32): str_to_numeric_with_trim,
    (pl.String, pl.UInt64): str_to_numeric_with_trim,
    (pl.String, pl.Float32): str_to_numeric_with_trim,
    (pl.String, pl.Float64): str_to_numeric_with_trim,
    
    # String to temporal types
    (pl.String, pl.Datetime): str_to_datetime,
    (pl.String, pl.Time): str_to_time,
    (pl.String, pl.Duration): str_to_duration,
    
    # String to other types
    (pl.String, pl.Categorical): str_to_categorical,
    (pl.String, pl.Decimal): str_to_decimal,
    (pl.String, pl.Binary): simple_cast,
}

__all__ = ["custom_cast_functions", "simple_cast"]
