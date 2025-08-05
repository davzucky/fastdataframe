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


custom_cast_functions: dict[
    tuple[type[pl.DataType], type[pl.DataType]], cast_function_type
] = {
    (pl.Int64, pl.Float64): simple_cast,
    (pl.String, pl.Boolean): str_to_bool,
    (pl.String, pl.Date): str_to_date,
}

__all__ = ["custom_cast_functions", "simple_cast"]
