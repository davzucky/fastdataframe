from typing import Annotated, Any, get_args, get_origin
import polars as pl
import inspect

type PolarsType = pl.DataType | pl.DataTypeClass


def get_polars_type(field_type: Any) -> PolarsType:
    if get_origin(field_type) is not Annotated:
        return pl.DataType.from_python(field_type)

    annotated_args = get_args(field_type)
    for arg in annotated_args:
        if inspect.isclass(arg) and issubclass(arg, pl.DataType):
            return arg

    return pl.DataType.from_python(annotated_args[0])
