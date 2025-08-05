from pydantic.fields import FieldInfo
import polars as pl
import inspect

type PolarsType = pl.DataType | pl.DataTypeClass


def get_polars_type(field_info: FieldInfo) -> PolarsType:
    # Handle case where annotation is None
    if field_info.annotation is None:
        polars_type: PolarsType = pl.String()
    else:
        polars_type = pl.DataType.from_python(field_info.annotation)

    for arg in field_info.metadata:
        if inspect.isclass(arg) and issubclass(arg, pl.DataType):
            polars_type = arg

    return polars_type
