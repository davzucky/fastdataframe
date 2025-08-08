from pydantic.fields import FieldInfo
import polars as pl
import inspect
from typing import get_origin, get_args, Any

type PolarsType = pl.DataType | pl.DataTypeClass


def _handle_collection_type(annotation: Any) -> PolarsType | None:
    """Handle collection types (list, tuple, set) that need special processing."""
    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is None or not args:
        return None

    # Handle set[T] -> pl.List(T) (sets are stored as lists in Polars)
    if origin is set:
        inner_type = pl.DataType.from_python(args[0])
        return pl.List(inner_type)

    return None


def get_polars_type(field_info: FieldInfo) -> PolarsType:
    # Handle case where annotation is None
    if field_info.annotation is None:
        polars_type: PolarsType = pl.String()
    else:
        # First try to handle collection types that need special processing
        collection_type = _handle_collection_type(field_info.annotation)
        if collection_type is not None:
            polars_type = collection_type
        else:
            # Fall back to default Polars type conversion
            polars_type = pl.DataType.from_python(field_info.annotation)

    # Allow explicit Polars type override via metadata
    for arg in field_info.metadata:
        if inspect.isclass(arg) and issubclass(arg, pl.DataType):
            polars_type = arg

    return polars_type
