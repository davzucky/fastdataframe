from pydantic.fields import FieldInfo
from pydantic import BaseModel
import polars as pl
import inspect
import datetime as dt
from typing import get_origin, get_args, Any, Union
from fastdataframe.core.column import ColumnDefinition
from fastdataframe.core.dtypes import (
    Binary,
    Boolean,
    Date,
    Decimal,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    Time,
    Timestamp,
)
from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)
from fastdataframe.core.types_helper import unwrap_annotated_optional

type PolarsType = pl.DataType | pl.DataTypeClass


def _handle_collection_type(
    annotation: Any, alias_type: str = "serialization"
) -> PolarsType | None:
    """Handle collection types (list, tuple, set) that need special processing."""
    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is None or not args:
        return None

    # Handle set[T] -> pl.List(T) (sets are stored as lists in Polars)
    if origin is set:
        inner_type = pl.DataType.from_python(args[0])
        return pl.List(inner_type)

    # Handle collections of BaseModel types (list[BaseModel], etc.)
    if origin in (list, tuple, set) and args:
        inner_type = args[0]
        if inspect.isclass(inner_type) and issubclass(inner_type, BaseModel):
            # Convert BaseModel to Struct first
            basemodel_struct = _convert_basemodel_to_struct(inner_type, alias_type)
            if origin is set:
                return pl.List(basemodel_struct)
            else:
                # Both list and tuple map to List in Polars
                return pl.List(basemodel_struct)

    # Handle Union types (including Optional[BaseModel])
    if origin is Union and args:
        # Check if it's Optional[BaseModel] (Union[BaseModel, NoneType])
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            # This is an Optional[T] case
            inner_type = non_none_args[0]
            if inspect.isclass(inner_type) and issubclass(inner_type, BaseModel):
                # Convert BaseModel to Struct
                return _convert_basemodel_to_struct(inner_type, alias_type)

    return None


def _convert_basemodel_to_struct(
    model_class: type[BaseModel], alias_type: str = "serialization"
) -> pl.Struct:
    """Convert a Pydantic BaseModel class to a Polars Struct type.

    Args:
        model_class: The BaseModel class to convert
        alias_type: Whether to use "serialization" or "validation" aliases

    Returns:
        pl.Struct: A Polars Struct type representing the BaseModel
    """
    alias_func = (
        get_serialization_alias
        if alias_type == "serialization"
        else get_validation_alias
    )

    fields = []
    for field_name, field_info in model_class.model_fields.items():
        field_alias = alias_func(field_info, field_name)

        # Recursively handle nested types
        field_type: PolarsType
        if field_info.annotation is None:
            field_type = pl.String()
        elif inspect.isclass(field_info.annotation) and issubclass(
            field_info.annotation, BaseModel
        ):
            # Nested BaseModel - recursively convert
            field_type = _convert_basemodel_to_struct(field_info.annotation, alias_type)
        else:
            # Check for collection types first
            collection_type = _handle_collection_type(field_info.annotation, alias_type)
            if collection_type is not None:
                field_type = collection_type
            else:
                # Use standard Polars conversion
                field_type = pl.DataType.from_python(field_info.annotation)

        # Allow explicit Polars type override via metadata
        for arg in field_info.metadata:
            if inspect.isclass(arg) and issubclass(arg, pl.DataType):
                field_type = arg
                break

        fields.append(pl.Field(field_alias, field_type))

    return pl.Struct(fields)


def _handle_basemodel_type(annotation: Any) -> PolarsType | None:
    """Handle BaseModel types by converting them to Polars Struct."""
    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return _convert_basemodel_to_struct(annotation)
    return None


def get_polars_type_from_column(
    column: ColumnDefinition, alias_type: str = "serialization"
) -> PolarsType:
    """Convert a ColumnDefinition to a Polars type."""
    dtype = column.info.dtype
    if dtype is not None:
        if isinstance(dtype, Boolean):
            return pl.Boolean
        if isinstance(dtype, String):
            return pl.String
        if isinstance(dtype, Binary):
            return pl.Binary
        if isinstance(dtype, Int8):
            return pl.Int8
        if isinstance(dtype, Int16):
            return pl.Int16
        if isinstance(dtype, Int32):
            return pl.Int32
        if isinstance(dtype, Int64):
            return pl.Int64
        if isinstance(dtype, Float32):
            return pl.Float32
        if isinstance(dtype, Float64):
            return pl.Float64
        if isinstance(dtype, Date):
            return pl.Date
        if isinstance(dtype, Time):
            return pl.Time
        if isinstance(dtype, Timestamp):
            return pl.Datetime(time_zone=dtype.timezone)
        if isinstance(dtype, Decimal):
            return pl.Decimal(dtype.precision, dtype.scale)

    return _annotation_to_polars_type(
        unwrap_annotated_optional(column.annotation),
        column.field_info.metadata,
        alias_type,
    )


def _annotation_to_polars_type(
    annotation: Any, metadata: list[Any], alias_type: str = "serialization"
) -> PolarsType:
    field_info = FieldInfo(annotation=annotation)
    field_info.metadata = metadata
    return get_polars_type(field_info, alias_type)


def get_polars_type(
    field_info: FieldInfo, alias_type: str = "serialization"
) -> PolarsType:
    # Handle case where annotation is None
    if field_info.annotation is None:
        polars_type: PolarsType = pl.String()
    else:
        # First try to handle BaseModel types
        basemodel_type = _handle_basemodel_type(field_info.annotation)
        if basemodel_type is not None:
            # For BaseModel types, we need to respect the alias_type
            if inspect.isclass(field_info.annotation) and issubclass(
                field_info.annotation, BaseModel
            ):
                polars_type = _convert_basemodel_to_struct(
                    field_info.annotation, alias_type
                )
            else:
                polars_type = basemodel_type
        else:
            # Then try to handle collection types that need special processing
            collection_type = _handle_collection_type(field_info.annotation, alias_type)
            if collection_type is not None:
                polars_type = collection_type
            else:
                # Handle special cases that need fully-specified types
                if field_info.annotation is dt.timedelta:
                    polars_type = pl.Duration("us")
                else:
                    # Fall back to default Polars type conversion
                    raw_type = pl.DataType.from_python(field_info.annotation)
                    # Handle types that need full specification
                    if raw_type == pl.Categorical:
                        polars_type = pl.Categorical()
                    elif raw_type == pl.Decimal:
                        # Use a reasonable default precision and scale
                        polars_type = pl.Decimal(10, 2)
                    else:
                        polars_type = raw_type

    # Allow explicit Polars type override via metadata
    for arg in field_info.metadata:
        if inspect.isclass(arg) and issubclass(arg, pl.DataType):
            # Handle types that need full specification
            if arg == pl.Categorical:
                polars_type = pl.Categorical()
            elif arg == pl.Decimal:
                # Use a reasonable default precision and scale
                polars_type = pl.Decimal(10, 2)
            else:
                polars_type = arg
            break
        elif isinstance(arg, pl.DataType):
            # Already a fully specified instance
            polars_type = arg
            break

    return polars_type
