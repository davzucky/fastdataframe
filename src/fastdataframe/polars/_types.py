from pydantic.fields import FieldInfo
from pydantic import BaseModel
import polars as pl
import inspect
from typing import get_origin, get_args, Any, Union
from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)

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
                # Fall back to default Polars type conversion
                polars_type = pl.DataType.from_python(field_info.annotation)

    # Allow explicit Polars type override via metadata
    for arg in field_info.metadata:
        if inspect.isclass(arg) and issubclass(arg, pl.DataType):
            polars_type = arg
            break

    return polars_type
