"""PyArrow type conversion utilities."""

import datetime as dt
import inspect
import uuid
from typing import Any, Union, get_args, get_origin

import pyarrow as pa
from pydantic import BaseModel
from pydantic.fields import FieldInfo

from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)
from fastdataframe.core.types_helper import is_optional_type

type PyArrowType = pa.DataType


# Mapping of Python types to PyArrow types
_PYTHON_TO_PYARROW: dict[type, pa.DataType] = {
    int: pa.int64(),
    str: pa.string(),
    float: pa.float64(),
    bool: pa.bool_(),
    bytes: pa.binary(),
    dt.date: pa.date32(),
    dt.datetime: pa.timestamp("us"),
    dt.time: pa.time64("us"),
    dt.timedelta: pa.duration("us"),
    uuid.UUID: pa.string(),  # PyArrow has no native UUID type
}


def _handle_collection_type(
    annotation: Any, alias_type: str = "serialization"
) -> PyArrowType | None:
    """Handle collection types (list, tuple, set) that need special processing.

    Args:
        annotation: The type annotation to process
        alias_type: Whether to use "serialization" or "validation" aliases

    Returns:
        PyArrowType | None: The PyArrow type for the collection, or None if not a collection
    """
    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is None or not args:
        return None

    # Handle set[T] -> pa.list_(T) (sets are stored as lists in PyArrow)
    if origin is set:
        inner_type = _python_type_to_pyarrow(args[0], alias_type)
        return pa.list_(inner_type)

    # Handle collections of BaseModel types (list[BaseModel], etc.)
    if origin in (list, tuple, set) and args:
        inner_type = args[0]
        if inspect.isclass(inner_type) and issubclass(inner_type, BaseModel):
            # Convert BaseModel to Struct first
            basemodel_struct = _convert_basemodel_to_struct(inner_type, alias_type)
            return pa.list_(basemodel_struct)
        else:
            # Handle regular types
            inner_pa_type = _python_type_to_pyarrow(inner_type, alias_type)
            return pa.list_(inner_pa_type)

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


def _python_type_to_pyarrow(
    py_type: Any, alias_type: str = "serialization"
) -> pa.DataType:
    """Convert a Python type to a PyArrow DataType.

    Args:
        py_type: The Python type to convert
        alias_type: Whether to use "serialization" or "validation" aliases

    Returns:
        pa.DataType: The corresponding PyArrow type
    """
    # Handle None type
    if py_type is None or py_type is type(None):
        return pa.null()

    # Unwrap Optional types to get the inner type
    if is_optional_type(py_type):
        args = get_args(py_type)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if non_none_args:
            py_type = non_none_args[0]

    # Check for collection types first
    collection_type = _handle_collection_type(py_type, alias_type)
    if collection_type is not None:
        return collection_type

    # Check for BaseModel types
    if inspect.isclass(py_type) and issubclass(py_type, BaseModel):
        return _convert_basemodel_to_struct(py_type, alias_type)

    # Check the direct mapping
    if py_type in _PYTHON_TO_PYARROW:
        return _PYTHON_TO_PYARROW[py_type]

    # Fallback to string for unknown types
    return pa.string()


def _convert_basemodel_to_struct(
    model_class: type[BaseModel], alias_type: str = "serialization"
) -> pa.StructType:
    """Convert a Pydantic BaseModel class to a PyArrow Struct type.

    Args:
        model_class: The BaseModel class to convert
        alias_type: Whether to use "serialization" or "validation" aliases

    Returns:
        pa.StructType: A PyArrow Struct type representing the BaseModel
    """
    alias_func = (
        get_serialization_alias
        if alias_type == "serialization"
        else get_validation_alias
    )

    fields = []
    for field_name, field_info in model_class.model_fields.items():
        field_alias = alias_func(field_info, field_name)

        # Determine nullability from the annotation
        nullable = is_optional_type(field_info.annotation)

        # Get the PyArrow type for this field
        field_type: pa.DataType
        if field_info.annotation is None:
            field_type = pa.string()
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
                # Use standard conversion
                field_type = _python_type_to_pyarrow(field_info.annotation, alias_type)

        fields.append(pa.field(field_alias, field_type, nullable=nullable))

    return pa.struct(fields)


def get_pyarrow_type(
    field_info: FieldInfo, alias_type: str = "serialization"
) -> PyArrowType:
    """Convert a Pydantic FieldInfo to a PyArrow DataType.

    Args:
        field_info: The Pydantic FieldInfo to convert
        alias_type: Whether to use "serialization" or "validation" aliases

    Returns:
        PyArrowType: The corresponding PyArrow type
    """
    # Handle case where annotation is None
    if field_info.annotation is None:
        return pa.string()

    # First try to handle BaseModel types
    if inspect.isclass(field_info.annotation) and issubclass(
        field_info.annotation, BaseModel
    ):
        return _convert_basemodel_to_struct(field_info.annotation, alias_type)

    # Then try to handle collection types that need special processing
    collection_type = _handle_collection_type(field_info.annotation, alias_type)
    if collection_type is not None:
        return collection_type

    # Handle Optional types - unwrap to get the inner type
    annotation = field_info.annotation
    if is_optional_type(annotation):
        args = get_args(annotation)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if non_none_args:
            annotation = non_none_args[0]

    # Check for explicit PyArrow type override via metadata
    for arg in field_info.metadata:
        if isinstance(arg, pa.DataType):
            return arg

    # Use standard conversion
    return _python_type_to_pyarrow(annotation, alias_type)
