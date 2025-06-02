from pydantic import BaseModel, create_model
from fastdataframe.core.json_schema import validate_missing_columns
from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.validation import ValidationError
from typing import Any, List, Type, TypeVar, get_args, get_origin, Annotated
from pyiceberg.table import Table
from pyiceberg.types import (
    NestedField,
    IntegerType,
    BooleanType,
    DoubleType,
    IcebergType,
    StringType,
    DateType,
    TimeType,
    TimestampType,
    UUIDType,
    BinaryType,
)
from pyiceberg.schema import Schema
from fastdataframe.core.types_helper import is_optional_type
from .json_schema import iceberg_schema_to_json_schema

T = TypeVar("T", bound="IcebergFastDataframeModel")


# Helper function to map Python/Pydantic types to pyiceberg types
def _python_type_to_iceberg_type(py_type: Any) -> IcebergType:
    origin = get_origin(py_type)
    if origin is Annotated:
        py_type = get_args(py_type)[0]
    # Unwrap Optional/Union[..., NoneType]
    if is_optional_type(py_type):
        args = get_args(py_type)
        # Remove NoneType from Union
        py_type = next((a for a in args if a is not type(None)), None)
    if py_type is int:
        return IntegerType()
    elif py_type is bool:
        return BooleanType()
    elif py_type is float:
        return DoubleType()
    elif py_type is str:
        return StringType()
    import datetime
    import uuid

    if py_type is datetime.date:
        return DateType()
    if py_type is datetime.time:
        return TimeType()
    if py_type is datetime.datetime:
        return TimestampType()
    if py_type is uuid.UUID:
        return UUIDType()
    if py_type is bytes:
        return BinaryType()
    return StringType()  # fallback


class IcebergFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Iceberg integration."""

    @classmethod
    def from_base_model(cls: Type[T], model: type[Any]) -> type[T]:
        """Convert any BaseModel to a IcebergFastDataframeModel using create_model."""

        is_base_model = issubclass(model, BaseModel)
        field_definitions = {
            field_name: (
                field_type,
                model.model_fields[field_name]
                if is_base_model
                else getattr(model, field_name, ...),
            )
            for field_name, field_type in model.__annotations__.items()
        }

        new_model: type[T] = create_model(
            f"{model.__name__}Iceberg",
            __base__=cls,
            __doc__=f"Iceberg version of {model.__name__}",
            **field_definitions,
        )  # type: ignore[call-overload]
        return new_model

    @classmethod
    def iceberg_schema(cls) -> Schema:
        """Return a pyiceberg Schema based on the model's fields, supporting Optional types."""
        fields = []
        for idx, (field_name, model_field) in enumerate(cls.model_fields.items(), 1):
            py_type = model_field.annotation
            nullable = is_optional_type(py_type)
            iceberg_type = _python_type_to_iceberg_type(py_type)
            fields.append(
                NestedField(
                    field_id=idx,
                    name=field_name,
                    field_type=iceberg_type,
                    required=not nullable,
                )
            )
        return Schema(*fields)

    @classmethod
    def validate_table(cls, table: Table) -> List[ValidationError]:
        """Validate that the Iceberg table's columns match the model's fields by name only.

        Args:
            table: The Iceberg table to validate.

        Returns:
            List[ValidationError]: A list of validation errors (missing columns).
        """

        table_json_schema = iceberg_schema_to_json_schema(table.schema())
        model_json_schema = cls.model_json_schema()

        errors = {}
        errors.update(validate_missing_columns(model_json_schema, table_json_schema))

        return list(errors.values())
