from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.validation import ValidationError
from typing import Any, List, get_args, get_origin, Annotated
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
        model_fields = set(cls.model_fields.keys())
        table_columns = set(field.name for field in table.schema().fields)
        errors = []
        for field in model_fields:
            if field not in table_columns:
                errors.append(
                    ValidationError(
                        column_name=field,
                        error_type="MissingColumn",
                        error_details=f"Column {field} is missing in the Iceberg table.",
                    )
                )
        return errors
