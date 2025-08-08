from fastdataframe.core.annotation import ColumnInfo
from fastdataframe.core.json_schema import validate_missing_columns
from fastdataframe.core.model import AliasType, FastDataframeModel, _get_column_info
from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)
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
    ListType
)
from pyiceberg.schema import Schema
from fastdataframe.core.types_helper import is_optional_type
from .json_schema import iceberg_schema_to_json_schema


# Helper function to map Python/Pydantic types to pyiceberg types
def _python_type_to_iceberg_type(py_type: Any, field_id: int, column_info: ColumnInfo) -> IcebergType:
    # Unwrap Annotated to the underlying type
    origin = get_origin(py_type)
    if origin is Annotated:
        py_type = get_args(py_type)[0]
        origin = get_origin(py_type)

    # Unwrap Optional/Union[..., NoneType] for type mapping
    if is_optional_type(py_type):
        args = get_args(py_type)
        py_type = next((a for a in args if a is not type(None)), None)
        origin = get_origin(py_type)

    # Primitive mappings
    if py_type is int:
        return IntegerType()
    if py_type is bool:
        return BooleanType()
    if py_type is float:
        return DoubleType()
    if py_type is str:
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

    # List and sequence-like mappings
    if origin is list:
        args = get_args(py_type)
        if args:
            element_annotation = args[0]
            element_required = not is_optional_type(element_annotation)
            element_type = _python_type_to_iceberg_type(
                element_annotation, field_id=field_id, column_info=column_info
            )
            return ListType(
                element_id=field_id,
                element_type=element_type,
                element_required=element_required,
            )
        return ListType()

    if py_type is tuple:
        return ListType()

    # Fallback to string for unsupported/unknown types
    return StringType()


class IcebergFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Iceberg integration."""

    @classmethod
    def iceberg_schema(cls, alias_type: AliasType = "serialization") -> Schema:
        """Return a pyiceberg Schema based on the model's fields, supporting Optional types."""
        alias_func = (
            get_serialization_alias
            if alias_type == "serialization"
            else get_validation_alias
        )

        fields = []
        for idx, (field_name, field_info) in enumerate(cls.model_fields.items(), 1):
            py_type = field_info.annotation
            column_info = _get_column_info(field_info)
            nullable = is_optional_type(py_type)
            iceberg_type = _python_type_to_iceberg_type(py_type, field_id=idx, column_info=column_info)
            fields.append(
                NestedField(
                    field_id=idx,
                    name=alias_func(cls.__pydantic_fields__[field_name], field_name),
                    field_type=iceberg_type,
                    required=not nullable,
                )
            )
        return Schema(*fields)

    @classmethod
    def validate_schema(cls, table: Table) -> List[ValidationError]:
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
