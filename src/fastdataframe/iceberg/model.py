"""Iceberg integration for FastDataFrame."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any, List, Union, cast, get_args, get_origin

from pydantic import BaseModel
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimeType,
    TimestampType,
    UUIDType,
)

from fastdataframe.core.annotation import ColumnInfo
from fastdataframe.core.column import ColumnDefinition, get_column_info
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
from fastdataframe.core.json_schema import validate_missing_columns
from fastdataframe.core.model import AliasType, FastDataframeModel
from fastdataframe.core.types_helper import is_optional_type, unwrap_annotated_optional
from fastdataframe.core.validation import ValidationError

from .json_schema import iceberg_schema_to_json_schema


def _column_name(
    column: ColumnDefinition, alias_type: AliasType = "serialization"
) -> str:
    if alias_type == "validation":
        return column.validation_name
    return column.storage_name


def _dtype_to_iceberg_type(column_info: ColumnInfo) -> IcebergType | None:
    dtype = column_info.dtype
    if dtype is None:
        return None
    if isinstance(dtype, Boolean):
        return BooleanType()
    if isinstance(dtype, String):
        return StringType()
    if isinstance(dtype, Binary):
        return BinaryType()
    if isinstance(dtype, (Int8, Int16, Int32)):
        return IntegerType()
    if isinstance(dtype, Int64):
        return LongType()
    if isinstance(dtype, Float32):
        return FloatType()
    if isinstance(dtype, Float64):
        return DoubleType()
    if isinstance(dtype, Date):
        return DateType()
    if isinstance(dtype, Time):
        return TimeType()
    if isinstance(dtype, Timestamp):
        return TimestampType()
    if isinstance(dtype, Decimal):
        return DecimalType(dtype.precision, dtype.scale)
    return None


def _model_fields_to_iceberg_fields(
    model_fields: Any,
    alias_func: Any = None,
    start_field_id: int = 1,
) -> List[NestedField]:
    """Convert Pydantic model fields to Iceberg NestedField list."""
    fields = []
    for idx, (field_name, field_info) in enumerate(
        model_fields.items(), start_field_id
    ):
        py_type = field_info.annotation
        column_info = get_column_info(field_info)
        nullable = is_optional_type(py_type)
        iceberg_type = _python_type_to_iceberg_type(
            py_type, field_id=idx, column_info=column_info
        )
        field_display_name = field_name
        if alias_func:
            field_display_name = alias_func(field_info, field_name)
        fields.append(
            NestedField(
                field_id=column_info.iceberg_id or idx,
                name=field_display_name,
                field_type=iceberg_type,
                required=not nullable,
            )
        )
    return fields


def _python_type_to_iceberg_type(
    py_type: Any, field_id: int, column_info: ColumnInfo
) -> IcebergType:
    dtype_type = _dtype_to_iceberg_type(column_info)
    if dtype_type is not None:
        return dtype_type

    py_type = unwrap_annotated_optional(py_type)
    origin = get_origin(py_type)

    if py_type is int:
        return IntegerType()
    if py_type is bool:
        return BooleanType()
    if py_type is float:
        return DoubleType()
    if py_type is str:
        return StringType()
    if py_type is dt.date:
        return DateType()
    if py_type is dt.time:
        return TimeType()
    if py_type is dt.datetime:
        return TimestampType()
    if py_type is uuid.UUID:
        return UUIDType()
    if py_type is bytes:
        return BinaryType()

    if origin in (list, set, tuple):
        import types

        args = get_args(py_type)
        if args:
            element_annotation = args[0]
            element_origin = get_origin(element_annotation)
            if element_origin in (Union, getattr(types, "UnionType", None)):
                union_args = [
                    a for a in get_args(element_annotation) if a is not type(None)
                ]
                if len(union_args) > 1:
                    raise ValueError(
                        "Sequence element types cannot be a union of multiple types; use a single type or Optional[T]."
                    )
            element_required = not is_optional_type(element_annotation)
            element_type = _python_type_to_iceberg_type(
                element_annotation, field_id=field_id, column_info=ColumnInfo()
            )
            return ListType(
                element_id=field_id,
                element_type=element_type,
                element_required=element_required,
            )
        return ListType()

    if origin is dict:
        import types

        args = get_args(py_type)
        if args and len(args) == 2:
            key_annotation, value_annotation = args
            for annotation, label in (
                (key_annotation, "Map key"),
                (value_annotation, "Map value"),
            ):
                annotation_origin = get_origin(annotation)
                if annotation_origin in (Union, getattr(types, "UnionType", None)):
                    union_args = [
                        a for a in get_args(annotation) if a is not type(None)
                    ]
                    if len(union_args) > 1:
                        raise ValueError(
                            f"{label} types cannot be a union of multiple types; use a single type or Optional[T]."
                        )

            value_required = not is_optional_type(value_annotation)
            key_type = _python_type_to_iceberg_type(
                key_annotation, field_id=field_id, column_info=ColumnInfo()
            )
            value_type = _python_type_to_iceberg_type(
                value_annotation, field_id=field_id, column_info=ColumnInfo()
            )
            return MapType(
                key_id=field_id,
                key_type=key_type,
                value_id=field_id,
                value_type=value_type,
                value_required=value_required,
            )
        return MapType()

    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        nested_fields = _model_fields_to_iceberg_fields(py_type.model_fields)
        return StructType(*nested_fields)

    return StringType()


def _column_to_nested_field(
    column: ColumnDefinition, idx: int, alias_type: AliasType
) -> NestedField:
    field_id = column.info.iceberg_id or idx
    return NestedField(
        field_id=field_id,
        name=_column_name(column, alias_type),
        field_type=_python_type_to_iceberg_type(
            column.annotation,
            field_id=field_id,
            column_info=column.info,
        ),
        required=not column.nullable,
    )


def schema(
    model: type[FastDataframeModel], alias_type: AliasType = "serialization"
) -> Schema:
    """Return a pyiceberg Schema based on a FastDataFrame model."""
    fields = [
        _column_to_nested_field(column, idx, alias_type)
        for idx, column in enumerate(model.column_definitions, 1)
    ]
    return Schema(*fields)


def validate_schema(
    model: type[FastDataframeModel], table: Table, *, canonical: bool = True
) -> List[ValidationError]:
    """Validate that an Iceberg table contains the model's columns."""
    table_json_schema = iceberg_schema_to_json_schema(table.schema())
    model_json_schema = model.model_json_schema().copy()
    if canonical:
        storage_names = [column.storage_name for column in model.column_definitions]
        model_json_schema["required"] = storage_names
        if "properties" in model_json_schema:
            model_json_schema["properties"] = {
                column.storage_name: model_json_schema["properties"].get(
                    column.python_name,
                    model_json_schema["properties"].get(column.storage_name, {}),
                )
                for column in model.column_definitions
            }
    errors = validate_missing_columns(model_json_schema, table_json_schema)
    return list(errors.values())


def append_polars(
    model: type[FastDataframeModel],
    table: Table,
    frame: Any,
    alias_type: AliasType = "serialization",
) -> None:
    """Append a Polars frame to Iceberg through the FastDataFrame PyArrow schema.

    Polars schemas do not encode nullability like PyArrow/Iceberg, so the Arrow
    schema generated from the FastDataFrame model is the persistence boundary.
    """
    from fastdataframe.pyarrow.model import schema as pyarrow_schema
    from fastdataframe.polars.model import cast as polars_cast

    normalized = polars_cast(model, frame, alias_type)
    collect = getattr(normalized, "collect", None)
    if callable(collect):
        normalized = collect()
    normalized = cast(Any, normalized)
    arrow_table = normalized.to_arrow().cast(pyarrow_schema(model, alias_type))
    table.append(arrow_table)


def apply_additive_migration(
    model: type[FastDataframeModel],
    table: Table,
    alias_type: AliasType = "serialization",
) -> Table:
    """Apply additive-only schema evolution to an Iceberg table."""
    new_schema = schema(model, alias_type)
    with table.transaction() as txn, txn.update_schema() as update:
        update.union_by_name(new_schema)
    return table


class IcebergFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Iceberg integration."""

    @classmethod
    def iceberg_schema(cls, alias_type: AliasType = "serialization") -> Schema:
        """Return a pyiceberg Schema based on the model's fields."""
        return schema(cls, alias_type)

    @classmethod
    def validate_schema(cls, table: Table) -> List[ValidationError]:
        """Validate that the Iceberg table's columns match the model's fields."""
        return validate_schema(cls, table, canonical=False)
