"""PyArrow integration for FastDataFrame."""

from __future__ import annotations

from typing import cast as typing_cast

import pyarrow as pa
from pydantic import BaseModel, create_model

from fastdataframe.core.model import AliasType, FastDataframeModel
from fastdataframe.pyarrow._types import get_pyarrow_type_from_column


def _column_name(column, alias_type: AliasType = "serialization") -> str:
    if alias_type == "validation":
        return column.validation_name
    return column.storage_name


def schema(
    model: type[FastDataframeModel], alias_type: AliasType = "serialization"
) -> pa.Schema:
    """Get the PyArrow schema for a FastDataFrame model."""
    fields = [
        pa.field(
            _column_name(column, alias_type),
            get_pyarrow_type_from_column(column, alias_type),
            nullable=column.nullable,
        )
        for column in model.column_definitions
    ]
    return pa.schema(fields)


def string_schema(
    model: type[FastDataframeModel], alias_type: AliasType = "serialization"
) -> pa.Schema:
    """Get the PyArrow schema for the model with all columns as strings."""
    fields = [
        pa.field(
            _column_name(column, alias_type), pa.string(), nullable=column.nullable
        )
        for column in model.column_definitions
    ]
    return pa.schema(fields)


class PyArrowFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for PyArrow integration."""

    @classmethod
    def from_base_model(cls, model: type[BaseModel]):
        """Create a PyArrow-compatible model from a Pydantic model."""
        field_definitions = {
            field_name: (field_type.annotation, field_type)
            for field_name, field_type in model.model_fields.items()
        }
        new_model = create_model(  # type: ignore[no-matching-overload]
            f"{model.__name__}PyArrow",
            __base__=cls,
            __doc__=f"PyArrow version of {model.__name__}",
            **field_definitions,
        )
        return typing_cast(type[PyArrowFastDataframeModel], new_model)

    @classmethod
    def get_pyarrow_schema(cls, alias_type: AliasType = "serialization") -> pa.Schema:
        """Get the PyArrow schema for the model."""
        return schema(cls, alias_type)

    @classmethod
    def get_stringified_schema(
        cls, alias_type: AliasType = "serialization"
    ) -> pa.Schema:
        """Get the PyArrow schema for the model with all columns as strings."""
        return string_schema(cls, alias_type)
