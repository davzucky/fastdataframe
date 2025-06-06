"""PolarsFastDataframeModel implementation."""

from pydantic_to_pyarrow import get_pyarrow_schema
from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.validation import ValidationError
import polars as pl
from typing import Any, List, Type, TypeVar
from pydantic import BaseModel, TypeAdapter, create_model
from fastdataframe.core.json_schema import (
    validate_missing_columns,
    validate_column_types,
)

T = TypeVar("T", bound="PolarsFastDataframeModel")


def _extract_polars_frame_json_schema(frame: pl.LazyFrame | pl.DataFrame) -> dict:
    """
    Given a Polars LazyFrame or DataFrame, return a JSON schema compatible dict for the frame.
    The returned dict will have 'type': 'object', 'properties', and 'required' as per JSON schema standards.
    """
    python_types = frame.collect_schema().to_python()  # {col: python_type}
    properties = {
        col: TypeAdapter(python_type).json_schema()
        for col, python_type in python_types.items()
    }
    required = list(properties.keys())
    return {
        "type": "object",
        "properties": properties,
        "required": required,
    }


class PolarsFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Polars integration."""

    @classmethod
    def from_base_model(cls: Type[T], model: type[Any]) -> type[T]:
        """Convert any FastDataframeModel to a PolarsFastDataframeModel using create_model."""

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
            f"{model.__name__}Polars",
            __base__=cls,
            __doc__=f"Polars version of {model.__name__}",
            **field_definitions,
        )  # type: ignore[call-overload]
        return new_model

    @classmethod
    def polars_schema(cls) -> pl.Schema:
        """Return a polars dataframe Schema based on the model's fields, supporting Optional types."""
        pyarrow_schema = get_pyarrow_schema(cls)
        empty_df = pl.from_arrow(pyarrow_schema.empty_table())
        return empty_df.schema

    @classmethod
    def validate_schema(
        cls, frame: pl.LazyFrame | pl.DataFrame
    ) -> list[ValidationError]:
        """Validate the schema of a polars lazy frame against the model's schema.

        Args:
            frame: The polars lazy frame or dataframe to validate.

        Returns:
            List[ValidationError]: A list of validation errors.
        """
        model_json_schema = cls.model_json_schema()
        df_json_schema = _extract_polars_frame_json_schema(frame)

        # Collect all validation errors
        errors = {}
        errors.update(validate_missing_columns(model_json_schema, df_json_schema))
        errors.update(validate_column_types(model_json_schema, df_json_schema))

        # only concern the required fields
        required_fields = [
            field for field in model_json_schema["required"] 
            if field not in errors or errors[field].error_type != "MissingColumn"
        ]
        frame_with_required_fields = frame.select(required_fields)
        if isinstance(frame, pl.LazyFrame):
            frame_with_required_fields = frame_with_required_fields.collect()
        errors.update(cls.validate_non_null_columns(required_fields, frame_with_required_fields))

        return list(errors.values())
    
    @classmethod
    def validate_non_null_columns(
        cls, required_fields: List[str], frame: pl.DataFrame
    ) -> dict[str, ValidationError]:
        """
        Validate that required columns in the given Polars LazyFrame or DataFrame do not contain null values.
        Args:
            required_fields: List of column names that are required.
            frame: The Polars LazyFrame or DataFrame to validate.

        Returns:
            dict[str, ValidationError]: A dictionary where keys are column names and values are ValidationError
            instances indicating columns that contain null values.
        """
        errors = {}
        for field_name in required_fields:
            if field_name in frame.columns and frame[field_name].has_nulls():
                errors[field_name] = ValidationError(
                    column_name=field_name, 
                    error_type="RequiredColumn", 
                    error_details=f"Required column contains null in the frame.",
                )
        return errors
