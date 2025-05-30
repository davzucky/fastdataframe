"""PolarsFastDataframeModel implementation."""

from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.validation import ValidationError
from fastdataframe.core.types import get_type_name, json_schema_is_subset
import polars as pl
from typing import Any, Type, TypeVar
from pydantic import BaseModel, TypeAdapter, create_model

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
    def from_fastdataframe_model(cls: Type[T], model: type[Any]) -> type[T]:
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
        )
        return new_model

    @classmethod
    def _validate_missing_columns(
        cls, model_json_schema: dict, df_json_schema: dict
    ) -> dict[str, ValidationError]:
        """Validate if all required columns are present in the frame, using the 'required' key from the model's JSON schema."""
        errors = {}
        # currently pydantic add all the fields
        # https://github.com/pydantic/pydantic/issues/7161
        model_required_fields = set(model_json_schema.get("required", []))
        df_required_fields = set(df_json_schema.get("required", []))

        for field_name in model_required_fields.difference(df_required_fields):
            errors[field_name] = ValidationError(
                column_name=field_name,
                error_type="MissingColumn",
                error_details=f"Column {field_name} is missing in the frame.",
            )
        return errors

    @classmethod
    def _validate_column_types(
        cls, model_json_schema: dict[str, Any], df_json_schema: dict[str, Any]
    ) -> dict[str, ValidationError]:
        """Validate if column types match the expected types, using FastDataframe.is_nullable."""
        errors = {}
        model_properties = model_json_schema.get("properties", {})
        df_properties = df_json_schema.get("properties", {})
        for field_name, field_schema in model_properties.items():
            frame_schema_field = df_properties.get(field_name)
            if frame_schema_field and not json_schema_is_subset(
                field_schema, frame_schema_field
            ):
                errors[field_name] = ValidationError(
                    column_name=field_name,
                    error_type="TypeMismatch",
                    error_details=f"Expected type {get_type_name(field_schema)}, but got {get_type_name(frame_schema_field)}.",
                )
        return errors

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
        errors.update(cls._validate_missing_columns(model_json_schema, df_json_schema))
        errors.update(cls._validate_column_types(model_json_schema, df_json_schema))

        return list(errors.values())
