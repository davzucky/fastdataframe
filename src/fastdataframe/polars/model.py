"""PolarsFastDataframeModel implementation."""

from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.validation import ValidationError
import polars as pl


class PolarsFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Polars integration."""

    @classmethod
    def _validate_missing_columns(cls, model_schema: dict, frame_schema: dict) -> dict[str, ValidationError]:
        """Validate if all required columns are present in the frame.

        Args:
            model_schema: The model's JSON schema.
            frame_schema: The frame's schema.

        Returns:
            dict[str, ValidationError]: A dictionary of validation errors.
        """
        errors = {}
        for field_name in model_schema.get("properties", {}):
            if field_name not in frame_schema:
                errors[field_name] = ValidationError(
                    column_name=field_name,
                    error_type="MissingColumn",
                    error_details=f"Column {field_name} is missing in the frame."
                )
        return errors

    @staticmethod
    def _polars_dtype_to_json_schema(dtype) -> str:
        """Map Polars dtype to JSON schema type string."""
        dtype_str = str(dtype)
        if dtype_str.startswith("Int"):
            return "integer"
        if dtype_str.startswith("Float"):
            return "number"
        if dtype_str == "String":
            return "string"
        if dtype_str == "Boolean":
            return "boolean"
        return dtype_str  # fallback for unknown types

    @classmethod
    def _validate_column_types(cls, model_schema: dict, frame_schema: dict) -> dict[str, ValidationError]:
        """Validate if column types match the expected types.

        Args:
            model_schema: The model's JSON schema.
            frame_schema: The frame's schema.

        Returns:
            dict[str, ValidationError]: A dictionary of validation errors.
        """
        errors = {}
        for field_name, field_schema in model_schema.get("properties", {}).items():
            if field_name in frame_schema:
                expected_type = field_schema.get("type")
                actual_type = cls._polars_dtype_to_json_schema(frame_schema[field_name])
                if expected_type != actual_type:
                    errors[field_name] = ValidationError(
                        column_name=field_name,
                        error_type="TypeMismatch",
                        error_details=f"Expected type {expected_type}, but got {actual_type}."
                    )
        return errors

    @classmethod
    def validate_schema(cls, lazy_frame: pl.LazyFrame) -> list[ValidationError]:
        """Validate the schema of a polars lazy frame against the model's schema.

        Args:
            lazy_frame: The polars lazy frame to validate.

        Returns:
            List[ValidationError]: A list of validation errors.
        """
        model_schema = cls.model_json_schema()
        frame_schema = lazy_frame.collect_schema()
        
        # Collect all validation errors
        errors = {}
        errors.update(cls._validate_missing_columns(model_schema, frame_schema))
        errors.update(cls._validate_column_types(model_schema, frame_schema))

        return list(errors.values()) 