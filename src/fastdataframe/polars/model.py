"""PolarsFastDataframeModel implementation."""

from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.validation import ValidationError
import polars as pl


class PolarsFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Polars integration."""

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
        errors = {}

        for field_name, field_schema in model_schema.get("properties", {}).items():
            if field_name not in frame_schema:
                error = errors.get(field_name, ValidationError(
                    column_name=field_name,
                    error_type="MissingColumn",
                    error_details=f"Column {field_name} is missing in the frame."
                ))
                errors[field_name] = error
                
            # Add additional validation logic here if needed
        return errors 