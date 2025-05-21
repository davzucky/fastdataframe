"""PolarsFastDataframeModel implementation."""

from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.validation import ValidationError
import polars as pl
from typing import Type, TypeVar

T = TypeVar("T", bound="PolarsFastDataframeModel")

class PolarsFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Polars integration."""

    @classmethod
    def from_fastdataframe_model(cls: Type[T], model: type[FastDataframeModel]) -> Type[T]:
        """Convert any FastDataframeModel to a PolarsFastDataframeModel.
        
        This method creates a new class that inherits from PolarsFastDataframeModel
        with the same fields and annotations as the input model.
        
        Args:
            model: The FastDataframeModel to convert
            
        Returns:
            A new class that inherits from PolarsFastDataframeModel with the same fields
            
        Example:
            ```python
            class MyModel(FastDataframeModel):
                field1: int
                field2: str
                
            # Convert to PolarsFastDataframeModel
            PolarsModel = PolarsFastDataframeModel.from_fastdataframe_model(MyModel)
            ```
        """
        # Create a new class that inherits from PolarsFastDataframeModel
        # with the same fields and annotations as the input model
        return type(
            f"{model.__name__}Polars",
            (cls,),
            {
                "__annotations__": model.__annotations__,
                "__doc__": f"Polars version of {model.__name__}",
            }
        )

    @classmethod
    def _validate_missing_columns(cls, model_schema: dict, frame_schema: dict) -> dict[str, ValidationError]:
        """Validate if all required columns are present in the frame, using FastDataframe.is_nullable."""
        errors = {}
        fastdataframe_annotations = cls.get_fastdataframe_annotations()
        for field_name, annotation in fastdataframe_annotations.items():
            if not annotation.is_nullable and field_name not in frame_schema:
                errors[field_name] = ValidationError(
                    column_name=field_name,
                    error_type="MissingColumn",
                    error_details=f"Column {field_name} is missing in the frame."
                )
        return errors

    @staticmethod
    def _polars_dtype_to_json_schema(dtype: pl.DataType) -> str:
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
        if dtype_str == "Date":
            return "date"
        if dtype_str == "Datetime":
            return "datetime"
        if dtype_str == "Time":
            return "time"
        if dtype_str == "Duration":
            return "timedelta"
        return dtype_str  # fallback for unknown types

    @classmethod
    def _validate_column_types(cls, model_schema: dict, frame_schema: dict) -> dict[str, ValidationError]:
        """Validate if column types match the expected types, using FastDataframe.is_nullable."""
        errors = {}
        fastdataframe_annotations = cls.get_fastdataframe_annotations()
        for field_name, field_schema in model_schema.get("properties", {}).items():
            if field_name in frame_schema:
                if "anyOf" in field_schema:
                    expected_types = [subschema.get("type") for subschema in field_schema["anyOf"] if "type" in subschema]
                else:
                    expected_type = field_schema.get("type")
                    expected_types = [expected_type] if expected_type is not None else []
                actual_type = cls._polars_dtype_to_json_schema(frame_schema[field_name])
                is_nullable = fastdataframe_annotations.get(field_name, None)
                is_nullable_flag = is_nullable.is_nullable if is_nullable is not None else False
                if actual_type in expected_types:
                    continue
                errors[field_name] = ValidationError(
                    column_name=field_name,
                    error_type="TypeMismatch",
                    error_details=f"Expected type {' or '.join(expected_types)}, but got {actual_type}."
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