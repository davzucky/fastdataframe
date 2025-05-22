"""PolarsFastDataframeModel implementation."""

from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.validation import ValidationError
from fastdataframe.core.types import get_type_name, json_schema_is_subset
import polars as pl
from typing import Type, TypeVar
from pydantic import TypeAdapter

T = TypeVar("T", bound="PolarsFastDataframeModel")


def _extract_polars_frame_json_schema(lazy_frame: pl.LazyFrame) -> dict[str, dict]:
    """
    Given a Polars LazyFrame, return a dict mapping column names to JSON schema dicts using TypeAdapter.
    """
    python_types = lazy_frame.collect_schema().to_python()  # {col: python_type}
    return {
        col: TypeAdapter(python_type).json_schema()
        for col, python_type in python_types.items()
    }


class PolarsFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Polars integration."""

    @classmethod
    def from_fastdataframe_model(
        cls: Type[T], model: type[FastDataframeModel]
    ) -> Type[T]:
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
            },
        )

    @classmethod
    def _validate_missing_columns(
        cls, frame_schema: dict
    ) -> dict[str, ValidationError]:
        """Validate if all required columns are present in the frame, using FastDataframe.is_nullable."""
        errors = {}
        fastdataframe_annotations = cls.get_fastdataframe_annotations()
        for field_name, annotation in fastdataframe_annotations.items():
            if not annotation.is_nullable and field_name not in frame_schema:
                errors[field_name] = ValidationError(
                    column_name=field_name,
                    error_type="MissingColumn",
                    error_details=f"Column {field_name} is missing in the frame.",
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
    def _validate_column_types(
        cls, model_schema: dict, frame_schema: dict
    ) -> dict[str, ValidationError]:
        """Validate if column types match the expected types, using FastDataframe.is_nullable."""
        errors = {}

        for field_name, field_schema in model_schema.items():
            frame_schema_field = frame_schema.get(field_name)
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
    def validate_schema(cls, lazy_frame: pl.LazyFrame) -> list[ValidationError]:
        """Validate the schema of a polars lazy frame against the model's schema.

        Args:
            lazy_frame: The polars lazy frame to validate.

        Returns:
            List[ValidationError]: A list of validation errors.
        """
        model_schema = cls.model_json_schema()["properties"]
        frame_schema = _extract_polars_frame_json_schema(lazy_frame)

        # Collect all validation errors
        errors = {}
        errors.update(cls._validate_missing_columns(frame_schema))
        errors.update(cls._validate_column_types(model_schema, frame_schema))

        return list(errors.values())
