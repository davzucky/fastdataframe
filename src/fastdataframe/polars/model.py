"""PolarsFastDataframeModel implementation."""

from fastdataframe.core.model import AliasType, FastDataframeModel
from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)
from fastdataframe.core.validation import ValidationError
import polars as pl
from typing import Any, Type, TypeVar, Union
from pydantic import BaseModel, TypeAdapter, create_model
from fastdataframe.core.json_schema import (
    validate_missing_columns,
    validate_column_types,
)
from fastdataframe.polars._types import get_polars_type

T = TypeVar("T", bound="PolarsFastDataframeModel")
TFrame = TypeVar("TFrame", bound=pl.DataFrame | pl.LazyFrame)


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
        return super().from_base_model(model, "Polars")

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

        return list(errors.values())

    @classmethod
    def get_polars_schema(cls, alias_type: AliasType = "serialization") -> pl.Schema:
        """Get the polars schema for the model."""
        alias_func = (
            get_serialization_alias
            if alias_type == "serialization"
            else get_validation_alias
        )
        return pl.Schema(
            {
                alias_func(
                    cls.__pydantic_fields__[field_name], field_name
                ): get_polars_type(field_type)
                for field_name, field_type in cls.__annotations__.items()
            }
        )

    @classmethod
    def get_stringified_schema(
        cls, alias_type: AliasType = "serialization"
    ) -> pl.Schema:
        """Get the polars schema for the model with all columns as strings."""
        alias_func = (
            get_serialization_alias
            if alias_type == "serialization"
            else get_validation_alias
        )
        return pl.Schema(
            {
                alias_func(cls.__pydantic_fields__[field_name], field_name): pl.String
                for field_name in cls.__annotations__.keys()
            }
        )

    @classmethod
    def rename(
        cls,
        df: pl.DataFrame | pl.LazyFrame,
        alias_type_from: AliasType = "serialization",
        alias_type_to: AliasType = "serialization",
    ) -> pl.DataFrame | pl.LazyFrame:
        """Rename dataframe columns between different alias types according to the model's schema.

        This method allows converting column names between validation aliases (used during data validation)
        and serialization aliases (used for storage/export). It maintains the model's schema constraints
        while adapting to different naming conventions.

        Args:
            df: Polars DataFrame or LazyFrame to rename columns on
            alias_type_from: The alias type currently used in the input dataframe columns.
                - 'serialization' for storage/export names
                - 'validation' for validation/processing names
            alias_type_to: The target alias type to convert column names to.
                Uses same options as alias_type_from.

        Returns:
            pl.DataFrame | pl.LazyFrame: New dataframe with renamed columns. Maintains original type
            (eager DataFrame or LazyFrame) of input.

        Raises:
            KeyError: If any existing column name is not found in the model's schema

        Example:
            ```python
            # Convert from database column names to validation names
            df = MyModel.rename(df, alias_type_from='serialization', alias_type_to='validation')

            # Convert back to serialization names for storage
            df = MyModel.rename(df, alias_type_from='validation', alias_type_to='serialization')
            ```
        """
        alias_func_from = (
            get_serialization_alias
            if alias_type_from == "serialization"
            else get_validation_alias
        )
        alias_func_to = (
            get_serialization_alias
            if alias_type_to == "serialization"
            else get_validation_alias
        )
        model_map = {
            alias_func_from(field_info, field_name): alias_func_to(
                field_info, field_name
            )
            for field_name, field_info in cls.__pydantic_fields__.items()
        }
        df_schema = df.collect_schema()
        rename_map = {
            field_name: model_map[field_name]
            for field_name in df_schema.keys()
            if field_name in model_map
        }
        return df.rename(rename_map)

    @classmethod
    def cast_to_model_schema(
        cls,
        df: Union[pl.DataFrame, pl.LazyFrame],
        alias_type: AliasType = "serialization",
    ) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Cast DataFrame or LazyFrame columns to match the model's schema types."""
        schema = cls.get_polars_schema(alias_type)
        return df.cast({k: v for k, v in schema.items()})
