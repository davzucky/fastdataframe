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
from fastdataframe.polars._cast_functions import custom_cast_functions, simple_cast
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
    def cast(
        cls,
        df: Union[pl.DataFrame, pl.LazyFrame],
        alias_type: AliasType = "serialization",
    ) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Cast DataFrame or LazyFrame columns to match the model's schema types.

        This method performs type casting on Polars DataFrame or LazyFrame columns to ensure
        they match the expected types defined in the model's schema. It uses intelligent
        casting functions that handle both simple type conversions and complex transformations
        based on the model's field annotations and metadata.

        The method supports both eager DataFrames and lazy LazyFrames, maintaining the
        original type in the return value. It only casts columns that have different types
        between the source and target schemas, skipping columns that already match.

        Args:
            df: Polars DataFrame or LazyFrame to cast columns on. The method maintains
                the original type (eager DataFrame or LazyFrame) in the return value.
            alias_type: The alias type to use for column name resolution.
                - 'serialization' (default): Use serialization aliases for column names
                - 'validation': Use validation aliases for column names

        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: New dataframe with cast columns. Maintains
            the original type (eager DataFrame or LazyFrame) of the input.

        Raises:
            ValueError: If any column required by the model's schema is not found in
                the source dataframe. For lazy frames, the error is raised when the dataframe is collected.

        Example:
            ```python
            from fastdataframe import PolarsFastDataframeModel, ColumnInfo
            from typing import Annotated
            import polars as pl
            from pydantic import Field

            # Define a model with custom casting metadata
            class UserModel(PolarsFastDataframeModel):
                id: int
                name: str
                is_active: Annotated[bool, ColumnInfo(
                    bool_true_string="yes",
                    bool_false_string="no"
                )]
                birth_date: Annotated[datetime.date, ColumnInfo(
                    date_format="%Y-%m-%d"
                )]

            # Create a dataframe with string columns that need casting
            df = pl.DataFrame({
                "id": ["1", "2", "3"],
                "name": ["Alice", "Bob", "Charlie"],
                "is_active": ["yes", "no", "yes"],
                "birth_date": ["1990-01-15", "1985-03-20", "1992-07-10"]
            })

            # Cast the dataframe to match the model's schema
            cast_df = UserModel.cast(df)
            
            # The resulting dataframe will have:
            # - id: Int64 (cast from String)
            # - name: String (no change needed)
            # - is_active: Boolean (cast from String using custom true/false strings)
            # - birth_date: Date (cast from String using custom date format)
            ```

        Notes:
            - The method uses custom casting functions for specific type combinations
              (e.g., String to Boolean with custom true/false strings, String to Date
              with custom date formats)
            - For type combinations without custom functions, it falls back to Polars'
              built-in casting with strict=True
            - Columns that already match the target type are skipped for efficiency
            - The method preserves the original dataframe's structure and only modifies
              column types as needed
        """
        source_schema = df.collect_schema()
        target_schema = cls.get_polars_schema(alias_type)
        column_infos = cls.get_column_infos(alias_type)
        cast_functions = []

        for target_col, target_type in target_schema.items():
            if target_col not in source_schema:
                raise ValueError(f"Column {target_col} not found in source schema")
            if source_schema[target_col] == target_type:
                continue
            cast_function = custom_cast_functions.get(
                (type(source_schema[target_col]), type(target_type)),
                simple_cast
            )

            cast_functions.append(
                cast_function(
                    source_schema[target_col],
                    target_type,
                    target_col,
                    column_infos[target_col],
                )
            )

        df = df.with_columns(cast_functions)

        return df
