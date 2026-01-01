"""PolarsFastDataframeModel implementation."""

from fastdataframe.core.model import AliasType, FastDataframeModel
from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)
from fastdataframe.core.validation import ValidationError, ValidationResult
import polars as pl
from typing import TypeVar, Union, Any
from pydantic import TypeAdapter
from fastdataframe.core.json_schema import (
    validate_missing_columns,
    validate_column_types,
)
from fastdataframe.polars._cast_functions import custom_cast_functions, simple_cast
from fastdataframe.polars._types import get_polars_type

TFrame = TypeVar("TFrame", bound=pl.DataFrame | pl.LazyFrame)


def _resolve_json_schema_refs(schema: Any, defs: dict | None = None) -> Any:
    """Resolve $ref references in a JSON schema by inlining the referenced schemas.

    Args:
        schema: The JSON schema that may contain $ref
        defs: The $defs section containing referenced schemas

    Returns:
        dict: Schema with $ref references resolved inline
    """
    if defs is None:
        defs = {}

    if isinstance(schema, dict):
        if "$ref" in schema:
            # Extract the reference name (e.g., "#/$defs/Address" -> "Address")
            ref = schema["$ref"]
            if ref.startswith("#/$defs/"):
                ref_name = ref[8:]  # Remove "#/$defs/" prefix
                if ref_name in defs:
                    # Recursively resolve the referenced schema
                    return _resolve_json_schema_refs(defs[ref_name], defs)
            # If we can't resolve the ref, return the original schema
            return schema
        else:
            # Recursively resolve refs in nested objects
            resolved = {}
            for key, value in schema.items():
                resolved[key] = _resolve_json_schema_refs(value, defs)
            return resolved
    elif isinstance(schema, list):
        # Recursively resolve refs in lists
        return [_resolve_json_schema_refs(item, defs) for item in schema]
    else:
        # Return primitive values as-is
        return schema


def _polars_dtype_to_json_schema(polars_dtype: Any) -> dict:
    """Convert a Polars DataType to a JSON schema dict."""
    if isinstance(polars_dtype, pl.List):
        inner_schema = _polars_dtype_to_json_schema(polars_dtype.inner)
        return {"type": "array", "items": inner_schema}
    elif isinstance(polars_dtype, pl.Array):
        inner_schema = _polars_dtype_to_json_schema(polars_dtype.inner)
        return {"type": "array", "items": inner_schema}
    elif isinstance(polars_dtype, pl.Struct):
        # Handle Struct types by converting each field
        properties = {}
        required = []

        for field in polars_dtype.fields:
            field_schema = _polars_dtype_to_json_schema(field.dtype)
            properties[field.name] = field_schema
            required.append(field.name)

        return {"type": "object", "properties": properties, "required": required}
    else:
        # For non-collection types, convert to Python type and use TypeAdapter
        python_type = polars_dtype.to_python()
        return TypeAdapter(python_type).json_schema()


def _extract_polars_frame_json_schema(frame: pl.LazyFrame | pl.DataFrame) -> dict:
    """
    Given a Polars LazyFrame or DataFrame, return a JSON schema compatible dict for the frame.
    The returned dict will have 'type': 'object', 'properties', and 'required' as per JSON schema standards.
    """
    schema = frame.collect_schema()
    properties = {
        col: _polars_dtype_to_json_schema(polars_dtype)
        for col, polars_dtype in schema.items()
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

        # Resolve $ref references in the model schema to match DataFrame schema format
        defs = model_json_schema.get("$defs", {})
        if defs:
            # Resolve references in properties
            resolved_properties = {}
            for prop_name, prop_schema in model_json_schema.get(
                "properties", {}
            ).items():
                resolved_properties[prop_name] = _resolve_json_schema_refs(
                    prop_schema, defs
                )

            # Create a new model schema with resolved references
            resolved_model_schema = model_json_schema.copy()
            resolved_model_schema["properties"] = resolved_properties
            model_json_schema = resolved_model_schema

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
                alias_func(field_info, field_name): get_polars_type(
                    field_info, alias_type
                )
                for field_name, field_info in cls.model_fields.items()
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
                alias_func(field_info, field_name): pl.String
                for field_name, field_info in cls.model_fields.items()
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
        column_infos = cls.model_columns(alias_type)
        cast_functions = []

        for target_col, target_type in target_schema.items():
            if target_col not in source_schema:
                raise ValueError(f"Column {target_col} not found in source schema")
            if source_schema[target_col] == target_type:
                continue
            cast_function = custom_cast_functions.get(
                (type(source_schema[target_col]), type(target_type)), simple_cast
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

    @classmethod
    def validate_data(cls, df: pl.DataFrame) -> ValidationResult:
        """Validate data content against the model's schema requirements.

        This method performs data validation beyond schema checking, focusing on
        data quality constraints defined in the model. It validates non-nullable
        constraints for required fields and returns detailed error information
        along with a clean dataset.

        Args:
            df: Polars DataFrame to validate against the model's requirements

        Returns:
            ValidationResult: Contains validation errors, clean data, and statistics

        Example:
            ```python
            class UserModel(PolarsFastDataframeModel):
                id: int  # Required field
                name: str  # Required field
                email: Optional[str] = None  # Optional field

            df = pl.DataFrame({
                "id": [1, None, 3],  # Row 1 has null in required field
                "name": ["Alice", "Bob", None],  # Row 2 has null in required field
                "email": [None, "bob@example.com", "charlie@example.com"]
            })

            result = UserModel.validate(df)
            print(f"Found {len(result.errors)} errors")
            print(f"Clean data has {result.valid_rows} out of {result.total_rows} rows")
            ```

        Notes:
            - Validates that required (non-optional) fields don't contain null values
            - Returns row-level error details for debugging and data quality reporting
            - Provides clean dataset with problematic rows removed for downstream processing
            - Future versions will support additional validation rules (uniqueness, ranges, etc.)
        """
        total_rows = len(df)
        errors = []
        error_row_indices: set[int] = set()

        # Get model fields and identify required (non-optional) fields
        from pydantic_core import PydanticUndefined

        required_fields = {}
        for field_name, field_info in cls.model_fields.items():
            # Check if field is optional by looking at the annotation and default value
            has_default = field_info.default is not PydanticUndefined
            has_default_factory = field_info.default_factory is not None
            is_union_with_none = getattr(
                field_info.annotation, "__origin__", None
            ) is Union and type(None) in getattr(field_info.annotation, "__args__", ())

            is_optional = has_default or has_default_factory or is_union_with_none
            if not is_optional:
                required_fields[field_name] = field_info

        # Check for null values in required fields
        for field_name, field_info in required_fields.items():
            if field_name in df.columns:
                # Find rows with null values in this required field
                null_mask = df.select(pl.col(field_name).is_null()).to_series()
                null_row_indices = [i for i, is_null in enumerate(null_mask) if is_null]

                if null_row_indices:
                    error = ValidationError(
                        column_name=field_name,
                        error_type="null_in_required_field",
                        error_details=f"Required field '{field_name}' contains null values",
                        error_rows=null_row_indices,
                    )
                    errors.append(error)
                    error_row_indices.update(null_row_indices)
            else:
                # Missing required column - all rows are invalid
                all_row_indices = list(range(total_rows))
                if all_row_indices:  # Only add error if there are rows
                    error = ValidationError(
                        column_name=field_name,
                        error_type="missing_required_column",
                        error_details=f"Required column '{field_name}' is missing from DataFrame",
                        error_rows=all_row_indices,
                    )
                    errors.append(error)
                    error_row_indices.update(all_row_indices)

        # Create clean data by filtering out error rows
        error_row_indices_list = sorted(list(error_row_indices))
        if error_row_indices_list:
            # Create a mask for valid rows (rows not in error_row_indices)
            all_indices = list(range(total_rows))
            valid_indices = [i for i in all_indices if i not in error_row_indices_list]
            if valid_indices:
                clean_data = df.slice(
                    0, 0
                )  # Start with empty DataFrame with same schema
                for idx in valid_indices:
                    clean_data = pl.concat([clean_data, df.slice(idx, 1)])
            else:
                # All rows have errors, return empty DataFrame with same schema
                clean_data = df.slice(0, 0)
        else:
            # No errors, return original DataFrame
            clean_data = df

        valid_rows = len(clean_data)

        return ValidationResult(
            errors=errors,
            clean_data=clean_data,
            error_row_indices=error_row_indices_list,
            total_rows=total_rows,
            valid_rows=valid_rows,
        )
