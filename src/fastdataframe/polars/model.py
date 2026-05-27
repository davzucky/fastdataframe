"""PolarsFastDataframeModel implementation."""

from fastdataframe.core.model import AliasType, FastDataframeModel
from fastdataframe.core.validation import ValidationError, ValidationResult
import polars as pl
from typing import TypeVar, Union, Any, cast as typing_cast
from pydantic import BaseModel, TypeAdapter, create_model
from fastdataframe.core.json_schema import (
    validate_missing_columns,
    validate_column_types,
)
from fastdataframe.polars._cast_functions import custom_cast_functions, simple_cast
from fastdataframe.polars._types import get_polars_type_from_column

TFrame = TypeVar("TFrame", bound=pl.DataFrame | pl.LazyFrame)

# Type alias for JSON schema values (can be dict, list, or primitives)
JsonSchemaValue = dict[str, Any] | list[Any] | str | int | float | bool | None


def _column_name(column: Any, alias_type: AliasType = "serialization") -> str:
    if alias_type == "validation":
        return column.validation_name
    return column.storage_name


def _resolve_json_schema_refs(
    schema: JsonSchemaValue, defs: dict[str, Any] | None = None
) -> JsonSchemaValue:
    """Resolve $ref references in a JSON schema by inlining the referenced schemas.

    Args:
        schema: The JSON schema value that may contain $ref (dict, list, or primitive)
        defs: The $defs section containing referenced schemas

    Returns:
        JsonSchemaValue: Schema with $ref references resolved inline
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


def validate_schema(
    model: type[FastDataframeModel],
    frame: pl.LazyFrame | pl.DataFrame,
    *,
    canonical: bool = True,
) -> list[ValidationError]:
    """Validate a Polars frame schema against a FastDataFrame model."""
    model_json_schema = model.model_json_schema()
    df_json_schema = _extract_polars_frame_json_schema(frame)

    defs = model_json_schema.get("$defs", {})
    if defs:
        resolved_properties = {}
        for prop_name, prop_schema in model_json_schema.get("properties", {}).items():
            resolved_properties[prop_name] = _resolve_json_schema_refs(
                prop_schema, defs
            )
        resolved_model_schema = model_json_schema.copy()
        resolved_model_schema["properties"] = resolved_properties
        model_json_schema = resolved_model_schema

    if canonical:
        storage_names = [column.storage_name for column in model.column_definitions]
        model_json_schema = model_json_schema.copy()
        model_json_schema["required"] = storage_names
        if "properties" in model_json_schema:
            model_json_schema["properties"] = {
                column.storage_name: model_json_schema["properties"].get(
                    column.python_name,
                    model_json_schema["properties"].get(column.storage_name, {}),
                )
                for column in model.column_definitions
            }

    errors = {}
    errors.update(validate_missing_columns(model_json_schema, df_json_schema))
    errors.update(validate_column_types(model_json_schema, df_json_schema))
    return list(errors.values())


def schema(
    model: type[FastDataframeModel], alias_type: AliasType = "serialization"
) -> pl.Schema:
    """Generate a Polars schema from a FastDataFrame model."""
    return pl.Schema(
        {
            _column_name(column, alias_type): get_polars_type_from_column(
                column, alias_type
            )
            for column in model.column_definitions
        }
    )


def string_schema(
    model: type[FastDataframeModel], alias_type: AliasType = "serialization"
) -> pl.Schema:
    """Generate a Polars schema where every model column is String."""
    return pl.Schema(
        {
            _column_name(column, alias_type): pl.String
            for column in model.column_definitions
        }
    )


def rename(
    model: type[FastDataframeModel],
    df: pl.DataFrame | pl.LazyFrame,
    alias_type_from: AliasType = "serialization",
    alias_type_to: AliasType = "serialization",
    *,
    strict: bool = True,
) -> pl.DataFrame | pl.LazyFrame:
    """Rename dataframe columns between FastDataFrame name sets."""
    model_map = {
        _column_name(column, alias_type_from): _column_name(column, alias_type_to)
        for column in model.column_definitions
    }
    df_schema = df.collect_schema()
    missing = set(df_schema.keys()) - set(model_map.keys())
    if strict and missing:
        names = ", ".join(sorted(missing))
        raise KeyError(f"DataFrame contains columns not defined by model: {names}")
    rename_map = {
        field_name: model_map[field_name]
        for field_name in df_schema.keys()
        if field_name in model_map
    }
    return df.rename(rename_map)


def cast(
    model: type[FastDataframeModel],
    df: Union[pl.DataFrame, pl.LazyFrame],
    alias_type: AliasType = "serialization",
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Cast DataFrame or LazyFrame columns to match the model schema."""
    source_schema = df.collect_schema()
    target_schema = schema(model, alias_type)
    cast_functions = []

    for column in model.column_definitions:
        target_col = _column_name(column, alias_type)
        target_type = target_schema[target_col]
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
                column.info,
            )
        )

    return df.with_columns(cast_functions)


class PolarsFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Polars integration."""

    @classmethod
    def from_base_model(cls, model: type[BaseModel]):
        """Create a Polars-compatible model from a Pydantic model."""
        field_definitions = {
            field_name: (field_type.annotation, field_type)
            for field_name, field_type in model.model_fields.items()
        }
        new_model = create_model(  # type: ignore[no-matching-overload]
            f"{model.__name__}Polars",
            __base__=cls,
            __doc__=f"Polars version of {model.__name__}",
            **field_definitions,
        )
        return typing_cast(type[PolarsFastDataframeModel], new_model)

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
        return validate_schema(cls, frame, canonical=False)

    @classmethod
    def get_polars_schema(cls, alias_type: AliasType = "serialization") -> pl.Schema:
        """Get the polars schema for the model."""
        return schema(cls, alias_type)

    @classmethod
    def get_stringified_schema(
        cls, alias_type: AliasType = "serialization"
    ) -> pl.Schema:
        """Get the polars schema for the model with all columns as strings."""
        return string_schema(cls, alias_type)

    @classmethod
    def rename(
        cls,
        df: pl.DataFrame | pl.LazyFrame,
        alias_type_from: AliasType = "serialization",
        alias_type_to: AliasType = "serialization",
        *,
        strict: bool = False,
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
            strict: Whether to raise a KeyError when the dataframe contains columns
                that are not defined by the model. Defaults to False for backwards
                compatibility with the previous classmethod behavior.

        Returns:
            pl.DataFrame | pl.LazyFrame: New dataframe with renamed columns. Maintains original type
            (eager DataFrame or LazyFrame) of input.

        Raises:
            KeyError: If strict=True and any existing column name is not found in
                the model's schema.

        Example:
            ```python
            # Convert from database column names to validation names
            df = MyModel.rename(df, alias_type_from='serialization', alias_type_to='validation')

            # Convert back to serialization names for storage
            df = MyModel.rename(df, alias_type_from='validation', alias_type_to='serialization')
            ```
        """
        return rename(cls, df, alias_type_from, alias_type_to, strict=strict)

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
        return cast(cls, df, alias_type)

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
