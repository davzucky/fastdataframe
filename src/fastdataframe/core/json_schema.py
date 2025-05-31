from fastdataframe.core.validation import ValidationError
from fastdataframe.core.types import get_type_name, json_schema_is_subset
from typing import Any


def validate_missing_columns(
    model_json_schema: dict, df_json_schema: dict
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


def validate_column_types(
    model_json_schema: dict[str, Any], df_json_schema: dict[str, Any]
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
