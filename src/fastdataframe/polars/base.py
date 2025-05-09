"""Base functionality for Polars operations."""

from typing import Any, Dict, List, Optional, Type, TypeVar, Union
import polars as pl
from fastdataframe.core.model import FastDataframeModel

T = TypeVar("T", bound=FastDataframeModel)

def get_model_schema(model_class: Type[T]) -> Dict[str, Any]:
    """
    Get the schema from a model class.
    
    Args:
        model_class: The model class to get the schema from
        
    Returns:
        Dictionary mapping field names to their types
    """
    return {
        field_name: field_info.annotation
        for field_name, field_info in model_class.model_fields.items()
    }

def validate_schema(
    columns: List[str],
    dtypes: List[pl.DataType],
    model_class: Type[T],
) -> None:
    """
    Validate that a schema matches the model schema.
    
    Args:
        columns: List of column names
        dtypes: List of column data types
        model_class: The model class to validate against
        
    Raises:
        ValueError: If the schema doesn't match the model schema
    """
    # Get model schema
    model_schema = get_model_schema(model_class)
    
    # Get DataFrame schema
    df_schema = dict(zip(columns, dtypes))
    
    # Check if all model fields are present in DataFrame
    missing_fields = set(model_schema.keys()) - set(df_schema.keys())
    if missing_fields:
        raise ValueError(f"Schema is missing required fields: {missing_fields}")
    
    # Check if all DataFrame columns are present in model
    extra_fields = set(df_schema.keys()) - set(model_schema.keys())
    if extra_fields:
        raise ValueError(f"Schema has extra fields not in model: {extra_fields}")

def models_to_dicts(models: List[T]) -> List[Dict[str, Any]]:
    """
    Convert a list of model instances to dictionaries.
    
    Args:
        models: List of model instances to convert
        
    Returns:
        List of dictionaries containing the model data
    """
    return [model.model_dump() for model in models]

__all__ = ["get_model_schema", "validate_schema", "models_to_dicts"] 