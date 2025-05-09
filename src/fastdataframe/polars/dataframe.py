"""Polars DataFrame operations for FastDataframe."""

from typing import Any, Dict, List, Optional, Type, TypeVar, Union
import polars as pl
from fastdataframe.core.model import FastDataframeModel

T = TypeVar("T", bound=FastDataframeModel)

def to_dataframe(models: List[T]) -> pl.DataFrame:
    """
    Convert a list of FastDataframeModel instances to a Polars DataFrame.
    
    Args:
        models: List of model instances to convert
        
    Returns:
        Polars DataFrame containing the model data
    """
    if not models:
        return pl.DataFrame()
    
    # Get the model class to determine the schema
    model_class = type(models[0])
    schema = {
        field_name: field_info.annotation
        for field_name, field_info in model_class.model_fields.items()
    }
    
    # Convert models to dictionaries
    data = [model.model_dump() for model in models]
    
    # Create DataFrame
    return pl.DataFrame(data, schema=schema)

def from_dataframe(df: pl.DataFrame, model_class: Type[T]) -> List[T]:
    """
    Convert a Polars DataFrame to a list of FastDataframeModel instances.
    
    Args:
        df: Polars DataFrame to convert
        model_class: The model class to instantiate
        
    Returns:
        List of model instances
    """
    if df.is_empty():
        return []
    
    # Convert DataFrame to list of dictionaries
    data = df.to_dicts()
    
    # Create model instances
    return [model_class(**row) for row in data]

def validate_dataframe(df: pl.DataFrame, model_class: Type[T]) -> None:
    """
    Validate that a Polars DataFrame matches the schema of a FastDataframeModel.
    
    Args:
        df: Polars DataFrame to validate
        model_class: The model class to validate against
        
    Raises:
        ValueError: If the DataFrame schema doesn't match the model schema
    """
    # Get model schema
    model_schema = {
        field_name: field_info.annotation
        for field_name, field_info in model_class.model_fields.items()
    }
    
    # Get DataFrame schema
    df_schema = dict(zip(df.columns, df.dtypes))
    
    # Check if all model fields are present in DataFrame
    missing_fields = set(model_schema.keys()) - set(df_schema.keys())
    if missing_fields:
        raise ValueError(f"DataFrame is missing required fields: {missing_fields}")
    
    # Check if all DataFrame columns are present in model
    extra_fields = set(df_schema.keys()) - set(model_schema.keys())
    if extra_fields:
        raise ValueError(f"DataFrame has extra fields not in model: {extra_fields}")

__all__ = ["to_dataframe", "from_dataframe", "validate_dataframe"] 