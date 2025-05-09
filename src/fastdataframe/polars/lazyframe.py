"""Polars LazyFrame operations for FastDataframe."""

from typing import Any, Dict, List, Optional, Type, TypeVar, Union
import polars as pl
from fastdataframe.core.model import FastDataframeModel
from fastdataframe.polars.base import get_model_schema, validate_schema, models_to_dicts

T = TypeVar("T", bound=FastDataframeModel)

def to_lazyframe(models: List[T]) -> pl.LazyFrame:
    """
    Convert a list of FastDataframeModel instances to a Polars LazyFrame.
    
    Args:
        models: List of model instances to convert
        
    Returns:
        Polars LazyFrame containing the model data
    """
    if not models:
        return pl.LazyFrame()
    
    # Get the model class to determine the schema
    model_class = type(models[0])
    schema = get_model_schema(model_class)
    
    # Convert models to dictionaries
    data = models_to_dicts(models)
    
    # Create LazyFrame
    return pl.LazyFrame(data, schema=schema)

def from_lazyframe(lf: pl.LazyFrame, model_class: Type[T]) -> List[T]:
    """
    Convert a Polars LazyFrame to a list of FastDataframeModel instances.
    
    Args:
        lf: Polars LazyFrame to convert
        model_class: The model class to instantiate
        
    Returns:
        List of model instances
    """
    # Collect the LazyFrame to a DataFrame first
    df = lf.collect()
    if df.is_empty():
        return []
    
    # Convert DataFrame to list of dictionaries
    data = df.to_dicts()
    
    # Create model instances
    return [model_class(**row) for row in data]

def validate_lazyframe(lf: pl.LazyFrame, model_class: Type[T]) -> None:
    """
    Validate that a Polars LazyFrame matches the schema of a FastDataframeModel.
    
    Args:
        lf: Polars LazyFrame to validate
        model_class: The model class to validate against
        
    Raises:
        ValueError: If the LazyFrame schema doesn't match the model schema
    """
    # Get schema from LazyFrame
    schema = lf.collect_schema()
    validate_schema(list(schema.keys()), list(schema.values()), model_class)

__all__ = ["to_lazyframe", "from_lazyframe", "validate_lazyframe"] 