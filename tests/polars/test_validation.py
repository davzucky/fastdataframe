"""Tests for validation models."""

import pytest
import polars as pl
from fastdataframe.polars.model import PolarsFastDataframeModel
from fastdataframe.core.validation import ValidationError

def test_validate_schema_missing_column():
    """Test that validate_schema correctly identifies missing columns."""
    class MyModel(PolarsFastDataframeModel):
        field1: int
        field2: str

    # Create a lazy frame missing 'field2'
    lazy_frame = pl.LazyFrame({"field1": [1, 2, 3]})

    errors = MyModel.validate_schema(lazy_frame)
    assert len(errors) == 1
    assert errors[0].column_name == "field2"
    assert errors[0].error_type == "MissingColumn"
    assert errors[0].error_details == "Column field2 is missing in the frame."

def test_validate_schema_type_mismatch():
    """Test that validate_schema correctly identifies type mismatches."""
    class MyModel(PolarsFastDataframeModel):
        field1: int
        field2: str

    # Create a lazy frame with incorrect type for 'field1'
    lazy_frame = pl.LazyFrame({"field1": ["1", "2", "3"], "field2": ["a", "b", "c"]})

    errors = MyModel.validate_schema(lazy_frame)
    assert len(errors) == 1
    assert errors[0].column_name == "field1"
    assert errors[0].error_type == "TypeMismatch"
    assert errors[0].error_details == "Expected type int, but got str." 