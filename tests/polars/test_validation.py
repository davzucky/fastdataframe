"""Tests for validation models."""

import polars as pl
from fastdataframe.polars.model import PolarsFastDataframeModel

class TestModel(PolarsFastDataframeModel):
    """Test model for validation tests."""
    field1: int
    field2: str

def test_validate_missing_columns():
    """Test that validate_schema correctly identifies missing columns."""
    # Create a lazy frame missing 'field2'
    lazy_frame = pl.LazyFrame({"field1": [1, 2, 3]})

    errors = TestModel.validate_schema(lazy_frame)
    assert len(errors) == 1
    assert errors[0].column_name == "field2"
    assert errors[0].error_type == "MissingColumn"
    assert errors[0].error_details == "Column field2 is missing in the frame."

def test_validate_column_types():
    """Test that validate_schema correctly identifies type mismatches."""
    # Create a lazy frame with incorrect type for 'field1'
    lazy_frame = pl.LazyFrame({"field1": ["1", "2", "3"], "field2": ["a", "b", "c"]})

    errors = TestModel.validate_schema(lazy_frame)
    assert len(errors) == 1
    assert errors[0].column_name == "field1"
    assert errors[0].error_type == "TypeMismatch"
    assert errors[0].error_details == "Expected type integer, but got string."

def test_validate_schema_valid_frame():
    """Test that validate_schema returns no errors for a valid frame."""
    # Create a valid lazy frame
    lazy_frame = pl.LazyFrame({"field1": [1, 2, 3], "field2": ["a", "b", "c"]})

    errors = TestModel.validate_schema(lazy_frame)
    assert len(errors) == 0 