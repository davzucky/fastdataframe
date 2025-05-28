"""Tests for validation models."""

import polars as pl
from fastdataframe.polars.model import PolarsFastDataframeModel
from fastdataframe.core.model import FastDataframeModel
from typing import Optional
import datetime as dt


class TestModel(PolarsFastDataframeModel):
    __test__ = False
    """Test model for validation tests."""
    field1: int
    field2: str


def test_from_fastdataframe_model_basic_conversion() -> None:
    """Test basic conversion of FastDataframeModel to PolarsFastDataframeModel."""

    # Create a base model
    class BaseModel(FastDataframeModel):
        """Base model for testing conversion."""

        name: str
        age: int
        is_active: bool
        score: Optional[float] = None

    # Convert to PolarsFastDataframeModel
    PolarsModel = PolarsFastDataframeModel.from_fastdataframe_model(BaseModel)

    # Verify the new class inherits from PolarsFastDataframeModel
    assert issubclass(PolarsModel, PolarsFastDataframeModel)

    # Verify the class name is correct
    assert PolarsModel.__name__ == "BaseModelPolars"

    # Verify all fields are preserved
    assert PolarsModel.__annotations__ == BaseModel.__annotations__

    # Verify the docstring is updated
    assert PolarsModel.__doc__ == "Polars version of BaseModel"


def test_from_fastdataframe_model_valid_frame() -> None:
    """Test validation of a valid frame with the converted model."""

    class BaseModel(FastDataframeModel):
        name: str
        age: int
        is_active: bool
        score: Optional[float] = None

    PolarsModel = PolarsFastDataframeModel.from_fastdataframe_model(BaseModel)

    # Test validation with a valid frame
    valid_frame = pl.LazyFrame(
        {
            "name": ["John", "Jane"],
            "age": [30, 25],
            "is_active": [True, False],
            "score": [95.5, None],
        }
    )
    errors = PolarsModel.validate_schema(valid_frame)
    assert len(errors) == 0, "Valid frame should not have validation errors"


def test_from_fastdataframe_model_missing_optional() -> None:
    """Test validation of a frame missing an optional field."""

    class BaseModel(FastDataframeModel):
        name: str
        age: int
        is_active: bool
        score: Optional[float] = None

    PolarsModel = PolarsFastDataframeModel.from_fastdataframe_model(BaseModel)

    # Test validation with an invalid frame (missing required field)
    invalid_frame = pl.LazyFrame(
        {
            "name": ["John", "Jane"],
            "age": [30, 25],
            "is_active": [True, False],
            # score is missing but it's optional
        }
    )
    errors = PolarsModel.validate_schema(invalid_frame)
    assert len(errors) == 0, (
        "Frame missing optional field should not have validation errors"
    )


def test_from_fastdataframe_model_type_mismatch() -> None:
    """Test validation of a frame with type mismatches."""

    class BaseModel(FastDataframeModel):
        name: str
        age: int
        is_active: bool
        score: Optional[float] = None

    PolarsModel = PolarsFastDataframeModel.from_fastdataframe_model(BaseModel)

    # Test validation with type mismatch
    type_mismatch_frame = pl.LazyFrame(
        {
            "name": ["John", "Jane"],
            "age": ["30", "25"],  # age should be int
            "is_active": [True, False],
            "score": ["95.5", None],  # score should be float or None
        }
    )
    errors = PolarsModel.validate_schema(type_mismatch_frame)
    assert len(errors) == 2, (
        "Frame with type mismatches should have two validation errors"
    )
    error_types = {error.column_name: error.error_type for error in errors}
    assert "age" in error_types
    assert "score" in error_types
    assert error_types["age"] == "TypeMismatch"
    assert error_types["score"] == "TypeMismatch"


def test_validate_missing_columns() -> None:
    """Test that validate_schema correctly identifies missing columns."""
    # Create a lazy frame missing 'field2'
    lazy_frame = pl.LazyFrame({"field1": [1, 2, 3]})

    errors = TestModel.validate_schema(lazy_frame)
    assert len(errors) == 1
    assert errors[0].column_name == "field2"
    assert errors[0].error_type == "MissingColumn"
    assert errors[0].error_details == "Column field2 is missing in the frame."


def test_validate_column_types() -> None:
    """Test that validate_schema correctly identifies type mismatches."""
    # Create a lazy frame with incorrect type for 'field1'
    lazy_frame = pl.LazyFrame({"field1": ["1", "2", "3"], "field2": ["a", "b", "c"]})

    errors = TestModel.validate_schema(lazy_frame)
    assert len(errors) == 1
    assert errors[0].column_name == "field1"
    assert errors[0].error_type == "TypeMismatch"
    assert errors[0].error_details == "Expected type integer, but got string."


def test_validate_schema_valid_frame() -> None:
    """Test that validate_schema returns no errors for a valid frame."""
    # Create a valid lazy frame
    lazy_frame = pl.LazyFrame({"field1": [1, 2, 3], "field2": ["a", "b", "c"]})

    errors = TestModel.validate_schema(lazy_frame)
    assert len(errors) == 0


def test_polarsfastdataframemodel_with_temporal_types() -> None:
    """Test PolarsFastDataframeModel schema validation with date, datetime, time, and timedelta fields."""

    class TemporalModel(FastDataframeModel):
        d: dt.date
        dt_: dt.datetime
        t: dt.time
        td: dt.timedelta

    PolarsModel = PolarsFastDataframeModel.from_fastdataframe_model(TemporalModel)

    today = dt.date.today()
    now = dt.datetime.now()
    t = now.time()
    td_ = dt.timedelta(days=1, hours=2)

    # Use ISO 8601 duration string for timedelta
    frame = pl.LazyFrame(
        {
            "d": [today, today],
            "dt_": [now, now],
            "t": [t, t],
            "td": [td_, td_],
        }
    )
    errors = PolarsModel.validate_schema(frame)
    assert len(errors) == 0, (
        f"Temporal types frame should not have validation errors, got: {errors}"
    )
