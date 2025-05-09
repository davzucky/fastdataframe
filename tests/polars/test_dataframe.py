"""Tests for Polars DataFrame operations."""

from typing import Optional
import pytest
import polars as pl
from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.field import Field
from fastdataframe.polars.dataframe import to_dataframe, from_dataframe, validate_dataframe

class UserModel(FastDataframeModel):
    """Test model for User data."""
    name: str = Field(description="User's full name")
    age: int = Field(ge=0, description="User's age in years")
    email: Optional[str] = Field(default=None, description="User's email address")

def test_to_dataframe():
    """Test converting models to DataFrame."""
    users = [
        UserModel(name="John Doe", age=30, email="john@example.com"),
        UserModel(name="Jane Doe", age=25),
    ]
    
    df = to_dataframe(users)
    
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, 3)
    assert list(df.columns) == ["name", "age", "email"]
    assert df["name"].to_list() == ["John Doe", "Jane Doe"]
    assert df["age"].to_list() == [30, 25]
    assert df["email"].to_list() == ["john@example.com", None]

def test_from_dataframe():
    """Test converting DataFrame to models."""
    df = pl.DataFrame({
        "name": ["John Doe", "Jane Doe"],
        "age": [30, 25],
        "email": ["john@example.com", None]
    })
    
    users = from_dataframe(df, UserModel)
    
    assert len(users) == 2
    assert users[0].name == "John Doe"
    assert users[0].age == 30
    assert users[0].email == "john@example.com"
    assert users[1].name == "Jane Doe"
    assert users[1].age == 25
    assert users[1].email is None

def test_validate_dataframe():
    """Test DataFrame validation."""
    # Valid DataFrame
    valid_df = pl.DataFrame({
        "name": ["John Doe"],
        "age": [30],
        "email": ["john@example.com"]
    })
    validate_dataframe(valid_df, UserModel)  # Should not raise
    
    # Missing required field
    invalid_df = pl.DataFrame({
        "name": ["John Doe"],
        "email": ["john@example.com"]
    })
    with pytest.raises(ValueError, match="DataFrame is missing required fields"):
        validate_dataframe(invalid_df, UserModel)
    
    # Extra field
    extra_field_df = pl.DataFrame({
        "name": ["John Doe"],
        "age": [30],
        "email": ["john@example.com"],
        "extra": ["field"]
    })
    with pytest.raises(ValueError, match="DataFrame has extra fields not in model"):
        validate_dataframe(extra_field_df, UserModel)

def test_empty_dataframe():
    """Test handling of empty DataFrames."""
    # Empty list to DataFrame
    empty_df = to_dataframe([])
    assert isinstance(empty_df, pl.DataFrame)
    assert empty_df.is_empty()
    
    # Empty DataFrame to models
    empty_models = from_dataframe(pl.DataFrame(), UserModel)
    assert isinstance(empty_models, list)
    assert len(empty_models) == 0 