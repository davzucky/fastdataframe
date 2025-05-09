"""Tests for Polars LazyFrame operations."""

import pytest
import polars as pl
from fastdataframe.core.model import FastDataframeModel
from fastdataframe.polars.lazyframe import to_lazyframe, from_lazyframe, validate_lazyframe

class UserModel(FastDataframeModel):
    """Test model for LazyFrame operations."""
    name: str
    age: int
    email: str | None = None

def test_to_lazyframe():
    """Test converting models to LazyFrame."""
    models = [
        UserModel(name="Alice", age=30, email="alice@example.com"),
        UserModel(name="Bob", age=25),
    ]
    
    lf = to_lazyframe(models)
    assert isinstance(lf, pl.LazyFrame)
    
    # Collect and check data
    df = lf.collect()
    assert len(df) == 2
    assert df["name"].to_list() == ["Alice", "Bob"]
    assert df["age"].to_list() == [30, 25]
    assert df["email"].to_list() == ["alice@example.com", None]

def test_from_lazyframe():
    """Test converting LazyFrame to models."""
    df = pl.DataFrame({
        "name": ["Alice", "Bob"],
        "age": [30, 25],
        "email": ["alice@example.com", None]
    })
    lf = df.lazy()
    
    models = from_lazyframe(lf, UserModel)
    assert len(models) == 2
    assert models[0].name == "Alice"
    assert models[0].age == 30
    assert models[0].email == "alice@example.com"
    assert models[1].name == "Bob"
    assert models[1].age == 25
    assert models[1].email is None

def test_validate_lazyframe():
    """Test validating LazyFrame against model schema."""
    # Valid schema
    df = pl.DataFrame({
        "name": ["Alice"],
        "age": [30],
        "email": ["alice@example.com"]
    })
    lf = df.lazy()
    validate_lazyframe(lf, UserModel)  # Should not raise
    
    # Missing required field
    df = pl.DataFrame({
        "name": ["Alice"],
        "email": ["alice@example.com"]
    })
    lf = df.lazy()
    with pytest.raises(ValueError, match="Schema is missing required fields"):
        validate_lazyframe(lf, UserModel)
    
    # Extra field
    df = pl.DataFrame({
        "name": ["Alice"],
        "age": [30],
        "email": ["alice@example.com"],
        "extra": ["field"]
    })
    lf = df.lazy()
    with pytest.raises(ValueError, match="Schema has extra fields not in model"):
        validate_lazyframe(lf, UserModel)

def test_empty_lazyframe():
    """Test handling empty LazyFrames."""
    # Empty to_lazyframe
    lf = to_lazyframe([])
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().is_empty()
    
    # Empty from_lazyframe
    df = pl.DataFrame({
        "name": [],
        "age": [],
        "email": []
    })
    lf = df.lazy()
    models = from_lazyframe(lf, UserModel)
    assert len(models) == 0 