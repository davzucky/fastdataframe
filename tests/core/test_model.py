from typing import Optional
import pytest
from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.field import Field

class UserModel(FastDataframeModel):
    """Test model for User data."""
    name: str = Field(description="User's full name")
    age: int = Field(ge=0, description="User's age in years")
    email: Optional[str] = Field(default=None, description="User's email address")

def test_model_creation():
    """Test basic model creation and validation."""
    user = UserModel(name="John Doe", age=30)
    assert user.name == "John Doe"
    assert user.age == 30
    assert user.email is None

def test_model_validation():
    """Test model validation rules."""
    # Test valid data
    user = UserModel(name="Jane Doe", age=25, email="jane@example.com")
    assert user.email == "jane@example.com"

    # Test invalid age
    with pytest.raises(ValueError):
        UserModel(name="Invalid Age", age=-1)

def test_model_to_dict():
    """Test model conversion to dictionary."""
    user = UserModel(name="John Doe", age=30, email="john@example.com")
    data = user.to_dict()
    assert data["name"] == "John Doe"
    assert data["age"] == 30
    assert data["email"] == "john@example.com"

def test_model_from_dict():
    """Test model creation from dictionary."""
    data = {
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com"
    }
    user = UserModel.from_dict(data)
    assert user.name == "John Doe"
    assert user.age == 30
    assert user.email == "john@example.com" 