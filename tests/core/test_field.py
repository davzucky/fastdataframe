from pydantic import BaseModel
from fastdataframe.core.field import FieldInfo
from typing import Optional, Union, Any

def test_field_with_default():
    """Test Field with a default value"""
    field = FieldInfo(default="test", unique=True, allow_missing=False)
    assert isinstance(field, FieldInfo)
    assert field.default == "test"
    assert field.unique is True
    assert field.allow_missing is False

def test_field_with_default_factory():
    """Test Field with a default_factory"""
    field = FieldInfo(default_factory=list, unique=False, allow_missing=True)
    assert isinstance(field, FieldInfo)
    assert field.default_factory == list
    assert field.unique is False
    assert field.allow_missing is True

def test_field_without_default():
    """Test Field without default value"""
    field = FieldInfo(unique=True, allow_missing=True)
    assert isinstance(field, FieldInfo)
    assert field.default is None
    assert field.unique is True
    assert field.allow_missing is True

def test_field_in_model():
    """Test Field usage in a Pydantic model"""
    class TestModel(BaseModel):
        name: str = FieldInfo(default="John", unique=True)
        age: int = FieldInfo(allow_missing=True)
        items: list = FieldInfo(default_factory=list, unique=True)

    model = TestModel()
    assert model.name == "John"
    assert model.age is None
    assert model.items == []

    # Test model validation
    model = TestModel(name="Jane", age=25, items=[1, 2, 3])
    assert model.name == "Jane"
    assert model.age == 25
    assert model.items == [1, 2, 3]

def test_field_info_allow_null():
    """Test the allow_null property of FieldInfo."""
    # Test with non-optional type
    field = FieldInfo(annotation=int)
    assert not field.allow_null

    # Test with Optional type
    field = FieldInfo(annotation=Optional[int])
    assert field.allow_null

    # Test with Union type containing None
    field = FieldInfo(annotation=Union[int, None])
    assert field.allow_null

    # Test with direct None type
    field = FieldInfo(annotation=type(None))
    assert field.allow_null

    # Test with Union type not containing None
    field = FieldInfo(annotation=Union[int, str])
    assert not field.allow_null

    # Test with no annotation
    field = FieldInfo()
    assert not field.allow_null

    # Test with Any type
    field = FieldInfo(annotation=Any)
    assert not field.allow_null 