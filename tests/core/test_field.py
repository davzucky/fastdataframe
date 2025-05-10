from pydantic import BaseModel
from fastdataframe.core.field import Field
from fastdataframe.core.model import FastDataframeModel
from typing import Optional, Union, Any

def test_field_with_default():
    """Test Field with a default value"""
    class TestModel(FastDataframeModel):
        name: str = Field(default="test", unique=True, allow_missing=False)
    
    model = TestModel()
    assert model.name == "test"
    field_info = TestModel.model_fields["name"]
    assert field_info.unique is True
    assert field_info.allow_missing is False

def test_field_with_default_factory():
    """Test Field with a default_factory"""
    class TestModel(FastDataframeModel):
        items: list = Field(default_factory=list, unique=False, allow_missing=True)
    
    model = TestModel()
    assert model.items == []
    field_info = TestModel.model_fields["items"]
    assert field_info.unique is False
    assert field_info.allow_missing is True

def test_field_without_default():
    """Test Field without default value"""
    class TestModel(FastDataframeModel):
        name: str = Field(unique=True, allow_missing=True)
    
    field_info = TestModel.model_fields["name"]
    assert field_info.default is None
    assert field_info.unique is True
    assert field_info.allow_missing is True

def test_field_in_model():
    """Test Field usage in a FastDataframeModel"""
    class TestModel(FastDataframeModel):
        name: str = Field(default="John", unique=True)
        age: int = Field(allow_missing=True)
        items: list = Field(default_factory=list, unique=True)

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
    """Test the allow_null property of Field."""
    class TestModel(FastDataframeModel):
        # Test with non-optional type
        required_int: int = Field()
        
        # Test with Optional type
        optional_int: Optional[int] = Field()
        
        # Test with Union type containing None
        union_int: Union[int, None] = Field()
        
        # Test with direct None type
        none_type: type(None) = Field()
        
        # Test with Union type not containing None
        union_int_str: Union[int, str] = Field()
        
        # Test with Any type
        any_type: Any = Field()

    # Test non-optional type
    assert not TestModel.model_fields["required_int"].allow_null

    # Test Optional type
    assert TestModel.model_fields["optional_int"].allow_null

    # Test Union type containing None
    assert TestModel.model_fields["union_int"].allow_null

    # Test direct None type
    assert TestModel.model_fields["none_type"].allow_null

    # Test Union type not containing None
    assert not TestModel.model_fields["union_int_str"].allow_null

    # Test Any type
    assert not TestModel.model_fields["any_type"].allow_null 