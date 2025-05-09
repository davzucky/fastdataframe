from pydantic import BaseModel
from fastdataframe.core.field_info import Field, FieldInfo

def test_field_with_default():
    """Test Field with a default value"""
    field = Field(default="test", unique=True, allow_missing=False)
    assert isinstance(field, FieldInfo)
    assert field.default == "test"
    assert field.unique is True
    assert field.allow_missing is False

def test_field_with_default_factory():
    """Test Field with a default_factory"""
    field = Field(default_factory=list, unique=False, allow_missing=True)
    assert isinstance(field, FieldInfo)
    assert field.default_factory == list
    assert field.unique is False
    assert field.allow_missing is True

def test_field_without_default():
    """Test Field without default value"""
    field = Field(unique=True, allow_missing=True)
    assert isinstance(field, FieldInfo)
    assert field.default is None
    assert field.unique is True
    assert field.allow_missing is True

def test_field_in_model():
    """Test Field usage in a Pydantic model"""
    class TestModel(BaseModel):
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