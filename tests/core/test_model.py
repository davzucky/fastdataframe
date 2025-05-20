"""Tests for FastDataframe model implementation."""

from typing import Optional, Union, Annotated, List, Dict, Any
from pydantic import Field
import pytest

from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.annotation import FastDataframe

@pytest.mark.parametrize(
    "field_type,expected_is_nullable,expected_is_unique",
    [
        # Basic types
        (int, False, False),
        (str, False, False),
        (float, False, False),
        (bool, False, False),
        
        # Optional types
        (Optional[int], True, False),
        (Optional[str], True, False),
        (Optional[float], True, False),
        (Optional[bool], True, False),
        
        # Union types with None
        (Union[int, None], True, False),
        (Union[str, None], True, False),
        (Union[float, None], True, False),
        (Union[bool, None], True, False),
        
        # Union types without None
        (Union[int, str], False, False),
        (Union[float, bool], False, False),
        
        # Collection types
        (List[int], False, False),
        (Dict[str, Any], False, False),
        
        # Optional collections
        (Optional[List[int]], True, False),
        (Optional[Dict[str, Any]], True, False),
        
        # Annotated types with explicit metadata
        (Annotated[int, FastDataframe(is_unique=True)], False, True),
        (Annotated[str, FastDataframe(is_unique=True)], False, True),
        (Annotated[Optional[int], FastDataframe(is_unique=False)], True, False),
        (Annotated[Optional[str], FastDataframe(is_unique=True)], True, True),

        # Annotation that contains Fiels
        (Annotated[int, Field()], False, False),
        (Annotated[Optional[str], Field()], True, False),
    ],
)
def test_model_base_type(field_type: Any, expected_is_nullable: bool, expected_is_unique: bool):
    """Test that model fields have correct is_nullable and is_unique properties based on their type.
    
    Args:
        field_type: The type to test
        expected_is_nullable: The expected value of is_nullable
        expected_is_unique: The expected value of is_unique
    """
    # Create a model with a field of the given type
    class MyModel(FastDataframeModel):
        test_field: field_type
    
    # Get the schema
    schema = MyModel.model_json_schema()
    
    # Check that the field has FastDataframe metadata
    assert "_fastdataframe" in schema["properties"]["test_field"]
    
    # Check that is_nullable and is_unique match the expected values
    fastdataframe_props = schema["properties"]["test_field"]["_fastdataframe"]["properties"]
    assert fastdataframe_props["is_nullable"] is expected_is_nullable
    assert fastdataframe_props["is_unique"] is expected_is_unique
