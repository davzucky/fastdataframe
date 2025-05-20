
from typing import Annotated, Any, Dict, List, Optional, Union

import pytest
from fastdataframe.core.types_helper import is_optional_type


@pytest.mark.parametrize(
    "field_type,expected",
    [
        # Optional types
        (Optional[int], True),
        (Optional[str], True),
        (Optional[List[int]], True),
        (Optional[Dict[str, Any]], True),
        
        # Union types with None
        (Union[int, None], True),
        (Union[str, None], True),
        (Union[None, int], True),
        
        # Union types without None
        (Union[int, str], False),
        (Union[List[int], Dict[str, Any]], False),
        
        # Annotated types
        (Annotated[Optional[int], "metadata"], True),
        (Annotated[Optional[str], "metadata"], True),
        (Annotated[Union[int, None], "metadata"], True),
        (Annotated[Union[str, None], "metadata"], True),
        (Annotated[int, "metadata"], False),
        (Annotated[str, "metadata"], False),
        
        # None type
        (type(None), True),
        
        # Basic non-optional types
        (int, False),
        (str, False),
        (float, False),
        (bool, False),
        
        # Collection types
        (list, False),
        (dict, False),
        (set, False),
        (tuple, False),
        
        # Complex types
        (List[int], False),
        (Dict[str, Any], False),
        
        # Nested types
        (Optional[Optional[int]], True),
        (Optional[List[Optional[int]]], True),
        (Union[Optional[int], None], True),
        (Union[List[Optional[int]], None], True),
        (Annotated[Optional[List[int]], "metadata"], True),
        (Annotated[Union[List[int], None], "metadata"], True),
    ],
)
def test_is_optional_type(field_type: Any, expected: bool):
    """Test the is_optional_type function with various type hints.
    
    Args:
        field_type: The type to check
        expected: The expected result (True if optional, False otherwise)
    """
    assert is_optional_type(field_type) is expected 