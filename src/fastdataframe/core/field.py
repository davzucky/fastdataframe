from typing import Any, Optional, TypeVar, overload, Union
from pydantic.fields import FieldInfo as PydanticFieldInfo
from pydantic import Field as PydanticField

T = TypeVar("T")

class FieldInfo(PydanticFieldInfo):
    """
    Extended FieldInfo class that adds additional attributes for DataFrame column configuration.
    """
    def __init__(
        self,
        *,
        unique: bool = False,
        allow_missing: bool = False,
        **kwargs: Any,
    ) -> None:
        # If allow_missing is True and no default is provided, set default to None
        if allow_missing and "default" not in kwargs and "default_factory" not in kwargs:
            kwargs["default"] = None
        super().__init__(**kwargs)
        self.unique = unique
        self.allow_missing = allow_missing

    @property
    def allow_null(self) -> bool:
        """Check if the field's annotation allows None values.
        
        Returns:
            bool: True if the field can be None, False otherwise.
        """
        annotation = getattr(self, "annotation", None)
        if annotation is None:
            return False
        
        # Handle Optional types
        if hasattr(annotation, "__origin__") and annotation.__origin__ is Union:
            return type(None) in annotation.__args__
        
        # Handle direct None type
        return annotation is type(None)

    def model_dump(self) -> dict[str, Any]:
        """Convert the field info to a dictionary."""
        base_dict = {
            "default": getattr(self, "default", None),
            "default_factory": getattr(self, "default_factory", None),
            "annotation": getattr(self, "annotation", None),
            "alias": getattr(self, "alias", None),
            "title": getattr(self, "title", None),
            "description": getattr(self, "description", None),
            "gt": getattr(self, "gt", None),
            "ge": getattr(self, "ge", None),
            "lt": getattr(self, "lt", None),
            "le": getattr(self, "le", None),
            "min_length": getattr(self, "min_length", None),
            "max_length": getattr(self, "max_length", None),
            "pattern": getattr(self, "pattern", None),
            "discriminator": getattr(self, "discriminator", None),
            "strict": getattr(self, "strict", None),
            "multiple_of": getattr(self, "multiple_of", None),
            "allow_inf_nan": getattr(self, "allow_inf_nan", None),
            "max_digits": getattr(self, "max_digits", None),
            "decimal_places": getattr(self, "decimal_places", None),
            "unique_items": getattr(self, "unique_items", None),
        }
        # Remove None values to avoid overriding defaults
        return {k: v for k, v in base_dict.items() if v is not None}

@overload
def Field(
    *,
    unique: bool = False,
    allow_missing: bool = False,
    default: T,
    **kwargs: Any,
) -> FieldInfo:
    ...

@overload
def Field(
    *,
    unique: bool = False,
    allow_missing: bool = False,
    default_factory: Any,
    **kwargs: Any,
) -> FieldInfo:
    ...

@overload
def Field(
    *,
    unique: bool = False,
    allow_missing: bool = False,
    **kwargs: Any,
) -> FieldInfo:
    ...

def Field(
    *,
    unique: bool = False,
    allow_missing: bool = False,
    **kwargs: Any,
) -> FieldInfo:
    """
    Create a field with additional DataFrame-specific attributes.
    
    Args:
        unique: Whether the column should contain unique values
        allow_missing: Whether the column can contain missing values
        **kwargs: Additional arguments passed to pydantic.Field
    
    Returns:
        A field with the specified configuration
    """
    return FieldInfo(
        unique=unique,
        allow_missing=allow_missing,
        **kwargs
    )

__all__ = ["FieldInfo", "Field"] 

