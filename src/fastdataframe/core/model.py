from typing import Any, Dict, Optional, Type, TypeVar, cast
from pydantic import BaseModel, create_model
from pydantic._internal._model_construction import ModelMetaclass as PydanticModelMetaclass

T = TypeVar("T", bound="FastDataframeModel")

class ModelMetaclass(PydanticModelMetaclass):
    """
    Custom metaclass for DataFrame models that extends Pydantic's ModelMetaclass.
    This allows us to add DataFrame-specific functionality while maintaining Pydantic's features.
    """
    def __new__(
        mcs,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type[Any]:
        # Create the model class using Pydantic's metaclass
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        
        # Add DataFrame-specific attributes and methods here
        # For example, we can add methods to convert to/from pandas DataFrame
        
        return cls

class FastDataframeModel(BaseModel, metaclass=ModelMetaclass):
    """
    Base class for DataFrame models that combines Pydantic's validation
    with DataFrame-specific functionality.
    """
    model_config = {
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
    }

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """Create a model instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary."""
        return self.model_dump()

    # We'll add DataFrame-specific methods here later
    # For example: to_dataframe(), from_dataframe(), etc.

__all__ = ["FastDataframeModel", "ModelMetaclass"] 