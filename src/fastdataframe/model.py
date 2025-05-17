"""FastDataframe model implementation."""

from typing import Any, Dict, Type, TypeVar

from pydantic import BaseModel, ConfigDict

from fastdataframe.metadata import FastDataframe

T = TypeVar("T", bound="FastDataframeModel")


class FastDataframeModel(BaseModel):
    """Base model that enforces FastDataframe annotation on all fields."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init_subclass__(cls: Type[T], **kwargs: Any) -> None:
        """Initialize subclass and ensure all fields have FastDataframe annotation."""
        super().__init_subclass__(**kwargs)
        
        # Get all fields from the model
        fields = cls.model_fields
        
        # Check each field for FastDataframe annotation
        for field_name, field in fields.items():
            extra = getattr(field, 'json_schema_extra', None)
            if not isinstance(extra, dict) or "_fastdataframe" not in extra:
                raise ValueError(
                    f"Field '{field_name}' must have FastDataframe annotation. "
                    "Use FastDataframe() to annotate the field."
                )

    @classmethod
    def model_json_schema(cls, **kwargs: Any) -> Dict[str, Any]:
        """Generate JSON schema with FastDataframe metadata."""
        schema = super().model_json_schema(**kwargs)
        
        # Add FastDataframe metadata to each property
        for field_name, field in cls.model_fields.items():
            if isinstance(field.json_schema_extra, dict) and "_fastdataframe" in field.json_schema_extra:
                schema["properties"][field_name]["_fastdataframe"] = field.json_schema_extra["_fastdataframe"]
        
        return schema 