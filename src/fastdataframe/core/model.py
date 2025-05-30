"""FastDataframe model implementation."""

from typing import Any, TypeVar, Annotated, get_args, get_origin

from pydantic import BaseModel
from pydantic._internal._model_construction import (
    ModelMetaclass as PydanticModelMetaclass,
)

from .annotation import ColumnInfo
from .types_helper import contains_type

T = TypeVar("T", bound="FastDataframeModel")


class FastDataframeModelMetaclass(PydanticModelMetaclass):
    """Metaclass that enforces FastDataframe metadata on all fields."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        class_dict: dict[str, Any],
        **kwargs: Any,
    ) -> type:
        """Create a new class with FastDataframe metadata on all fields."""

        new_class_dict = class_dict.copy()
        if "__annotations__" not in new_class_dict:
            return super().__new__(mcs, name, bases, new_class_dict, **kwargs)

        # annotations = new_class_dict["__annotations__"]
        for field_name, field_type in new_class_dict["__annotations__"].items():
            origin = get_origin(field_type)
            args = get_args(field_type)
            if origin is not Annotated:
                new_class_dict["__annotations__"][field_name] = Annotated[
                    field_type, ColumnInfo.from_field_type(field_type)
                ]
            elif origin is Annotated and contains_type(list(args), ColumnInfo):
                # Just keep the FastDataframe as is, do not try to set nullability
                new_class_dict["__annotations__"][field_name] = field_type
            else:
                new_class_dict["__annotations__"][field_name] = Annotated[
                    args + (ColumnInfo.from_field_type(args[0]),)
                ]

        return super().__new__(mcs, name, bases, new_class_dict, **kwargs)


class FastDataframeModel(BaseModel, metaclass=FastDataframeModelMetaclass):
    """Base model that enforces FastDataframe annotation on all fields."""

    @classmethod
    def get_fastdataframe_annotations(cls) -> dict[str, ColumnInfo]:
        """Return a dictionary mapping field_name to FastDataframe annotation objects from the model_json_schema."""
        schema = cls.model_json_schema()
        fastdataframes = {}
        for field_name, field_schema in schema.get("properties", {}).items():
            fastdataframe_doc = field_schema.get("_fastdataframe")
            if fastdataframe_doc:
                fastdataframes[field_name] = ColumnInfo.from_schema(
                    {"json_schema_extra": {"_fastdataframe": fastdataframe_doc}}
                )
        return fastdataframes
