"""FastDataframe model implementation."""

from typing import Any, TypeVar, Annotated, get_args, get_origin

from pydantic import BaseModel
from pydantic._internal._model_construction import (
    ModelMetaclass as PydanticModelMetaclass,
)

from .metadata import FastDataframe
from .types_helper import contains_type, filter_type, get_item_of_type

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
                    field_type, FastDataframe.from_field_type(field_type)
                ]
            elif origin is Annotated and contains_type(args, FastDataframe):
                fastdataframe_item = get_item_of_type(args, FastDataframe)
                temp_args = filter_type(args, FastDataframe)
                temp_args.append(fastdataframe_item.set_is_nullable_from_type(args[0]))
                new_class_dict["__annotations__"][field_name] = Annotated[tuple(temp_args)]
            else:
                new_class_dict["__annotations__"][field_name] = Annotated[args + (FastDataframe.from_field_type(args[0]),)]            # else:

        return super().__new__(mcs, name, bases, new_class_dict, **kwargs)



class FastDataframeModel(BaseModel, metaclass=FastDataframeModelMetaclass):
    """Base model that enforces FastDataframe annotation on all fields."""

    # model_config = ConfigDict(arbitrary_types_allowed=True)

    # @classmethod
    # def model_json_schema(cls, **kwargs: Any) -> Dict[str, Any]:
    #     """Generate JSON schema with FastDataframe metadata."""
    #     schema = super().model_json_schema(**kwargs)

    #     # Add FastDataframe metadata to each property
    #     for field_name, field in cls.model_fields.items():
    #         if isinstance(field.json_schema_extra, dict) and "_fastdataframe" in field.json_schema_extra:
    #             schema["properties"][field_name]["_fastdataframe"] = field.json_schema_extra["_fastdataframe"]

    #     return schema
