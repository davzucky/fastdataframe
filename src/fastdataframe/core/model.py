"""FastDataframe model implementation."""

from typing import Any, Literal, TypeVar, Annotated, get_args, get_origin

from pydantic import BaseModel
from pydantic._internal._model_construction import (
    ModelMetaclass as PydanticModelMetaclass,
)

from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)

from .annotation import ColumnInfo
from .types_helper import contains_type, get_item_of_type

T = TypeVar("T", bound="FastDataframeModel")
AliasType = Literal["serialization", "validation"]


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
    def get_column_infos(
        cls, alias_type: AliasType = "serialization"
    ) -> dict[str, ColumnInfo]:
        """Return a dictionary mapping field_name to ColumnInfo objects from the model_json_schema."""
        fastdataframes = {}
        alias_func = (
            get_serialization_alias
            if alias_type == "serialization"
            else get_validation_alias
        )
        for field_name, field_type in cls.__annotations__.items():
            origin = get_origin(field_type)
            args = get_args(field_type)
            alias_name = alias_func(cls.__pydantic_fields__[field_name], field_name)
            if origin is not Annotated:
                fastdataframes[alias_name] = ColumnInfo.from_field_type(field_type)
            elif origin is Annotated:
                col_info = get_item_of_type(args, ColumnInfo)
                fastdataframes[alias_name] = (
                    col_info
                    if col_info is not None
                    else ColumnInfo.from_field_type(args[0])
                )
        return fastdataframes
