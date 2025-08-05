"""FastDataframe model implementation."""

from typing import Any, Literal, Type, TypeVar, Annotated, get_args, get_origin

from pydantic import BaseModel, create_model
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
TBaseModel = TypeVar("TBaseModel", bound=BaseModel)
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
    def from_base_model(cls: Type[T], model: type[TBaseModel]) -> type[T]:
        """Convert a Pydantic BaseModel to a FastDataframeModel subclass.
        
        This method creates a new FastDataframeModel class that inherits from the calling class
        and includes all the fields from the provided Pydantic model. This is useful for creating
        dataframe-specific versions of existing Pydantic models while preserving their schema
        and validation rules.
        
        The method extracts field definitions from the source model and creates a new model
        using Pydantic's `create_model` function. The new model will have the same field types,
        validation rules, and metadata as the original model, but will also inherit the
        FastDataframe-specific functionality from the calling class.
        
        Args:
            cls: The FastDataframeModel subclass to inherit from (e.g., PolarsFastDataframeModel)
            model: A Pydantic BaseModel class to convert from
            
        Returns:
            A new FastDataframeModel subclass that combines the schema of the input model
            with the functionality of the calling class.
            
        Example:
            ```python
            from pydantic import BaseModel
            from fastdataframe.polars.model import PolarsFastDataframeModel
            
            # Define a base Pydantic model
            class User(BaseModel):
                id: int
                name: str
                age: int
                is_active: bool = True
                
            # Convert to a Polars-compatible model
            PolarsUser = PolarsFastDataframeModel.from_base_model(User)
            
            # The new model has all the original fields plus Polars functionality
            assert issubclass(PolarsUser, PolarsFastDataframeModel)
            assert PolarsUser.__name__ == "UserPolars"
            assert "id" in PolarsUser.__annotations__
            assert "name" in PolarsUser.__annotations__
            assert "age" in PolarsUser.__annotations__
            assert "is_active" in PolarsUser.__annotations__
            
            # You can now use it with Polars dataframes
            import polars as pl
            df = pl.DataFrame({
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "is_active": [True, False, True]
            })
            
            # Validate the dataframe against the model
            errors = PolarsUser.validate_schema(df)
            if not errors:
                print("DataFrame is valid!")
            ```
            
        Notes:
            - The new model's name will be "{original_model_name}{base_class_suffix}"
              where the suffix is derived from the calling class name (e.g., "Polars" for PolarsFastDataframeModel)
            - All field types, validation rules, and metadata from the original model are preserved
            - The new model inherits all FastDataframe-specific methods from the calling class
            - This method is commonly used to create dataframe-specific versions of existing Pydantic models
            - The generated model maintains the same JSON schema as the original model
        """
        
        field_definitions = {
            field_name: (field_type.annotation, field_type)
            for field_name, field_type in model.model_fields.items()
        }
        base_model_name = cls.__name__[: -len("FastDataframeModel")]
        new_model: type[T] = create_model(
            f"{model.__name__}{base_model_name}",
            __base__=cls,
            __doc__=f"{base_model_name} version of {model.__name__}",
            **field_definitions,
        )  # type: ignore[call-overload]
        return new_model

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
