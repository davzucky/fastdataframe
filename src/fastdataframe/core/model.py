"""FastDataframe model implementation."""

from typing import Literal, Type, TypeVar

from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo

from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)

from .annotation import ColumnInfo

T = TypeVar("T", bound="FastDataframeModel")
TBaseModel = TypeVar("TBaseModel", bound=BaseModel)
AliasType = Literal["serialization", "validation"]


def _get_column_info(field_info: FieldInfo) -> ColumnInfo:
    for m in field_info.metadata:
        if isinstance(m, ColumnInfo):
            return m
    return ColumnInfo.from_field_type(field_info)


class FastDataframeModel(BaseModel):
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
            assert "id" in PolarsUser.model_fields
            assert "name" in PolarsUser.model_fields
            assert "age" in PolarsUser.model_fields
            assert "is_active" in PolarsUser.model_fields

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
    def model_columns(
        cls, alias_type: AliasType = "serialization"
    ) -> dict[str, ColumnInfo]:
        """Extract column information from the model's fields with alias support.
        
        This method returns a dictionary mapping column names (using the specified alias type)
        to their corresponding ColumnInfo objects. It processes all fields in the model and
        extracts their metadata, including any FastDataframe-specific annotations like
        uniqueness constraints, boolean string mappings, and date formats.
        
        The method supports two alias types:
        - "serialization": Uses serialization aliases (default names for storage/export)
        - "validation": Uses validation aliases (names used during data validation)
        
        This is useful for:
        - Understanding the schema of a model's columns
        - Extracting metadata for dataframe operations
        - Validating column configurations
        - Building schema-aware data processing pipelines
        
        Args:
            alias_type: The type of alias to use for column names.
                - "serialization": Use serialization aliases (default)
                - "validation": Use validation aliases
                
        Returns:
            A dictionary mapping column names (using the specified alias) to ColumnInfo objects.
            Each ColumnInfo contains metadata about the column including type information,
            uniqueness constraints, and any custom FastDataframe annotations.
            
        Example:
            ```python
            from fastdataframe import FastDataframeModel, ColumnInfo
            from typing import Optional, Annotated
            
            class UserModel(FastDataframeModel):
                user_id: Annotated[int, ColumnInfo(is_unique=True)]
                name: str
                age: Optional[int] = None
                is_active: Annotated[bool, ColumnInfo(
                    bool_true_string="1", 
                    bool_false_string="0"
                )]
                birth_date: Annotated[str, ColumnInfo(date_format="%Y-%m-%d")]
            
            # Get column information with serialization aliases
            columns = UserModel.model_columns(alias_type="serialization")
            
            # The result contains column names and their metadata
            assert "user_id" in columns
            assert columns["user_id"].is_unique is True
            assert columns["is_active"].bool_true_string == "1"
            assert columns["is_active"].bool_false_string == "0"
            assert columns["birth_date"].date_format == "%Y-%m-%d"
            
            # Get column information with validation aliases (if different)
            validation_columns = UserModel.model_columns(alias_type="validation")
            
            # Use the column information for dataframe operations
            for column_name, column_info in columns.items():
                print(f"Column: {column_name}")
                print(f"  Is unique: {column_info.is_unique}")
                print(f"  Date format: {column_info.date_format}")
            ```
            
        Notes:
            - Column names are determined by the alias type specified
            - Fields without explicit ColumnInfo annotations get default ColumnInfo objects
            - The method processes all model fields, including inherited ones
            - ColumnInfo objects contain metadata useful for dataframe operations
            - This method is commonly used by dataframe-specific subclasses (Polars, Iceberg)
            - The returned dictionary preserves the order of fields in the model
        """
        
        columns = {}
        alias_func = (
            get_serialization_alias
            if alias_type == "serialization"
            else get_validation_alias
        )
        for field_name, field_info in cls.model_fields.items():
            col_info = _get_column_info(field_info)
            columns[alias_func(field_info, field_name)] = col_info
        return columns
