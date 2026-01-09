"""PyArrowFastDataframeModel implementation."""

import pyarrow as pa

from fastdataframe.core.model import AliasType, FastDataframeModel
from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)
from fastdataframe.core.types_helper import is_optional_type
from fastdataframe.pyarrow._types import get_pyarrow_type


class PyArrowFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for PyArrow integration."""

    @classmethod
    def get_pyarrow_schema(cls, alias_type: AliasType = "serialization") -> pa.Schema:
        """Get the PyArrow schema for the model.

        This method generates a PyArrow schema based on the model's field definitions,
        including proper type mappings and nullability information. The schema can be
        used for creating Arrow tables, reading/writing Parquet files, and integrating
        with other Arrow-based systems.

        Args:
            alias_type: The alias type to use for field names.
                - 'serialization' (default): Use serialization aliases for field names
                - 'validation': Use validation aliases for field names

        Returns:
            pa.Schema: A PyArrow Schema object representing the model's structure.

        Example:
            ```python
            from fastdataframe.pyarrow.model import PyArrowFastDataframeModel
            from typing import Optional

            class UserModel(PyArrowFastDataframeModel):
                id: int
                name: str
                email: Optional[str] = None
                is_active: bool

            # Get the schema
            schema = UserModel.get_pyarrow_schema()

            # Use with PyArrow
            import pyarrow as pa
            table = pa.table({
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "email": ["alice@example.com", None, "charlie@example.com"],
                "is_active": [True, True, False]
            }, schema=schema)
            ```

        Notes:
            - Required fields (non-Optional) have nullable=False in the schema
            - Optional fields have nullable=True in the schema
            - Collection types (list, set, tuple) are mapped to pa.list_()
            - Pydantic BaseModel fields are mapped to pa.struct()
            - The schema preserves field order as defined in the model
        """
        alias_func = (
            get_serialization_alias
            if alias_type == "serialization"
            else get_validation_alias
        )

        fields = []
        for field_name, field_info in cls.model_fields.items():
            field_alias = alias_func(field_info, field_name)
            pa_type = get_pyarrow_type(field_info, alias_type)
            nullable = is_optional_type(field_info.annotation)
            fields.append(pa.field(field_alias, pa_type, nullable=nullable))

        return pa.schema(fields)

    @classmethod
    def get_stringified_schema(
        cls, alias_type: AliasType = "serialization"
    ) -> pa.Schema:
        """Get the PyArrow schema for the model with all columns as strings.

        This is useful when reading data from sources where all values are strings
        (like CSV files) and need to be cast to the proper types later.

        Args:
            alias_type: The alias type to use for field names.
                - 'serialization' (default): Use serialization aliases for field names
                - 'validation': Use validation aliases for field names

        Returns:
            pa.Schema: A PyArrow Schema with all fields as pa.string() type.

        Example:
            ```python
            class UserModel(PyArrowFastDataframeModel):
                id: int
                name: str
                score: float

            # Get stringified schema for reading CSV
            string_schema = UserModel.get_stringified_schema()
            # All fields will be pa.string()
            ```
        """
        alias_func = (
            get_serialization_alias
            if alias_type == "serialization"
            else get_validation_alias
        )

        fields = []
        for field_name, field_info in cls.model_fields.items():
            field_alias = alias_func(field_info, field_name)
            nullable = is_optional_type(field_info.annotation)
            fields.append(pa.field(field_alias, pa.string(), nullable=nullable))

        return pa.schema(fields)
