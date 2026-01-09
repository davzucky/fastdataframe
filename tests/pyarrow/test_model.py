"""Tests for PyArrowFastDataframeModel."""

import datetime as dt
from typing import Annotated, Optional

import pyarrow as pa
import pytest
from pydantic import BaseModel, Field

from fastdataframe.pyarrow.model import PyArrowFastDataframeModel


class TestGetPyArrowSchema:
    """Tests for get_pyarrow_schema method."""

    def test_get_pyarrow_schema_with_simple_types(self) -> None:
        """Test schema generation with basic Python types."""

        class TestModel(PyArrowFastDataframeModel):
            a: int
            b: str

        schema = TestModel.get_pyarrow_schema()

        assert len(schema) == 2
        assert schema.field("a").type == pa.int64()
        assert schema.field("b").type == pa.string()
        # Required fields should have nullable=False
        assert schema.field("a").nullable is False
        assert schema.field("b").nullable is False

    def test_get_pyarrow_schema_with_all_basic_types(self) -> None:
        """Test schema generation with all basic Python types."""

        class TestModel(PyArrowFastDataframeModel):
            int_field: int
            str_field: str
            float_field: float
            bool_field: bool
            bytes_field: bytes

        schema = TestModel.get_pyarrow_schema()

        assert schema.field("int_field").type == pa.int64()
        assert schema.field("str_field").type == pa.string()
        assert schema.field("float_field").type == pa.float64()
        assert schema.field("bool_field").type == pa.bool_()
        assert schema.field("bytes_field").type == pa.binary()

    def test_get_pyarrow_schema_with_temporal_types(self) -> None:
        """Test schema generation with temporal types."""

        class TestModel(PyArrowFastDataframeModel):
            date_field: dt.date
            datetime_field: dt.datetime
            time_field: dt.time
            timedelta_field: dt.timedelta

        schema = TestModel.get_pyarrow_schema()

        assert schema.field("date_field").type == pa.date32()
        assert schema.field("datetime_field").type == pa.timestamp("us")
        assert schema.field("time_field").type == pa.time64("us")
        assert schema.field("timedelta_field").type == pa.duration("us")

    def test_get_pyarrow_schema_with_optional_types(self) -> None:
        """Test schema generation with optional types."""

        class TestModel(PyArrowFastDataframeModel):
            required_field: int
            optional_field: Optional[str] = None

        schema = TestModel.get_pyarrow_schema()

        assert schema.field("required_field").nullable is False
        assert schema.field("optional_field").nullable is True
        assert schema.field("optional_field").type == pa.string()

    def test_get_pyarrow_schema_with_alias_serialization(self) -> None:
        """Test schema generation with serialization aliases."""

        class TestModel(PyArrowFastDataframeModel):
            a_alias: Annotated[str, Field(alias="aAlias")]
            a_alias_serialize: Annotated[
                str, Field(serialization_alias="aliasSerialize")
            ]
            a_alias_validate: Annotated[str, Field(validation_alias="aliasValidate")]

        schema = TestModel.get_pyarrow_schema("serialization")

        field_names = [f.name for f in schema]
        assert "aAlias" in field_names
        assert "aliasSerialize" in field_names
        assert "a_alias_validate" in field_names

    def test_get_pyarrow_schema_with_alias_validation(self) -> None:
        """Test schema generation with validation aliases."""

        class TestModel(PyArrowFastDataframeModel):
            a_alias: Annotated[str, Field(alias="aAlias")]
            a_alias_serialize: Annotated[
                str, Field(serialization_alias="aliasSerialize")
            ]
            a_alias_validate: Annotated[str, Field(validation_alias="aliasValidate")]

        schema = TestModel.get_pyarrow_schema("validation")

        field_names = [f.name for f in schema]
        assert "aAlias" in field_names
        assert "a_alias_serialize" in field_names
        assert "aliasValidate" in field_names

    def test_get_pyarrow_schema_with_empty_model(self) -> None:
        """Test schema generation with an empty model."""

        class EmptyModel(PyArrowFastDataframeModel):
            pass

        schema = EmptyModel.get_pyarrow_schema()
        assert len(schema) == 0


class TestGetStringifiedSchema:
    """Tests for get_stringified_schema method."""

    def test_get_stringified_schema_with_simple_types(self) -> None:
        """Test that all field types are converted to pa.string()."""

        class TestModel(PyArrowFastDataframeModel):
            a: int
            b: str
            c: float
            d: bool

        schema = TestModel.get_stringified_schema()

        for field in schema:
            assert field.type == pa.string()

    def test_get_stringified_schema_preserves_nullability(self) -> None:
        """Test that stringified schema preserves nullability."""

        class TestModel(PyArrowFastDataframeModel):
            required: int
            optional: Optional[str] = None

        schema = TestModel.get_stringified_schema()

        assert schema.field("required").nullable is False
        assert schema.field("optional").nullable is True

    def test_get_stringified_schema_vs_regular_schema(self) -> None:
        """Test that stringified schema differs from regular schema."""

        class TestModel(PyArrowFastDataframeModel):
            id: int
            name: str
            value: float

        regular_schema = TestModel.get_pyarrow_schema()
        stringified_schema = TestModel.get_stringified_schema()

        # Field names should be the same
        assert [f.name for f in regular_schema] == [f.name for f in stringified_schema]

        # Types should be different
        assert regular_schema.field("id").type == pa.int64()
        assert stringified_schema.field("id").type == pa.string()

        assert regular_schema.field("value").type == pa.float64()
        assert stringified_schema.field("value").type == pa.string()


class TestCollectionTypes:
    """Tests for collection type support (list, tuple, set)."""

    def test_list_type_schema_generation(self) -> None:
        """Test that list[T] types generate correct pa.list_() schemas."""

        class TestModel(PyArrowFastDataframeModel):
            int_list: list[int]
            str_list: list[str]
            float_list: list[float]

        schema = TestModel.get_pyarrow_schema()

        assert schema.field("int_list").type == pa.list_(pa.int64())
        assert schema.field("str_list").type == pa.list_(pa.string())
        assert schema.field("float_list").type == pa.list_(pa.float64())

    def test_tuple_variable_length_schema_generation(self) -> None:
        """Test that tuple[T, ...] types generate correct pa.list_() schemas."""

        class TestModel(PyArrowFastDataframeModel):
            int_tuple: tuple[int, ...]
            str_tuple: tuple[str, ...]
            bool_tuple: tuple[bool, ...]

        schema = TestModel.get_pyarrow_schema()

        assert schema.field("int_tuple").type == pa.list_(pa.int64())
        assert schema.field("str_tuple").type == pa.list_(pa.string())
        assert schema.field("bool_tuple").type == pa.list_(pa.bool_())

    def test_set_type_schema_generation(self) -> None:
        """Test that set[T] types generate correct pa.list_() schemas."""

        class TestModel(PyArrowFastDataframeModel):
            int_set: set[int]
            str_set: set[str]
            float_set: set[float]

        schema = TestModel.get_pyarrow_schema()

        assert schema.field("int_set").type == pa.list_(pa.int64())
        assert schema.field("str_set").type == pa.list_(pa.string())
        assert schema.field("float_set").type == pa.list_(pa.float64())

    def test_nested_collection_types(self) -> None:
        """Test nested collection types like list[list[int]]."""

        class TestModel(PyArrowFastDataframeModel):
            matrix: list[list[int]]

        schema = TestModel.get_pyarrow_schema()

        # list[list[int]] should be pa.list_(pa.list_(pa.int64()))
        expected_type = pa.list_(pa.list_(pa.int64()))
        assert schema.field("matrix").type == expected_type

    @pytest.mark.parametrize(
        "python_type,expected_pyarrow_type",
        [
            (list[int], pa.list_(pa.int64())),
            (list[str], pa.list_(pa.string())),
            (list[float], pa.list_(pa.float64())),
            (list[bool], pa.list_(pa.bool_())),
            (tuple[int, ...], pa.list_(pa.int64())),
            (tuple[str, ...], pa.list_(pa.string())),
            (set[int], pa.list_(pa.int64())),
            (set[str], pa.list_(pa.string())),
            (list[list[int]], pa.list_(pa.list_(pa.int64()))),
        ],
    )
    def test_collection_type_mapping_parametrized(
        self, python_type, expected_pyarrow_type
    ) -> None:
        """Parametrized test for various collection type mappings."""
        from pydantic.fields import FieldInfo

        from fastdataframe.pyarrow._types import get_pyarrow_type

        field_info = FieldInfo(annotation=python_type)
        result = get_pyarrow_type(field_info)
        assert result == expected_pyarrow_type


class TestBaseModelTypes:
    """Tests for Pydantic BaseModel type support."""

    def test_simple_basemodel_schema_generation(self) -> None:
        """Test that BaseModel fields generate correct pa.struct() schemas."""

        class Address(BaseModel):
            street: str
            city: str
            zip_code: int

        class TestModel(PyArrowFastDataframeModel):
            name: str
            address: Address

        schema = TestModel.get_pyarrow_schema()

        # Check that address field is a struct
        address_type = schema.field("address").type
        assert pa.types.is_struct(address_type)

        # Check struct fields
        assert address_type.field("street").type == pa.string()
        assert address_type.field("city").type == pa.string()
        assert address_type.field("zip_code").type == pa.int64()

    def test_nested_basemodel_schema_generation(self) -> None:
        """Test nested BaseModel structures."""

        class ContactInfo(BaseModel):
            email: str
            phone: str

        class Address(BaseModel):
            street: str
            city: str
            contact: ContactInfo

        class TestModel(PyArrowFastDataframeModel):
            name: str
            address: Address

        schema = TestModel.get_pyarrow_schema()

        # Check outer struct
        address_type = schema.field("address").type
        assert pa.types.is_struct(address_type)

        # Find the contact field within the address struct
        contact_type = address_type.field("contact").type
        assert pa.types.is_struct(contact_type)

        # Check nested struct fields
        assert contact_type.field("email").type == pa.string()
        assert contact_type.field("phone").type == pa.string()

    def test_optional_basemodel_schema_generation(self) -> None:
        """Test optional BaseModel fields."""

        class Address(BaseModel):
            street: str
            city: str

        class TestModel(PyArrowFastDataframeModel):
            name: str
            address: Optional[Address] = None

        schema = TestModel.get_pyarrow_schema()

        # Check that optional address is still a struct but nullable
        address_field = schema.field("address")
        assert pa.types.is_struct(address_field.type)
        assert address_field.nullable is True

        # Check struct fields are correct
        address_type = address_field.type
        assert address_type.field("street").type == pa.string()
        assert address_type.field("city").type == pa.string()

    def test_basemodel_with_collections(self) -> None:
        """Test BaseModel fields combined with collection types."""

        class Tag(BaseModel):
            name: str
            priority: int

        class TestModel(PyArrowFastDataframeModel):
            title: str
            tags: list[Tag]  # List of BaseModel objects

        schema = TestModel.get_pyarrow_schema()

        # Check that tags is a list of struct
        tags_type = schema.field("tags").type
        assert pa.types.is_list(tags_type)
        assert pa.types.is_struct(tags_type.value_type)

        # Check the struct fields inside the list
        tag_struct = tags_type.value_type
        assert tag_struct.field("name").type == pa.string()
        assert tag_struct.field("priority").type == pa.int64()

    def test_basemodel_with_field_aliases(self) -> None:
        """Test BaseModel with Pydantic field aliases."""

        class Address(BaseModel):
            street_name: Annotated[str, Field(alias="street")]
            city_name: Annotated[str, Field(alias="city")]

        class TestModel(PyArrowFastDataframeModel):
            name: str
            address: Address

        # Test serialization alias schema
        schema = TestModel.get_pyarrow_schema("serialization")
        address_type = schema.field("address").type

        # Should use the aliases
        field_names = [f.name for f in address_type]
        assert "street" in field_names
        assert "city" in field_names

    def test_empty_basemodel(self) -> None:
        """Test BaseModel with no fields."""

        class EmptyModel(BaseModel):
            pass

        class TestModel(PyArrowFastDataframeModel):
            name: str
            empty: EmptyModel

        schema = TestModel.get_pyarrow_schema()
        empty_type = schema.field("empty").type
        assert pa.types.is_struct(empty_type)
        assert empty_type.num_fields == 0

    @pytest.mark.parametrize(
        "basemodel_structure",
        [
            "simple",
            "nested_2_levels",
            "nested_3_levels",
        ],
    )
    def test_basemodel_type_mapping_parametrized(self, basemodel_structure) -> None:
        """Parametrized test for various BaseModel structures."""
        from pydantic.fields import FieldInfo

        from fastdataframe.pyarrow._types import get_pyarrow_type

        if basemodel_structure == "simple":

            class TestModel(BaseModel):
                name: str
                value: int

            field_info = FieldInfo(annotation=TestModel)
            result = get_pyarrow_type(field_info)

            assert pa.types.is_struct(result)
            assert result.field("name").type == pa.string()
            assert result.field("value").type == pa.int64()

        elif basemodel_structure == "nested_2_levels":

            class Inner(BaseModel):
                data: str

            class Outer(BaseModel):
                name: str
                inner: Inner

            field_info = FieldInfo(annotation=Outer)
            result = get_pyarrow_type(field_info)

            assert pa.types.is_struct(result)
            assert result.field("name").type == pa.string()
            assert pa.types.is_struct(result.field("inner").type)
            assert result.field("inner").type.field("data").type == pa.string()

        elif basemodel_structure == "nested_3_levels":

            class Deep(BaseModel):
                value: int

            class Middle(BaseModel):
                name: str
                deep: Deep

            class Top(BaseModel):
                title: str
                middle: Middle

            field_info = FieldInfo(annotation=Top)
            result = get_pyarrow_type(field_info)

            assert pa.types.is_struct(result)
            assert result.field("title").type == pa.string()

            middle_type = result.field("middle").type
            assert pa.types.is_struct(middle_type)
            assert middle_type.field("name").type == pa.string()

            deep_type = middle_type.field("deep").type
            assert pa.types.is_struct(deep_type)
            assert deep_type.field("value").type == pa.int64()


class TestFromBaseModel:
    """Tests for from_base_model class method."""

    def test_from_base_model_basic_conversion(self) -> None:
        """Test converting a BaseModel to PyArrowFastDataframeModel."""

        class UserModel(BaseModel):
            id: int
            name: str
            is_active: bool

        PyArrowModel = PyArrowFastDataframeModel.from_base_model(UserModel)

        assert issubclass(PyArrowModel, PyArrowFastDataframeModel)
        assert PyArrowModel.__name__ == "UserModelPyArrow"
        assert PyArrowModel.model_fields.keys() == UserModel.model_fields.keys()

        # Verify the schema can be generated
        schema = PyArrowModel.get_pyarrow_schema()
        assert len(schema) == 3
        assert schema.field("id").type == pa.int64()
        assert schema.field("name").type == pa.string()
        assert schema.field("is_active").type == pa.bool_()


class TestSchemaUsageWithPyArrow:
    """Tests verifying the generated schema works with PyArrow."""

    def test_create_table_with_schema(self) -> None:
        """Test creating a PyArrow table using the generated schema."""

        class TestModel(PyArrowFastDataframeModel):
            id: int
            name: str
            is_active: bool

        schema = TestModel.get_pyarrow_schema()

        # Create a table using the schema
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "is_active": [True, False, True],
            },
            schema=schema,
        )

        assert table.schema == schema
        assert table.num_rows == 3

    def test_create_table_with_optional_fields(self) -> None:
        """Test creating a PyArrow table with optional fields."""

        class TestModel(PyArrowFastDataframeModel):
            id: int
            description: Optional[str] = None

        schema = TestModel.get_pyarrow_schema()

        # Create a table with null values
        table = pa.table(
            {
                "id": [1, 2, 3],
                "description": ["first", None, "third"],
            },
            schema=schema,
        )

        assert table.schema == schema
        assert table.column("description").null_count == 1

    def test_create_table_with_list_fields(self) -> None:
        """Test creating a PyArrow table with list fields."""

        class TestModel(PyArrowFastDataframeModel):
            id: int
            tags: list[str]

        schema = TestModel.get_pyarrow_schema()

        # Create a table with list data
        table = pa.table(
            {
                "id": [1, 2],
                "tags": [["a", "b"], ["c", "d", "e"]],
            },
            schema=schema,
        )

        assert table.schema == schema
        assert pa.types.is_list(table.schema.field("tags").type)

    def test_create_table_with_struct_fields(self) -> None:
        """Test creating a PyArrow table with struct fields."""

        class Address(BaseModel):
            street: str
            city: str

        class TestModel(PyArrowFastDataframeModel):
            id: int
            address: Address

        schema = TestModel.get_pyarrow_schema()

        # Create a table with struct data
        table = pa.table(
            {
                "id": [1, 2],
                "address": [
                    {"street": "123 Main St", "city": "NYC"},
                    {"street": "456 Oak Ave", "city": "LA"},
                ],
            },
            schema=schema,
        )

        assert table.schema == schema
        assert pa.types.is_struct(table.schema.field("address").type)
