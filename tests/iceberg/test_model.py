import pytest
from pyiceberg.types import (
    IntegerType,
    StringType,
    BooleanType,
    DateType,
    DoubleType,
    BinaryType,
    UUIDType,
    TimestampType,
    IcebergType,
    ListType,
    MapType,
    StructType,
)
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField
from pyiceberg.table import Table
from fastdataframe.iceberg.model import IcebergFastDataframeModel
import datetime
import uuid
import typing
from pydantic import BaseModel


class TestIcebergFastDataframeModel:
    @pytest.mark.parametrize(
        "field_type,expected_iceberg_type,expected_required",
        [
            (int, IntegerType, True),
            (typing.Optional[int], IntegerType, False),
            (str, StringType, True),
            (typing.Optional[str], StringType, False),
            (bool, BooleanType, True),
            (typing.Optional[bool], BooleanType, False),
            (datetime.date, DateType, True),
            (typing.Optional[datetime.date], DateType, False),
            (float, DoubleType, True),
            (typing.Optional[float], DoubleType, False),
            (bytes, BinaryType, True),
            (typing.Optional[bytes], BinaryType, False),
            (uuid.UUID, UUIDType, True),
            (typing.Optional[uuid.UUID], UUIDType, False),
            (datetime.datetime, TimestampType, True),
            (typing.Optional[datetime.datetime], TimestampType, False),
        ],
    )
    def test_to_iceberg_schema_primitives(
        self,
        field_type: typing.Any,
        expected_iceberg_type: typing.Type[IcebergType],
        expected_required: bool,
    ) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: field_type

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, expected_iceberg_type)
        assert field.required is expected_required

    @pytest.mark.parametrize(
        "field_type,expected_iceberg_type",
        [
            (
                list[str],
                ListType(
                    element_id=1, element_type=StringType(), element_required=True
                ),
            ),
            (
                set[str],
                ListType(
                    element_id=1, element_type=StringType(), element_required=True
                ),
            ),
            (
                tuple[str],
                ListType(
                    element_id=1, element_type=StringType(), element_required=True
                ),
            ),
            (
                dict[str, int],
                MapType(
                    key_id=1,
                    key_type=StringType(),
                    value_id=1,
                    value_type=IntegerType(),
                    value_required=True,
                ),
            ),
            (
                dict[str, str],
                MapType(
                    key_id=1,
                    key_type=StringType(),
                    value_id=1,
                    value_type=StringType(),
                    value_required=True,
                ),
            ),
        ],
    )
    def test_to_iceberg_schema_complex_type(
        self, field_type: typing.Any, expected_iceberg_type: typing.Type[IcebergType]
    ) -> None:
        # Dynamically create a model class with a single field
        class DynamicModel(IcebergFastDataframeModel):
            field_name: field_type

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert field.field_type == expected_iceberg_type
        assert field.required is True

    @pytest.mark.parametrize(
        "field_type,element_type,element_required,field_required",
        [
            (typing.Optional[list[int]], IntegerType, True, False),
            (typing.Optional[set[int]], IntegerType, True, False),
            (typing.Optional[tuple[int]], IntegerType, True, False),
        ],
    )
    def test_to_iceberg_schema_optional_container_field(
        self,
        field_type: typing.Any,
        element_type: type,
        element_required: bool,
        field_required: bool,
    ) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: field_type

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, ListType)
        assert isinstance(field.field_type.element_type, element_type)
        assert field.field_type.element_required is element_required
        assert field.required is field_required

    @pytest.mark.parametrize(
        "field_type,key_type,value_type,value_required,field_required",
        [
            (typing.Optional[dict[str, int]], StringType, IntegerType, True, False),
            (typing.Optional[dict[int, str]], IntegerType, StringType, True, False),
        ],
    )
    def test_to_iceberg_schema_optional_map_field(
        self,
        field_type: typing.Any,
        key_type: type,
        value_type: type,
        value_required: bool,
        field_required: bool,
    ) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: field_type

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, MapType)
        assert isinstance(field.field_type.key_type, key_type)
        assert isinstance(field.field_type.value_type, value_type)
        assert field.field_type.value_required is value_required
        assert field.required is field_required

    @pytest.mark.parametrize(
        "field_type,element_type,element_required,field_required",
        [
            (list[typing.Optional[int]], IntegerType, False, True),
            (set[typing.Optional[int]], IntegerType, False, True),
            (tuple[typing.Optional[int]], IntegerType, False, True),
        ],
    )
    def test_to_iceberg_schema_container_of_optional_elements(
        self,
        field_type: typing.Any,
        element_type: type,
        element_required: bool,
        field_required: bool,
    ) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: field_type

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, ListType)
        assert isinstance(field.field_type.element_type, element_type)
        assert field.field_type.element_required is element_required
        assert field.required is field_required

    @pytest.mark.parametrize(
        "field_type,key_type,value_type,value_required,field_required",
        [
            (dict[str, typing.Optional[int]], StringType, IntegerType, False, True),
            (dict[int, typing.Optional[str]], IntegerType, StringType, False, True),
        ],
    )
    def test_to_iceberg_schema_map_with_optional_values(
        self,
        field_type: typing.Any,
        key_type: type,
        value_type: type,
        value_required: bool,
        field_required: bool,
    ) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: field_type

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, MapType)
        assert isinstance(field.field_type.key_type, key_type)
        assert isinstance(field.field_type.value_type, value_type)
        assert field.field_type.value_required is value_required
        assert field.required is field_required

    @pytest.mark.parametrize(
        "ann",
        [
            list[int | str],
            list[typing.Union[int, str]],
            set[int | float],
            tuple[int | bytes],
            dict[int | str, int],  # Union key types
            dict[str, int | str],  # Union value types
            dict[typing.Union[int, str], int],  # Union key types with explicit Union
            dict[str, typing.Union[int, str]],  # Union value types with explicit Union
        ],
    )
    def test_container_with_multi_union_element_raises(self, ann: typing.Any) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: ann

        with pytest.raises(ValueError):
            DynamicModel.iceberg_schema()

    def test_container_with_optional_element_ok(self) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: list[typing.Optional[int]]

        # Should not raise
        DynamicModel.iceberg_schema()

    def test_map_with_optional_values_ok(self) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: dict[str, typing.Optional[int]]

        # Should not raise
        DynamicModel.iceberg_schema()

    class TestIcebergValidation:
        @pytest.fixture
        def model(self) -> type[IcebergFastDataframeModel]:
            class TestModel(IcebergFastDataframeModel):
                field1: int
                field2: str

            return TestModel

        class DummyTable(Table):
            """A minimal in-memory Table implementation for testing."""

            def __init__(self, columns: list[str]) -> None:
                self._schema = Schema(
                    *[
                        NestedField(
                            field_id=i + 1,
                            name=col,
                            field_type=IntegerType()
                            if col == "field1"
                            else StringType(),
                            required=True,
                        )
                        for i, col in enumerate(columns)
                    ]
                )

            def schema(self) -> Schema:
                return self._schema

        def test_validate_table_all_columns_present(
            self, model: type[IcebergFastDataframeModel]
        ) -> None:
            table = self.DummyTable(["field1", "field2"])
            errors = model.validate_schema(table)
            assert errors == []

        def test_validate_table_missing_column(
            self, model: type[IcebergFastDataframeModel]
        ) -> None:
            table = self.DummyTable(["field1"])
            errors = model.validate_schema(table)
            assert len(errors) == 1
            assert errors[0].column_name == "field2"
            assert errors[0].error_type == "MissingColumn"
            assert "missing" in errors[0].error_details.lower()


class TestBaseModelSupport:
    """Test cases for Pydantic BaseModel support in Iceberg schema generation."""

    def test_simple_basemodel_field(self) -> None:
        """Test that a simple BaseModel field maps to StructType."""

        class NestedModel(BaseModel):
            nested_field: str
            nested_int: int

        class MainModel(IcebergFastDataframeModel):
            nested: NestedModel

        schema = MainModel.iceberg_schema()
        assert len(schema.fields) == 1

        field = schema.fields[0]
        assert field.name == "nested"
        assert isinstance(field.field_type, StructType)
        assert field.required is True

        # Check nested fields
        struct_fields = field.field_type.fields
        assert len(struct_fields) == 2

        nested_field = next(f for f in struct_fields if f.name == "nested_field")
        assert isinstance(nested_field.field_type, StringType)
        assert nested_field.required is True

        nested_int = next(f for f in struct_fields if f.name == "nested_int")
        assert isinstance(nested_int.field_type, IntegerType)
        assert nested_int.required is True

    def test_optional_basemodel_field(self) -> None:
        """Test that an Optional BaseModel field maps to nullable StructType."""

        class NestedModel(BaseModel):
            name: str

        class MainModel(IcebergFastDataframeModel):
            nested: typing.Optional[NestedModel]

        schema = MainModel.iceberg_schema()
        field = schema.fields[0]
        assert field.name == "nested"
        assert isinstance(field.field_type, StructType)
        assert field.required is False

    def test_nested_basemodel_fields(self) -> None:
        """Test deeply nested BaseModel structures."""

        class Level3Model(BaseModel):
            level3_field: str

        class Level2Model(BaseModel):
            level2_field: int
            level3: Level3Model

        class Level1Model(BaseModel):
            level1_field: bool
            level2: Level2Model

        class MainModel(IcebergFastDataframeModel):
            root: Level1Model

        schema = MainModel.iceberg_schema()
        field = schema.fields[0]
        assert isinstance(field.field_type, StructType)

        # Check level 1 structure
        level1_fields = field.field_type.fields
        assert len(level1_fields) == 2

        level1_bool = next(f for f in level1_fields if f.name == "level1_field")
        assert isinstance(level1_bool.field_type, BooleanType)

        level2_field = next(f for f in level1_fields if f.name == "level2")
        assert isinstance(level2_field.field_type, StructType)

        # Check level 2 structure
        level2_fields = level2_field.field_type.fields
        assert len(level2_fields) == 2

        level2_int = next(f for f in level2_fields if f.name == "level2_field")
        assert isinstance(level2_int.field_type, IntegerType)

        level3_field = next(f for f in level2_fields if f.name == "level3")
        assert isinstance(level3_field.field_type, StructType)

        # Check level 3 structure
        level3_fields = level3_field.field_type.fields
        assert len(level3_fields) == 1
        assert level3_fields[0].name == "level3_field"
        assert isinstance(level3_fields[0].field_type, StringType)

    def test_basemodel_with_optional_fields(self) -> None:
        """Test BaseModel with optional nested fields."""

        class NestedModel(BaseModel):
            required_field: str
            optional_field: typing.Optional[int]

        class MainModel(IcebergFastDataframeModel):
            nested: NestedModel

        schema = MainModel.iceberg_schema()
        field = schema.fields[0]
        struct_fields = field.field_type.fields

        required_field = next(f for f in struct_fields if f.name == "required_field")
        assert required_field.required is True

        optional_field = next(f for f in struct_fields if f.name == "optional_field")
        assert optional_field.required is False
        assert isinstance(optional_field.field_type, IntegerType)

    def test_list_of_basemodels(self) -> None:
        """Test list containing BaseModel elements."""

        class ItemModel(BaseModel):
            item_name: str
            item_value: int

        class MainModel(IcebergFastDataframeModel):
            items: list[ItemModel]

        schema = MainModel.iceberg_schema()
        field = schema.fields[0]
        assert isinstance(field.field_type, ListType)
        assert isinstance(field.field_type.element_type, StructType)

        # Check the structure of list elements
        element_fields = field.field_type.element_type.fields
        assert len(element_fields) == 2

        name_field = next(f for f in element_fields if f.name == "item_name")
        assert isinstance(name_field.field_type, StringType)

        value_field = next(f for f in element_fields if f.name == "item_value")
        assert isinstance(value_field.field_type, IntegerType)

    def test_dict_with_basemodel_values(self) -> None:
        """Test dict with BaseModel as values."""

        class ValueModel(BaseModel):
            value_field: str

        class MainModel(IcebergFastDataframeModel):
            mapping: dict[str, ValueModel]

        schema = MainModel.iceberg_schema()
        field = schema.fields[0]
        assert isinstance(field.field_type, MapType)
        assert isinstance(field.field_type.key_type, StringType)
        assert isinstance(field.field_type.value_type, StructType)

        # Check the structure of map values
        value_fields = field.field_type.value_type.fields
        assert len(value_fields) == 1
        assert value_fields[0].name == "value_field"
        assert isinstance(value_fields[0].field_type, StringType)


class TestIcebergValidationComplex:
    """Comprehensive tests for validate_schema with complex types (list, map, BaseModel)."""

    class ComplexDummyTable(Table):
        """A table implementation for testing complex type validation."""

        def __init__(self, schema_dict: dict) -> None:
            """Create table with schema from nested dict structure."""
            self._schema = self._dict_to_schema(schema_dict)

        def _dict_to_schema(self, schema_dict: dict) -> Schema:
            """Convert dict representation to Iceberg Schema."""
            fields = []
            for i, (field_name, field_spec) in enumerate(schema_dict.items(), 1):
                field_type = self._spec_to_iceberg_type(field_spec, i)
                fields.append(
                    NestedField(
                        field_id=i,
                        name=field_name,
                        field_type=field_type,
                        required=field_spec.get("required", True),
                    )
                )
            return Schema(*fields)

        def _spec_to_iceberg_type(self, spec: dict, field_id: int) -> IcebergType:
            """Convert spec dict to IcebergType."""
            if spec["type"] == "integer":
                return IntegerType()
            elif spec["type"] == "string":
                return StringType()
            elif spec["type"] == "boolean":
                return BooleanType()
            elif spec["type"] == "list":
                element_type = self._spec_to_iceberg_type(spec["element"], field_id)
                return ListType(
                    element_id=field_id,
                    element_type=element_type,
                    element_required=spec["element"].get("required", True),
                )
            elif spec["type"] == "map":
                key_type = self._spec_to_iceberg_type(spec["key"], field_id)
                value_type = self._spec_to_iceberg_type(spec["value"], field_id)
                return MapType(
                    key_id=field_id,
                    key_type=key_type,
                    value_id=field_id,
                    value_type=value_type,
                    value_required=spec["value"].get("required", True),
                )
            elif spec["type"] == "struct":
                nested_fields = []
                for j, (nested_name, nested_spec) in enumerate(
                    spec["fields"].items(), 1
                ):
                    nested_type = self._spec_to_iceberg_type(nested_spec, field_id + j)
                    nested_fields.append(
                        NestedField(
                            field_id=field_id + j,
                            name=nested_name,
                            field_type=nested_type,
                            required=nested_spec.get("required", True),
                        )
                    )
                return StructType(*nested_fields)
            else:
                return StringType()

        def schema(self) -> Schema:
            return self._schema

    def test_validate_list_field_present(self) -> None:
        """Test validation when list field is present."""

        class TestModel(IcebergFastDataframeModel):
            tags: list[str]
            numbers: list[int]

        table = self.ComplexDummyTable(
            {
                "tags": {"type": "list", "element": {"type": "string"}},
                "numbers": {"type": "list", "element": {"type": "integer"}},
            }
        )

        errors = TestModel.validate_schema(table)
        assert errors == []

    def test_validate_list_field_missing(self) -> None:
        """Test validation when list field is missing."""

        class TestModel(IcebergFastDataframeModel):
            tags: list[str]
            numbers: list[int]

        table = self.ComplexDummyTable(
            {
                "tags": {"type": "list", "element": {"type": "string"}},
                # missing numbers field
            }
        )

        errors = TestModel.validate_schema(table)
        assert len(errors) == 1
        assert errors[0].column_name == "numbers"
        assert errors[0].error_type == "MissingColumn"

    def test_validate_optional_list_field_missing(self) -> None:
        """Test validation when optional list field is missing (should be allowed)."""

        class TestModel(IcebergFastDataframeModel):
            tags: list[str]
            optional_numbers: typing.Optional[list[int]]

        table = self.ComplexDummyTable(
            {
                "tags": {"type": "list", "element": {"type": "string"}},
                # missing optional_numbers is OK
            }
        )

        errors = TestModel.validate_schema(table)
        assert len(errors) == 1  # Still expect missing column error for validation
        assert errors[0].column_name == "optional_numbers"

    def test_validate_map_field_present(self) -> None:
        """Test validation when map field is present."""

        class TestModel(IcebergFastDataframeModel):
            metadata: dict[str, int]
            config: dict[str, str]

        table = self.ComplexDummyTable(
            {
                "metadata": {
                    "type": "map",
                    "key": {"type": "string"},
                    "value": {"type": "integer"},
                },
                "config": {
                    "type": "map",
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                },
            }
        )

        errors = TestModel.validate_schema(table)
        assert errors == []

    def test_validate_map_field_missing(self) -> None:
        """Test validation when map field is missing."""

        class TestModel(IcebergFastDataframeModel):
            metadata: dict[str, int]
            config: dict[str, str]

        table = self.ComplexDummyTable(
            {
                "metadata": {
                    "type": "map",
                    "key": {"type": "string"},
                    "value": {"type": "integer"},
                },
                # missing config field
            }
        )

        errors = TestModel.validate_schema(table)
        assert len(errors) == 1
        assert errors[0].column_name == "config"
        assert errors[0].error_type == "MissingColumn"

    def test_validate_basemodel_field_present(self) -> None:
        """Test validation when BaseModel field is present."""

        class NestedModel(BaseModel):
            name: str
            value: int

        class TestModel(IcebergFastDataframeModel):
            nested: NestedModel

        table = self.ComplexDummyTable(
            {
                "nested": {
                    "type": "struct",
                    "fields": {
                        "name": {"type": "string"},
                        "value": {"type": "integer"},
                    },
                }
            }
        )

        errors = TestModel.validate_schema(table)
        assert errors == []

    def test_validate_basemodel_field_missing(self) -> None:
        """Test validation when BaseModel field is missing."""

        class NestedModel(BaseModel):
            name: str
            value: int

        class TestModel(IcebergFastDataframeModel):
            nested: NestedModel
            other_field: str

        table = self.ComplexDummyTable(
            {
                "other_field": {"type": "string"},
                # missing nested field
            }
        )

        errors = TestModel.validate_schema(table)
        assert len(errors) == 1
        assert errors[0].column_name == "nested"
        assert errors[0].error_type == "MissingColumn"

    def test_validate_nested_complex_types(self) -> None:
        """Test validation with deeply nested complex types."""

        class ItemModel(BaseModel):
            item_name: str
            item_value: int

        class TestModel(IcebergFastDataframeModel):
            items: list[ItemModel]
            mappings: dict[str, ItemModel]

        table = self.ComplexDummyTable(
            {
                "items": {
                    "type": "list",
                    "element": {
                        "type": "struct",
                        "fields": {
                            "item_name": {"type": "string"},
                            "item_value": {"type": "integer"},
                        },
                    },
                },
                "mappings": {
                    "type": "map",
                    "key": {"type": "string"},
                    "value": {
                        "type": "struct",
                        "fields": {
                            "item_name": {"type": "string"},
                            "item_value": {"type": "integer"},
                        },
                    },
                },
            }
        )

        errors = TestModel.validate_schema(table)
        assert errors == []

    def test_validate_partial_nested_complex_types(self) -> None:
        """Test validation with some complex fields missing."""

        class ItemModel(BaseModel):
            item_name: str
            item_value: int

        class TestModel(IcebergFastDataframeModel):
            items: list[ItemModel]
            mappings: dict[str, ItemModel]
            simple_field: str

        table = self.ComplexDummyTable(
            {
                "items": {
                    "type": "list",
                    "element": {
                        "type": "struct",
                        "fields": {
                            "item_name": {"type": "string"},
                            "item_value": {"type": "integer"},
                        },
                    },
                },
                # missing mappings and simple_field
            }
        )

        errors = TestModel.validate_schema(table)
        assert len(errors) == 2
        error_columns = {error.column_name for error in errors}
        assert error_columns == {"mappings", "simple_field"}

    def test_validate_mixed_optional_complex_types(self) -> None:
        """Test validation with mix of optional and required complex types."""

        class NestedModel(BaseModel):
            nested_field: str

        class TestModel(IcebergFastDataframeModel):
            required_list: list[str]
            optional_list: typing.Optional[list[int]]
            required_map: dict[str, str]
            optional_map: typing.Optional[dict[str, int]]
            required_nested: NestedModel
            optional_nested: typing.Optional[NestedModel]

        # Table only has required fields
        table = self.ComplexDummyTable(
            {
                "required_list": {"type": "list", "element": {"type": "string"}},
                "required_map": {
                    "type": "map",
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                },
                "required_nested": {
                    "type": "struct",
                    "fields": {"nested_field": {"type": "string"}},
                },
            }
        )

        errors = TestModel.validate_schema(table)
        # Should report missing optional fields too (current validation behavior)
        assert len(errors) == 3
        error_columns = {error.column_name for error in errors}
        assert error_columns == {"optional_list", "optional_map", "optional_nested"}
