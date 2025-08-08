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
)
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField
from pyiceberg.table import Table
from fastdataframe.iceberg.model import IcebergFastDataframeModel
import datetime
import uuid
import typing


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
            (list[str], ListType(element_id=1, element_type=StringType(), element_required=True)),
            (set[str], ListType(element_id=1, element_type=StringType(), element_required=True)),
            (tuple[str], ListType(element_id=1, element_type=StringType(), element_required=True)),
            (dict[str, int], MapType(key_id=1, key_type=StringType(), value_id=1, value_type=IntegerType(), value_required=True)),
            (dict[str, str], MapType(key_id=1, key_type=StringType(), value_id=1, value_type=StringType(), value_required=True)),
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
        self, field_type: typing.Any, element_type: type, element_required: bool, field_required: bool
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
        self, field_type: typing.Any, key_type: type, value_type: type, value_required: bool, field_required: bool
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
        self, field_type: typing.Any, element_type: type, element_required: bool, field_required: bool
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
        self, field_type: typing.Any, key_type: type, value_type: type, value_required: bool, field_required: bool
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
