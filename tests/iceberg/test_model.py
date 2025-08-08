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
        "field_type,expected_iceberg_type",
        [
            (int, IntegerType),
            (str, StringType),
            (bool, BooleanType),
            (datetime.date, DateType),
            (float, DoubleType),
            (bytes, BinaryType),
            (uuid.UUID, UUIDType),
            (datetime.datetime, TimestampType),
        ],
    )
    def test_to_iceberg_schema_simple_type(
        self, field_type: typing.Any, expected_iceberg_type: typing.Type[IcebergType]
    ) -> None:
        # Dynamically create a model class with a single field
        class DynamicModel(IcebergFastDataframeModel):
            field_name: field_type

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, expected_iceberg_type)
        assert field.required is True

    @pytest.mark.parametrize(
        "field_type,expected_iceberg_type",
        [
            (list[str], ListType(element_id=1, element_type=StringType(), element_required=True)),
            (set[str], ListType(element_id=1, element_type=StringType(), element_required=True)),
            (tuple[str], ListType(element_id=1, element_type=StringType(), element_required=True)),
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

    def test_to_iceberg_schema_optional_list_field(self) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: typing.Optional[list[int]]

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, ListType)
        assert isinstance(field.field_type.element_type, IntegerType)
        assert field.field_type.element_required is True
        assert field.required is False

    def test_to_iceberg_schema_optional_set_field(self) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: typing.Optional[set[int]]

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, ListType)
        assert isinstance(field.field_type.element_type, IntegerType)
        assert field.field_type.element_required is True
        assert field.required is False

    def test_to_iceberg_schema_optional_tuple_field(self) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: typing.Optional[tuple[int]]

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, ListType)
        assert isinstance(field.field_type.element_type, IntegerType)
        assert field.field_type.element_required is True
        assert field.required is False

    def test_to_iceberg_schema_list_of_optional_elements(self) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: list[typing.Optional[int]]

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, ListType)
        assert isinstance(field.field_type.element_type, IntegerType)
        assert field.field_type.element_required is False
        assert field.required is True

    def test_to_iceberg_schema_set_of_optional_elements(self) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: set[typing.Optional[int]]

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, ListType)
        assert isinstance(field.field_type.element_type, IntegerType)
        assert field.field_type.element_required is False
        assert field.required is True

    def test_to_iceberg_schema_tuple_of_optional_elements(self) -> None:
        class DynamicModel(IcebergFastDataframeModel):
            field_name: tuple[typing.Optional[int]]

        schema = DynamicModel.iceberg_schema()
        assert len(schema.fields) == 1
        field = schema.fields[0]
        assert isinstance(field.field_type, ListType)
        assert isinstance(field.field_type.element_type, IntegerType)
        assert field.field_type.element_required is False
        assert field.required is True

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
    def test_to_iceberg_schema_required(
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
