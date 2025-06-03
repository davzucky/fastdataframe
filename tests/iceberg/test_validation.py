import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType
from pyiceberg.table import Table
from fastdataframe.iceberg.model import IcebergFastDataframeModel


@pytest.fixture
def model() -> type[IcebergFastDataframeModel]:
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
                    field_type=IntegerType() if col == "field1" else StringType(),
                    required=True,
                )
                for i, col in enumerate(columns)
            ]
        )

    def schema(self) -> Schema:
        return self._schema


def test_validate_table_all_columns_present(
    model: type[IcebergFastDataframeModel],
) -> None:
    table = DummyTable(["field1", "field2"])
    errors = model.validate_schema(table)
    assert errors == []


def test_validate_table_missing_column(model: type[IcebergFastDataframeModel]) -> None:
    table = DummyTable(["field1"])
    errors = model.validate_schema(table)
    assert len(errors) == 1
    assert errors[0].column_name == "field2"
    assert errors[0].error_type == "MissingColumn"
    assert "missing" in errors[0].error_details.lower()
