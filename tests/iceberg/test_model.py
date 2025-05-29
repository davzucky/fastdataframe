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
)
from pyiceberg.schema import Schema, NestedField
from src.fastdataframe.iceberg.model import IcebergFastDataframeModel
import datetime
import uuid


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
def test_to_iceberg_schema(field_type, expected_iceberg_type):
    # Dynamically create a model class with a single field
    class DynamicModel(IcebergFastDataframeModel):
        field_name: field_type

    schema = DynamicModel.to_iceberg_schema()
    assert len(schema.fields) == 1
    field = schema.fields[0]
    assert isinstance(field.field_type, expected_iceberg_type)
    assert field.required is True
