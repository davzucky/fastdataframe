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
from fastdataframe.iceberg.model import IcebergFastDataframeModel
import datetime
import uuid
import typing


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
def test_to_iceberg_schema_required(field_type, expected_iceberg_type, expected_required):
    class DynamicModel(IcebergFastDataframeModel):
        field_name: field_type

    schema = DynamicModel.to_iceberg_schema()
    assert len(schema.fields) == 1
    field = schema.fields[0]
    assert isinstance(field.field_type, expected_iceberg_type)
    assert field.required is expected_required
