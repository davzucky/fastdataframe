import pytest
from pyiceberg.types import (
    NestedField,
    IntegerType,
    StringType,
    BooleanType,
    FloatType,
    DoubleType,
    LongType,
    DateType,
    TimeType,
    TimestampType,
    TimestamptzType,
    UUIDType,
    BinaryType,
    FixedType,
    DecimalType,
    StructType,
    ListType,
    MapType,
)
from fastdataframe.iceberg.json_schema import iceberg_schema_to_json_schema
from typing import Any


class TestIcebergJsonSchema:
    @pytest.mark.parametrize(
        "iceberg_type,expected_schema",
        [
            (IntegerType(), {"type": "integer"}),
            (LongType(), {"type": "integer"}),
            (FloatType(), {"type": "number"}),
            (DoubleType(), {"type": "number"}),
            (StringType(), {"type": "string"}),
            (BooleanType(), {"type": "boolean"}),
            (DateType(), {"type": "string", "format": "date"}),
            (TimeType(), {"type": "string", "format": "time"}),
            (TimestampType(), {"type": "string", "format": "date-time"}),
            (TimestamptzType(), {"type": "string", "format": "date-time"}),
            (UUIDType(), {"type": "string", "format": "uuid"}),
            (BinaryType(), {"type": "string", "format": "base64"}),
            (FixedType(4), {"type": "string", "format": "base64"}),
            (DecimalType(10, 2), {"type": "number"}),
            (
                StructType(NestedField(1, "field", IntegerType(), required=True)),
                {
                    "type": "object",
                    "properties": {"field": {"type": "integer"}},
                    "required": ["field"],
                },
            ),
            (
                ListType(1, IntegerType()),
                {"type": "array", "items": {"type": "integer"}},
            ),
            (
                MapType(1, StringType(), 2, IntegerType()),
                {"type": "object", "additionalProperties": {"type": "integer"}},
            ),
        ],
    )
    def test_iceberg_schema_to_json_schema_primitive(
        self, iceberg_type: Any, expected_schema: dict[str, Any]
    ) -> None:
        json_schema = iceberg_schema_to_json_schema(iceberg_type)
        assert json_schema == expected_schema
