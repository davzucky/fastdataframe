from typing import Any, Union
from pyiceberg.schema import Schema, SchemaVisitorPerPrimitiveType, visit
from pyiceberg.types import (
    NestedField,
    IcebergType,
    IntegerType,
    BooleanType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
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


class JsonSchemaVisitor(SchemaVisitorPerPrimitiveType[dict[str, Any]]):
    def visit_boolean(self, field: BooleanType) -> dict[str, Any]:
        return {"type": "boolean"}

    def visit_integer(self, integer_type: IntegerType) -> dict[str, Any]:
        return {"type": "integer"}

    def visit_long(self, long_type: LongType) -> dict[str, Any]:
        return {"type": "integer"}

    def visit_float(self, float_type: FloatType) -> dict[str, Any]:
        return {"type": "number"}

    def visit_double(self, double_type: DoubleType) -> dict[str, Any]:
        return {"type": "number"}

    def visit_string(self, string_type: StringType) -> dict[str, Any]:
        return {"type": "string"}

    def visit_date(self, date_type: DateType) -> dict[str, Any]:
        return {"type": "string", "format": "date"}

    def visit_time(self, time_type: TimeType) -> dict[str, Any]:
        return {"type": "string", "format": "time"}

    def visit_timestamp(self, timestamp_type: TimestampType) -> dict[str, Any]:
        return {"type": "string", "format": "date-time"}

    def visit_timestamptz(self, timestamptz_type: TimestamptzType) -> dict[str, Any]:
        return {"type": "string", "format": "date-time"}

    def visit_uuid(self, uuid_type: UUIDType) -> dict[str, Any]:
        return {"type": "string", "format": "uuid"}

    def visit_binary(self, binary_type: BinaryType) -> dict[str, Any]:
        return {"type": "string", "format": "base64"}

    def visit_fixed(self, fixed_type: FixedType) -> dict[str, Any]:
        return {"type": "string", "format": "base64"}

    def visit_decimal(self, decimal_type: DecimalType) -> dict[str, Any]:
        return {"type": "number"}

    def struct(
        self, struct: StructType, field_results: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                field.name: result
                for field, result in zip(struct.fields, field_results)
            },
            "required": [field.name for field in struct.fields if field.required],
        }

    def list(
        self, list_type: ListType, element_result: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "type": "array",
            "items": element_result,
        }

    def map(
        self,
        map_type: MapType,
        key_result: dict[str, Any],
        value_result: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "type": "object",
            "additionalProperties": value_result,
        }

    def field(self, field: NestedField, field_result: dict[str, Any]) -> dict[str, Any]:
        return (
            field_result
            if field.required
            else {"anyOf": [field_result, {"type": "null"}]}
        )

    def schema(self, schema: Schema, struct_result: dict[str, Any]) -> dict[str, Any]:
        return struct_result


def iceberg_schema_to_json_schema(schema: Union[Schema, IcebergType]) -> dict[str, Any]:
    return visit(schema, JsonSchemaVisitor())
