from __future__ import annotations

from typing import Annotated

from pyiceberg.types import IntegerType, LongType

from fastdataframe import ColumnInfo, FastDataFrameModel, Int16, Int64
import fastdataframe.iceberg as fice


class User(FastDataFrameModel):
    small_id: Annotated[int, ColumnInfo(dtype=Int16(), iceberg_id=10)]
    large_id: Annotated[int, ColumnInfo(dtype=Int64())]


def test_functional_schema_uses_dtype_widening_and_optional_field_id() -> None:
    schema = fice.schema(User)

    small = schema.find_field("small_id")
    large = schema.find_field("large_id")

    assert small.field_id == 10
    assert isinstance(small.field_type, IntegerType)
    assert isinstance(large.field_type, LongType)
