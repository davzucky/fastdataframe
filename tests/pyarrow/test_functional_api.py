from __future__ import annotations

from typing import Annotated

import pyarrow as pa

from fastdataframe import ColumnInfo, FastDataFrameModel, Int16
import fastdataframe.pyarrow as farrow


class User(FastDataFrameModel):
    user_id: Annotated[int, ColumnInfo(dtype=Int16())]
    score: float = 0.0
    nickname: str | None = None


def test_functional_schema_uses_dtype_and_nullability() -> None:
    schema = farrow.schema(User)

    assert schema.field("user_id").type == pa.int16()
    assert schema.field("user_id").nullable is False
    assert schema.field("score").nullable is False
    assert schema.field("nickname").nullable is True


def test_functional_string_schema_preserves_nullability() -> None:
    schema = farrow.string_schema(User)

    assert schema.field("user_id").type == pa.string()
    assert schema.field("user_id").nullable is False
    assert schema.field("nickname").type == pa.string()
    assert schema.field("nickname").nullable is True
