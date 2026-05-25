from __future__ import annotations

from typing import Annotated

import polars as pl

from fastdataframe import ColumnInfo, FastDataFrameModel, Int16
import fastdataframe.polars as fpl


class User(FastDataFrameModel):
    user_id: Annotated[int, ColumnInfo(dtype=Int16())]
    score: float = 0.0


def test_functional_schema_uses_column_definitions_and_dtype() -> None:
    assert fpl.schema(User) == pl.Schema({"user_id": pl.Int16, "score": pl.Float64})


def test_functional_string_schema() -> None:
    assert fpl.string_schema(User) == pl.Schema(
        {"user_id": pl.String, "score": pl.String}
    )


def test_functional_validate_schema_is_canonical() -> None:
    df = pl.DataFrame({"user_id": [1]})

    errors = fpl.validate_schema(User, df)

    assert len(errors) == 1
    assert errors[0].column_name == "score"


def test_functional_cast_uses_dtype() -> None:
    df = pl.DataFrame({"user_id": ["1"], "score": ["1.5"]})

    result = fpl.cast(User, df)

    assert result.schema["user_id"] == pl.Int16
    assert result.schema["score"] == pl.Float64
