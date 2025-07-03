from typing import Annotated
import polars as pl
from fastdataframe.polars.model import PolarsFastDataframeModel


class TestGetPolarsSchema:
    def test_get_polars_schema_with_simple_types(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            a: int
            b: str

        assert TestModel.get_polars_schema() == pl.Schema({"a": pl.Int64, "b": pl.Utf8})

    def test_get_polars_schema_with_annotated_polars_types(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            a_int_8: Annotated[int, pl.Int8]
            a_int_128: Annotated[int, pl.Int128]

        assert TestModel.get_polars_schema() == pl.Schema(
            {"a_int_8": pl.Int8, "a_int_128": pl.Int128}
        )
