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


class TestGetStringifiedSchema:
    def test_get_stringified_schema_with_simple_types(self) -> None:
        """Test that all field types are converted to pl.String regardless of original type."""
        class TestModel(PolarsFastDataframeModel):
            a: int
            b: str
            c: float
            d: bool

        expected_schema = pl.Schema({
            "a": pl.String,
            "b": pl.String,
            "c": pl.String,
            "d": pl.String
        })
        
        assert TestModel.get_stringified_schema() == expected_schema

    def test_get_stringified_schema_with_annotated_polars_types(self) -> None:
        """Test that annotated Polars types are also converted to pl.String."""
        class TestModel(PolarsFastDataframeModel):
            a_int_8: Annotated[int, pl.Int8]
            a_int_128: Annotated[int, pl.Int128]
            a_float: Annotated[float, pl.Float64]

        expected_schema = pl.Schema({
            "a_int_8": pl.String,
            "a_int_128": pl.String,
            "a_float": pl.String
        })
        
        assert TestModel.get_stringified_schema() == expected_schema

    def test_get_stringified_schema_with_empty_model(self) -> None:
        """Test that an empty model returns an empty schema."""
        class EmptyModel(PolarsFastDataframeModel):
            pass

        expected_schema = pl.Schema({})
        assert EmptyModel.get_stringified_schema() == expected_schema

    def test_get_stringified_schema_preserves_field_names(self) -> None:
        """Test that field names are preserved exactly as defined."""
        class TestModel(PolarsFastDataframeModel):
            user_id: int
            user_name: str
            is_active: bool
            created_at: float

        result_schema = TestModel.get_stringified_schema()
        
        # Check that all field names are present
        expected_field_names = {"user_id", "user_name", "is_active", "created_at"}
        assert set(result_schema.keys()) == expected_field_names
        
        # Check that all types are String
        for field_type in result_schema.values():
            assert field_type == pl.String

    def test_get_stringified_schema_vs_regular_schema(self) -> None:
        """Test that stringified schema differs from regular schema but has same field names."""
        class TestModel(PolarsFastDataframeModel):
            id: int
            name: str
            value: float

        regular_schema = TestModel.get_polars_schema()
        stringified_schema = TestModel.get_stringified_schema()
        
        # Field names should be the same
        assert set(regular_schema.keys()) == set(stringified_schema.keys())
        
        # Types should be different
        assert regular_schema != stringified_schema
        
        # Regular schema should have original types
        assert regular_schema["id"] == pl.Int64
        assert regular_schema["name"] == pl.Utf8
        assert regular_schema["value"] == pl.Float64
        
        # Stringified schema should have all String types
        assert stringified_schema["id"] == pl.String
        assert stringified_schema["name"] == pl.String
        assert stringified_schema["value"] == pl.String
