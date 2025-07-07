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


class TestCastToModelSchema:
    def test_cast_to_model_schema_with_dataframe(self) -> None:
        """Test casting DataFrame columns to match model schema."""
        class TestModel(PolarsFastDataframeModel):
            id: int
            name: str
            value: float

        # Create DataFrame with string types
        df = pl.DataFrame({
            "id": ["1", "2", "3"],
            "name": ["Alice", "Bob", "Charlie"],
            "value": ["10.5", "20.7", "30.2"]
        })

        result = TestModel.cast_to_model_schema(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        # Check that types are cast correctly
        assert df_collected.schema["id"] == pl.Int64
        assert df_collected.schema["name"] == pl.Utf8
        assert df_collected.schema["value"] == pl.Float64
        # Check that values are properly converted
        assert df_collected["id"].to_list() == [1, 2, 3]
        assert df_collected["name"].to_list() == ["Alice", "Bob", "Charlie"]
        assert df_collected["value"].to_list() == [10.5, 20.7, 30.2]

    def test_cast_to_model_schema_with_lazyframe(self) -> None:
        """Test casting LazyFrame columns to match model schema."""
        class TestModel(PolarsFastDataframeModel):
            user_id: int
            is_active: bool
            score: float

        # Create LazyFrame with int types for is_active
        lf = pl.LazyFrame({
            "user_id": ["1", "2", "3"],
            "is_active": [1, 0, 1],
            "score": ["95.5", "87.2", "92.8"]
        })

        result = TestModel.cast_to_model_schema(lf)
        assert isinstance(result, pl.LazyFrame)
        df_collected = result.collect()
        resuld_schema = df_collected.schema
        # Check that types are cast correctly
        assert resuld_schema["user_id"] == pl.Int64
        assert resuld_schema["is_active"] == pl.Boolean
        assert resuld_schema["score"] == pl.Float64
        # Check that values are properly converted
        assert df_collected["user_id"].to_list() == [1, 2, 3]
        assert df_collected["is_active"].to_list() == [True, False, True]
        assert df_collected["score"].to_list() == [95.5, 87.2, 92.8]

    def test_cast_to_model_schema_with_annotated_types(self) -> None:
        """Test casting with annotated Polars types."""
        class TestModel(PolarsFastDataframeModel):
            small_int: Annotated[int, pl.Int8]
            big_int: Annotated[int, pl.Int128]
            precise_float: Annotated[float, pl.Float32]

        df = pl.DataFrame({
            "small_int": ["127", "-128", "0"],
            "big_int": ["9223372036854775807", "-9223372036854775808", "0"],
            "precise_float": ["3.14", "2.718", "1.414"]
        })

        result = TestModel.cast_to_model_schema(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        # Check that annotated types are respected
        assert df_collected.schema["small_int"] == pl.Int8
        assert df_collected.schema["big_int"] == pl.Int128
        assert df_collected.schema["precise_float"] == pl.Float32

    def test_cast_to_model_schema_preserves_data(self) -> None:
        """Test that casting preserves the original data values."""
        class TestModel(PolarsFastDataframeModel):
            count: int
            label: str
            ratio: float

        original_data = {
            "count": [42, 100, 7],
            "label": ["test", "production", "dev"],
            "ratio": [0.5, 1.0, 0.25]
        }
        
        df = pl.DataFrame(original_data)
        result = TestModel.cast_to_model_schema(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        # Check that data is preserved
        assert df_collected["count"].to_list() == original_data["count"]
        assert df_collected["label"].to_list() == original_data["label"]
        assert df_collected["ratio"].to_list() == original_data["ratio"]

    def test_cast_to_model_schema_with_mixed_types(self) -> None:
        """Test casting with mixed input types."""
        class TestModel(PolarsFastDataframeModel):
            number: int
            text: str
            decimal: float
            flag: bool

        # Create DataFrame with mixed types
        df = pl.DataFrame({
            "number": [1, 2, 3],  # Already int
            "text": ["a", "b", "c"],  # Already str
            "decimal": [1.1, 2.2, 3.3],  # Already float
            "flag": [True, False, True]  # Already bool
        })

        result = TestModel.cast_to_model_schema(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        # Check that types remain correct
        assert df_collected.schema["number"] == pl.Int64
        assert df_collected.schema["text"] == pl.Utf8
        assert df_collected.schema["decimal"] == pl.Float64
        assert df_collected.schema["flag"] == pl.Boolean
        # Check that data is unchanged
        assert df_collected["number"].to_list() == [1, 2, 3]
        assert df_collected["text"].to_list() == ["a", "b", "c"]
        assert df_collected["decimal"].to_list() == [1.1, 2.2, 3.3]
        assert df_collected["flag"].to_list() == [True, False, True]

    def test_cast_to_model_schema_returns_dataframe(self) -> None:
        """Test that the function always returns a DataFrame or LazyFrame as expected."""
        class TestModel(PolarsFastDataframeModel):
            x: int
            y: str

        # Test with DataFrame
        df = pl.DataFrame({"x": ["1", "2"], "y": ["a", "b"]})
        result_df = TestModel.cast_to_model_schema(df)
        assert isinstance(result_df, pl.DataFrame)
        # Test with LazyFrame
        lf = pl.LazyFrame({"x": ["1", "2"], "y": ["a", "b"]})
        result_lf = TestModel.cast_to_model_schema(lf)
        assert isinstance(result_lf, pl.LazyFrame)
