from typing import Annotated
import polars as pl
from polars.exceptions import InvalidOperationError
from pydantic import Field
import pytest
from fastdataframe.core.annotation import ColumnInfo
from fastdataframe.polars.model import PolarsFastDataframeModel
import datetime as dt
from tests.test_models import UserTestModel, TemporalModel


class TestGetPolarsSchema:
    def test_get_polars_schema_with_simple_types(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            a: int
            b: str

        assert TestModel.get_polars_schema() == pl.Schema({"a": pl.Int64, "b": pl.Utf8})

    def test_get_polars_schema_with_alias_serialization(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            a_alias: Annotated[str, Field(alias="aAlias")]
            a_alias_serialize: Annotated[
                str, Field(serialization_alias="aliasSerialize")
            ]
            a_alias_validate: Annotated[str, Field(validation_alias="aliasValidate")]

        assert TestModel.get_polars_schema("serialization") == pl.Schema(
            {
                "aAlias": pl.String,
                "aliasSerialize": pl.String,
                "a_alias_validate": pl.String,
            }
        )

    def test_get_polars_schema_with_alias_validation(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            a_alias: Annotated[str, Field(alias="aAlias")]
            a_alias_serialize: Annotated[
                str, Field(serialization_alias="aliasSerialize")
            ]
            a_alias_validate: Annotated[str, Field(validation_alias="aliasValidate")]

        assert TestModel.get_polars_schema("validation") == pl.Schema(
            {
                "aAlias": pl.String,
                "a_alias_serialize": pl.String,
                "aliasValidate": pl.String,
            }
        )

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

        expected_schema = pl.Schema(
            {"a": pl.String, "b": pl.String, "c": pl.String, "d": pl.String}
        )

        assert TestModel.get_stringified_schema() == expected_schema

    def test_get_stringified_schema_with_annotated_polars_types(self) -> None:
        """Test that annotated Polars types are also converted to pl.String."""

        class TestModel(PolarsFastDataframeModel):
            a_int_8: Annotated[int, pl.Int8]
            a_int_128: Annotated[int, pl.Int128]
            a_float: Annotated[float, pl.Float64]

        expected_schema = pl.Schema(
            {"a_int_8": pl.String, "a_int_128": pl.String, "a_float": pl.String}
        )

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


class TestCast:
    def test_cast_to_model_schema_with_dataframe(self) -> None:
        """Test casting DataFrame columns to match model schema."""

        class TestModel(PolarsFastDataframeModel):
            id: int
            name: str
            value: float

        # Create DataFrame with string types
        df = pl.DataFrame(
            {
                "id": ["1", "2", "3"],
                "name": ["Alice", "Bob", "Charlie"],
                "value": ["10.5", "20.7", "30.2"],
            }
        )

        result = TestModel.cast(df)
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
            is_active: Annotated[
                bool, ColumnInfo(bool_true_string="1", bool_false_string="0")
            ]
            score: float

        # Create LazyFrame with int types for is_active
        lf = pl.LazyFrame(
            {
                "user_id": ["1", "2", "3"],
                "is_active": ["1", "0", "1"],
                "score": ["95.5", "87.2", "92.8"],
            }
        )

        result = TestModel.cast(lf)
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

    @pytest.mark.parametrize(
        "true_str, false_str, input_values",
        [("1", "0", ["1", "0", "1"]), ("true", "false", ["true", "false", "true"])],
    )
    def test_cast_to_model_string_to_bool(
        self, true_str: str, false_str: str, input_values: list[str]
    ) -> None:
        class TestModel(PolarsFastDataframeModel):
            is_active: Annotated[
                bool, ColumnInfo(bool_true_string=true_str, bool_false_string=false_str)
            ]

        df = pl.DataFrame({"is_active": input_values})
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        assert df_collected["is_active"].to_list() == [True, False, True]

    @pytest.mark.parametrize(
        "true_str, false_str, input_values",
        [("1", "0", ["1", "0", "true"]), ("true", "false", ["true", "false", "0"])],
    )
    def test_cast_to_model_string_to_bool_raise_error(
        self, true_str: str, false_str: str, input_values: list[str]
    ) -> None:
        class TestModel(PolarsFastDataframeModel):
            is_active: Annotated[
                bool, ColumnInfo(bool_true_string=true_str, bool_false_string=false_str)
            ]

        df = pl.DataFrame({"is_active": input_values})
        with pytest.raises(InvalidOperationError):
            TestModel.cast(df)

    @pytest.mark.parametrize(
        "date_format, input_values",
        [
            ("%Y-%m-%d", ["2021-01-01", "2021-01-02", "2021-01-03"]),
            ("%Y/%m/%d", ["2021/01/01", "2021/01/02", "2021/01/03"]),
        ],
    )
    def test_cast_to_model_string_to_date(
        self, date_format: str, input_values: list[str]
    ) -> None:
        class TestModel(PolarsFastDataframeModel):
            birthday: Annotated[dt.date, ColumnInfo(date_format=date_format)]

        df = pl.DataFrame({"birthday": input_values})
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        assert df_collected["birthday"].to_list() == [
            dt.date(2021, 1, 1),
            dt.date(2021, 1, 2),
            dt.date(2021, 1, 3),
        ]

    @pytest.mark.parametrize(
        "date_format, input_values",
        [
            ("%Y-%m-%d", ["2021-01-01", "2021/01/02", "2021-01-03"]),
            ("%Y/%m/%d", ["2021/01/01", "2021-01-02", "2021/01/03"]),
        ],
    )
    def test_cast_to_model_string_to_date_raise_error(
        self, date_format: str, input_values: list[str]
    ) -> None:
        class TestModel(PolarsFastDataframeModel):
            birthday: Annotated[dt.date, ColumnInfo(date_format=date_format)]

        df = pl.DataFrame({"birthday": input_values})
        with pytest.raises(InvalidOperationError):
            TestModel.cast(df)

    def test_cast_to_model_schema_with_annotated_types(self) -> None:
        """Test casting with annotated Polars types."""

        class TestModel(PolarsFastDataframeModel):
            small_int: Annotated[int, pl.Int8]
            big_int: Annotated[int, pl.Int128]
            precise_float: Annotated[float, pl.Float32]

        df = pl.DataFrame(
            {
                "small_int": ["127", "-128", "0"],
                "big_int": ["9223372036854775807", "-9223372036854775808", "0"],
                "precise_float": ["3.14", "2.718", "1.414"],
            }
        )

        result = TestModel.cast(df)
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
            "ratio": [0.5, 1.0, 0.25],
        }

        df = pl.DataFrame(original_data)
        result = TestModel.cast(df)
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
        df = pl.DataFrame(
            {
                "number": [1, 2, 3],  # Already int
                "text": ["a", "b", "c"],  # Already str
                "decimal": [1.1, 2.2, 3.3],  # Already float
                "flag": [True, False, True],  # Already bool
            }
        )

        result = TestModel.cast(df)
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
        result_df = TestModel.cast(df)
        assert isinstance(result_df, pl.DataFrame)
        # Test with LazyFrame
        lf = pl.LazyFrame({"x": ["1", "2"], "y": ["a", "b"]})
        result_lf = TestModel.cast(lf)
        assert isinstance(result_lf, pl.LazyFrame)


class TestPolarsValidation:
    class TestModel(PolarsFastDataframeModel):
        __test__ = False
        field1: int
        field2: str

    def test_from_fastdataframe_model_basic_conversion(self) -> None:
        PolarsModel = PolarsFastDataframeModel.from_base_model(UserTestModel)
        assert issubclass(PolarsModel, PolarsFastDataframeModel)
        assert PolarsModel.__name__ == "UserTestModelPolars"
        assert PolarsModel.model_fields.keys() == UserTestModel.model_fields.keys()
        assert PolarsModel.__doc__ == "Polars version of UserTestModel"
        polars_json_schema = PolarsModel.model_json_schema()
        base_json_shema = UserTestModel.model_json_schema()
        assert polars_json_schema["properties"] == base_json_shema["properties"]
        assert polars_json_schema["required"] == base_json_shema["required"]

    def test_from_fastdataframe_model_valid_frame(self) -> None:
        PolarsModel = PolarsFastDataframeModel.from_base_model(UserTestModel)
        valid_frame = pl.LazyFrame(
            {
                "name": ["John", "Jane"],
                "age": [30, 25],
                "is_active": [True, False],
                "score": [95.5, None],
            }
        )
        errors = PolarsModel.validate_schema(valid_frame)
        assert len(errors) == 0

    def test_from_fastdataframe_model_missing_optional(self) -> None:
        PolarsModel = PolarsFastDataframeModel.from_base_model(UserTestModel)
        invalid_frame = pl.LazyFrame(
            {
                "name": ["John", "Jane"],
                "age": [30, 25],
                "is_active": [True, False],
            }
        )
        errors = PolarsModel.validate_schema(invalid_frame)
        assert len(errors) == 0

    def test_from_fastdataframe_model_type_mismatch(self) -> None:
        PolarsModel = PolarsFastDataframeModel.from_base_model(UserTestModel)
        type_mismatch_frame = pl.LazyFrame(
            {
                "name": ["John", "Jane"],
                "age": ["30", "25"],
                "is_active": [True, False],
                "score": ["95.5", None],
            }
        )
        errors = PolarsModel.validate_schema(type_mismatch_frame)
        assert len(errors) == 2
        error_types = {error.column_name: error.error_type for error in errors}
        assert "age" in error_types
        assert "score" in error_types
        assert error_types["age"] == "TypeMismatch"
        assert error_types["score"] == "TypeMismatch"

    def test_validate_missing_columns(self) -> None:
        lazy_frame = pl.LazyFrame({"field1": [1, 2, 3]})
        errors = TestPolarsValidation.TestModel.validate_schema(lazy_frame)
        assert len(errors) == 1
        assert errors[0].column_name == "field2"
        assert errors[0].error_type == "MissingColumn"
        assert errors[0].error_details == "Column field2 is missing in the frame."

    def test_validate_column_types(self) -> None:
        lazy_frame = pl.LazyFrame(
            {"field1": ["1", "2", "3"], "field2": ["a", "b", "c"]}
        )
        errors = TestPolarsValidation.TestModel.validate_schema(lazy_frame)
        assert len(errors) == 1
        assert errors[0].column_name == "field1"
        assert errors[0].error_type == "TypeMismatch"
        assert errors[0].error_details == "Expected type integer, but got string."

    def test_validate_schema_valid_frame(self) -> None:
        lazy_frame = pl.LazyFrame({"field1": [1, 2, 3], "field2": ["a", "b", "c"]})
        errors = TestPolarsValidation.TestModel.validate_schema(lazy_frame)
        assert len(errors) == 0

    def test_polarsfastdataframemodel_with_temporal_types(self) -> None:
        PolarsModel = PolarsFastDataframeModel.from_base_model(TemporalModel)
        today = dt.date.today()
        now = dt.datetime.now()
        t = now.time()
        td_ = dt.timedelta(days=1, hours=2)
        frame = pl.LazyFrame(
            {
                "d": [today, today],
                "dt_": [now, now],
                "t": [t, t],
                "td": [td_, td_],
            }
        )
        errors = PolarsModel.validate_schema(frame)
        assert len(errors) == 0


class TestRename:
    def test_rename_serialization_to_validation(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            a: int
            b: str
            c: Annotated[
                int, Field(validation_alias="val_c", serialization_alias="ser_c")
            ]

        df = pl.DataFrame(
            {
                "a": [1],
                "b": ["x"],
                "ser_c": [42],
            }
        )

        renamed = df.pipe(
            TestModel.rename,
            alias_type_from="serialization",
            alias_type_to="validation",
        )
        assert isinstance(renamed, pl.DataFrame)
        assert set(renamed.columns) == {"a", "b", "val_c"}
        assert renamed["val_c"][0] == 42

    def test_rename_validation_to_serialization(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            a: int
            b: str
            c: Annotated[
                int, Field(validation_alias="val_c", serialization_alias="ser_c")
            ]

        df = pl.DataFrame(
            {
                "a": [1],
                "b": ["x"],
                "val_c": [99],
            }
        )
        renamed = df.pipe(
            TestModel.rename,
            alias_type_from="validation",
            alias_type_to="serialization",
        )
        assert isinstance(renamed, pl.DataFrame)

        assert set(renamed.columns) == {"a", "b", "ser_c"}
        assert renamed["ser_c"][0] == 99

    def test_rename_identity_when_no_alias(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            x: int
            y: str

        df = pl.DataFrame({"x": [1], "y": ["foo"]})
        renamed = df.pipe(TestModel.rename)
        assert isinstance(renamed, pl.DataFrame)

        assert set(renamed.columns) == {"x", "y"}
        assert renamed["x"][0] == 1
        assert renamed["y"][0] == "foo"

    def test_rename_with_missing_columns(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            a: int
            b: str
            c: Annotated[
                int, Field(validation_alias="val_c", serialization_alias="ser_c")
            ]

        df = pl.DataFrame({"a": [1], "ser_c": [2]})
        renamed = df.pipe(
            TestModel.rename,
            alias_type_from="serialization",
            alias_type_to="validation",
        )
        # Only columns present in both df and model mapping are renamed
        assert isinstance(renamed, pl.DataFrame)
        assert set(renamed.columns) == {"a", "val_c"}
        assert renamed["a"][0] == 1
        assert renamed["val_c"][0] == 2

    def test_rename_lazyframe(self) -> None:
        class TestModel(PolarsFastDataframeModel):
            foo: int
            bar: Annotated[
                str, Field(validation_alias="baz", serialization_alias="qux")
            ]

        lf = pl.LazyFrame({"foo": [1], "qux": ["hello"]})
        renamed = lf.pipe(
            TestModel.rename,
            alias_type_from="serialization",
            alias_type_to="validation",
        )
        assert isinstance(renamed, pl.LazyFrame)
        collected = renamed.collect()
        assert set(collected.columns) == {"foo", "baz"}
        assert collected["foo"][0] == 1
        assert collected["baz"][0] == "hello"


class TestCollectionTypes:
    """Test support for list, tuple, and set types in Polars models."""

    def test_list_type_schema_generation(self) -> None:
        """Test that list[T] types generate correct Polars List schemas."""

        class TestModel(PolarsFastDataframeModel):
            int_list: list[int]
            str_list: list[str]
            float_list: list[float]

        schema = TestModel.get_polars_schema()
        assert schema["int_list"] == pl.List(pl.Int64)
        assert schema["str_list"] == pl.List(pl.String)
        assert schema["float_list"] == pl.List(pl.Float64)

    def test_tuple_variable_length_schema_generation(self) -> None:
        """Test that tuple[T, ...] types generate correct Polars List schemas."""

        class TestModel(PolarsFastDataframeModel):
            int_tuple: tuple[int, ...]
            str_tuple: tuple[str, ...]
            bool_tuple: tuple[bool, ...]

        schema = TestModel.get_polars_schema()
        assert schema["int_tuple"] == pl.List(pl.Int64)
        assert schema["str_tuple"] == pl.List(pl.String)
        assert schema["bool_tuple"] == pl.List(pl.Boolean)

    def test_set_type_schema_generation(self) -> None:
        """Test that set[T] types generate correct Polars List schemas."""

        class TestModel(PolarsFastDataframeModel):
            int_set: set[int]
            str_set: set[str]
            float_set: set[float]

        schema = TestModel.get_polars_schema()
        assert schema["int_set"] == pl.List(pl.Int64)
        assert schema["str_set"] == pl.List(pl.String)
        assert schema["float_set"] == pl.List(pl.Float64)

    def test_list_validation_success(self) -> None:
        """Test that valid list data passes validation."""

        class TestModel(PolarsFastDataframeModel):
            numbers: list[int]
            words: list[str]

        frame = pl.LazyFrame(
            {
                "numbers": [[1, 2, 3], [4, 5], [6]],
                "words": [["hello", "world"], ["foo"], ["bar", "baz"]],
            }
        )

        errors = TestModel.validate_schema(frame)
        assert len(errors) == 0

    def test_tuple_validation_success(self) -> None:
        """Test that valid tuple data (represented as lists) passes validation."""

        class TestModel(PolarsFastDataframeModel):
            coordinates: tuple[float, ...]
            tags: tuple[str, ...]

        frame = pl.LazyFrame(
            {
                "coordinates": [[1.0, 2.0], [3.0, 4.0, 5.0], [6.0]],
                "tags": [["tag1", "tag2"], ["tag3"], ["tag4", "tag5", "tag6"]],
            }
        )

        errors = TestModel.validate_schema(frame)
        assert len(errors) == 0

    def test_set_validation_success(self) -> None:
        """Test that valid set data (represented as lists) passes validation."""

        class TestModel(PolarsFastDataframeModel):
            unique_ids: set[int]
            categories: set[str]

        frame = pl.LazyFrame(
            {
                "unique_ids": [[1, 2, 3], [4, 5], [6, 7, 8, 9]],
                "categories": [["A", "B"], ["C"], ["D", "E"]],
            }
        )

        errors = TestModel.validate_schema(frame)
        assert len(errors) == 0

    def test_nested_collection_types(self) -> None:
        """Test nested collection types like list[list[int]]."""

        class TestModel(PolarsFastDataframeModel):
            matrix: list[list[int]]

        schema = TestModel.get_polars_schema()
        # list[list[int]] should be pl.List(pl.List(pl.Int64))
        expected_inner = pl.List(pl.Int64)
        assert schema["matrix"] == pl.List(expected_inner)

    def test_collection_type_casting(self) -> None:
        """Test casting string representations to collection types."""

        class TestModel(PolarsFastDataframeModel):
            numbers: list[int]
            words: list[str]

        # Create DataFrame with actual list data (not string representations)
        # since Polars handles list parsing differently than primitive types
        df = pl.DataFrame(
            {
                "numbers": [[1, 2, 3], [4, 5], []],
                "words": [["hello", "world"], ["foo"], ["bar"]],
            }
        )

        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result

        assert df_collected.schema["numbers"] == pl.List(pl.Int64)
        assert df_collected.schema["words"] == pl.List(pl.String)

        # Verify data integrity
        assert df_collected["numbers"].to_list() == [[1, 2, 3], [4, 5], []]
        assert df_collected["words"].to_list() == [["hello", "world"], ["foo"], ["bar"]]

    @pytest.mark.parametrize(
        "python_type,expected_polars_type",
        [
            (list[int], pl.List(pl.Int64)),
            (list[str], pl.List(pl.String)),
            (list[float], pl.List(pl.Float64)),
            (list[bool], pl.List(pl.Boolean)),
            (tuple[int, ...], pl.List(pl.Int64)),
            (tuple[str, ...], pl.List(pl.String)),
            (set[int], pl.List(pl.Int64)),
            (set[str], pl.List(pl.String)),
            (list[list[int]], pl.List(pl.List(pl.Int64))),
        ],
    )
    def test_collection_type_mapping_parametrized(
        self, python_type, expected_polars_type
    ) -> None:
        """Parametrized test for various collection type mappings."""
        from fastdataframe.polars._types import get_polars_type
        from pydantic.fields import FieldInfo

        field_info = FieldInfo(annotation=python_type)
        result = get_polars_type(field_info)
        assert result == expected_polars_type

    def test_empty_collections_validation(self) -> None:
        """Test that empty collections are handled correctly."""

        class TestModel(PolarsFastDataframeModel):
            empty_list: list[int]
            empty_set: set[str]

        frame = pl.LazyFrame(
            {"empty_list": [[], [1, 2], []], "empty_set": [[], ["a"], []]}
        )

        errors = TestModel.validate_schema(frame)
        assert len(errors) == 0


class TestBaseModelTypes:
    """Test support for Pydantic BaseModel types in Polars models."""

    def test_simple_basemodel_schema_generation(self) -> None:
        """Test that BaseModel fields generate correct Polars Struct schemas."""
        from pydantic import BaseModel

        class Address(BaseModel):
            street: str
            city: str
            zip_code: int

        class TestModel(PolarsFastDataframeModel):
            name: str
            address: Address

        schema = TestModel.get_polars_schema()

        # Check that address field is a Struct
        assert isinstance(schema["address"], pl.Struct)

        # Check struct fields
        address_struct = schema["address"]
        expected_fields = {"street": pl.String, "city": pl.String, "zip_code": pl.Int64}

        actual_fields = {field.name: field.dtype for field in address_struct.fields}
        assert actual_fields == expected_fields

    def test_nested_basemodel_schema_generation(self) -> None:
        """Test nested BaseModel structures."""
        from pydantic import BaseModel

        class ContactInfo(BaseModel):
            email: str
            phone: str

        class Address(BaseModel):
            street: str
            city: str
            contact: ContactInfo

        class TestModel(PolarsFastDataframeModel):
            name: str
            address: Address

        schema = TestModel.get_polars_schema()

        # Check outer struct
        assert isinstance(schema["address"], pl.Struct)
        address_struct = schema["address"]

        # Find the contact field within the address struct
        contact_field = next(f for f in address_struct.fields if f.name == "contact")
        assert isinstance(contact_field.dtype, pl.Struct)

        # Check nested struct fields
        contact_struct = contact_field.dtype
        contact_fields = {field.name: field.dtype for field in contact_struct.fields}
        assert contact_fields == {"email": pl.String, "phone": pl.String}

    def test_optional_basemodel_schema_generation(self) -> None:
        """Test optional BaseModel fields."""
        from pydantic import BaseModel
        from typing import Optional

        class Address(BaseModel):
            street: str
            city: str

        class TestModel(PolarsFastDataframeModel):
            name: str
            address: Optional[Address] = None

        schema = TestModel.get_polars_schema()

        # Check that optional address is still a Struct (Polars handles nullability at the data level)
        assert isinstance(schema["address"], pl.Struct)

        # Check struct fields are correct
        address_struct = schema["address"]
        actual_fields = {field.name: field.dtype for field in address_struct.fields}
        assert actual_fields == {"street": pl.String, "city": pl.String}

    def test_basemodel_validation_success(self) -> None:
        """Test that valid BaseModel data passes validation."""
        from pydantic import BaseModel

        class Address(BaseModel):
            street: str
            city: str
            zip_code: int

        class TestModel(PolarsFastDataframeModel):
            name: str
            age: int
            address: Address

        frame = pl.LazyFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [30, 25],
                "address": [
                    {"street": "123 Main St", "city": "NYC", "zip_code": 10001},
                    {"street": "456 Oak Ave", "city": "LA", "zip_code": 90210},
                ],
            }
        )

        errors = TestModel.validate_schema(frame)
        assert len(errors) == 0

    def test_nested_basemodel_validation_success(self) -> None:
        """Test that nested BaseModel data passes validation."""
        from pydantic import BaseModel

        class ContactInfo(BaseModel):
            email: str
            phone: str

        class Address(BaseModel):
            street: str
            city: str
            contact: ContactInfo

        class TestModel(PolarsFastDataframeModel):
            name: str
            address: Address

        frame = pl.LazyFrame(
            {
                "name": ["Alice", "Bob"],
                "address": [
                    {
                        "street": "123 Main St",
                        "city": "NYC",
                        "contact": {"email": "alice@example.com", "phone": "555-0001"},
                    },
                    {
                        "street": "456 Oak Ave",
                        "city": "LA",
                        "contact": {"email": "bob@example.com", "phone": "555-0002"},
                    },
                ],
            }
        )

        errors = TestModel.validate_schema(frame)
        assert len(errors) == 0

    def test_basemodel_with_collections(self) -> None:
        """Test BaseModel fields combined with collection types."""
        from pydantic import BaseModel

        class Tag(BaseModel):
            name: str
            priority: int

        class TestModel(PolarsFastDataframeModel):
            title: str
            tags: list[Tag]  # List of BaseModel objects

        schema = TestModel.get_polars_schema()

        # Check that tags is a List of Struct
        assert isinstance(schema["tags"], pl.List)
        assert isinstance(schema["tags"].inner, pl.Struct)

        # Check the struct fields inside the list
        tag_struct = schema["tags"].inner
        tag_fields = {field.name: field.dtype for field in tag_struct.fields}
        assert tag_fields == {"name": pl.String, "priority": pl.Int64}

    def test_basemodel_casting(self) -> None:
        """Test casting BaseModel data."""
        from pydantic import BaseModel

        class Address(BaseModel):
            street: str
            city: str
            zip_code: int

        class TestModel(PolarsFastDataframeModel):
            name: str
            address: Address

        # Create DataFrame with struct data
        df = pl.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "address": [
                    {"street": "123 Main St", "city": "NYC", "zip_code": 10001},
                    {"street": "456 Oak Ave", "city": "LA", "zip_code": 90210},
                ],
            }
        )

        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result

        # Check schema is correct
        assert df_collected.schema["name"] == pl.String
        assert isinstance(df_collected.schema["address"], pl.Struct)

        # Check data integrity
        addresses = df_collected["address"].to_list()
        assert addresses[0]["street"] == "123 Main St"
        assert addresses[0]["zip_code"] == 10001
        assert addresses[1]["city"] == "LA"

    @pytest.mark.parametrize(
        "basemodel_structure",
        [
            # Simple BaseModel
            "simple",
            # Nested BaseModel (2 levels)
            "nested_2_levels",
            # Deeply nested BaseModel (3 levels)
            "nested_3_levels",
        ],
    )
    def test_basemodel_type_mapping_parametrized(self, basemodel_structure) -> None:
        """Parametrized test for various BaseModel structures."""
        from pydantic import BaseModel
        from fastdataframe.polars._types import get_polars_type
        from pydantic.fields import FieldInfo

        if basemodel_structure == "simple":

            class TestModel(BaseModel):
                name: str
                value: int

            field_info = FieldInfo(annotation=TestModel)
            result = get_polars_type(field_info)

            assert isinstance(result, pl.Struct)
            fields = {f.name: f.dtype for f in result.fields}
            assert fields == {"name": pl.String, "value": pl.Int64}

        elif basemodel_structure == "nested_2_levels":

            class Inner(BaseModel):
                data: str

            class Outer(BaseModel):
                name: str
                inner: Inner

            field_info = FieldInfo(annotation=Outer)
            result = get_polars_type(field_info)

            assert isinstance(result, pl.Struct)
            # Check outer fields
            outer_fields = {f.name: f.dtype for f in result.fields}
            assert "name" in outer_fields and outer_fields["name"] == pl.String
            assert "inner" in outer_fields and isinstance(
                outer_fields["inner"], pl.Struct
            )

            # Check inner struct
            inner_struct = outer_fields["inner"]
            inner_fields = {f.name: f.dtype for f in inner_struct.fields}
            assert inner_fields == {"data": pl.String}

        elif basemodel_structure == "nested_3_levels":

            class Deep(BaseModel):
                value: int

            class Middle(BaseModel):
                name: str
                deep: Deep

            class Top(BaseModel):
                title: str
                middle: Middle

            field_info = FieldInfo(annotation=Top)
            result = get_polars_type(field_info)

            assert isinstance(result, pl.Struct)

            # Navigate through the nested structure
            top_fields = {f.name: f.dtype for f in result.fields}
            assert "title" in top_fields and top_fields["title"] == pl.String
            assert "middle" in top_fields and isinstance(
                top_fields["middle"], pl.Struct
            )

            middle_struct = top_fields["middle"]
            middle_fields = {f.name: f.dtype for f in middle_struct.fields}
            assert "name" in middle_fields and middle_fields["name"] == pl.String
            assert "deep" in middle_fields and isinstance(
                middle_fields["deep"], pl.Struct
            )

            deep_struct = middle_fields["deep"]
            deep_fields = {f.name: f.dtype for f in deep_struct.fields}
            assert deep_fields == {"value": pl.Int64}

    def test_basemodel_with_field_aliases(self) -> None:
        """Test BaseModel with Pydantic field aliases."""
        from pydantic import BaseModel, Field
        from typing import Annotated

        class Address(BaseModel):
            street_name: Annotated[str, Field(alias="street")]
            city_name: Annotated[str, Field(alias="city")]

        class TestModel(PolarsFastDataframeModel):
            name: str
            address: Address

        # Test serialization alias schema
        schema = TestModel.get_polars_schema("serialization")
        assert isinstance(schema["address"], pl.Struct)

        address_struct = schema["address"]
        address_fields = {field.name: field.dtype for field in address_struct.fields}
        # Should use the aliases
        assert "street" in address_fields
        assert "city" in address_fields
        assert address_fields == {"street": pl.String, "city": pl.String}

    def test_empty_basemodel(self) -> None:
        """Test BaseModel with no fields."""
        from pydantic import BaseModel

        class EmptyModel(BaseModel):
            pass

        class TestModel(PolarsFastDataframeModel):
            name: str
            empty: EmptyModel

        schema = TestModel.get_polars_schema()
        assert isinstance(schema["empty"], pl.Struct)

        # Empty struct should have no fields
        empty_struct = schema["empty"]
        assert len(empty_struct.fields) == 0
