"""Test cases for polars cast functions."""

import polars as pl
import pytest
from typing import Annotated
import datetime as dt
from decimal import Decimal

from fastdataframe.core.annotation import ColumnInfo
from fastdataframe.polars.model import PolarsFastDataframeModel


class TestStringToNumericCasting:
    """Test string casting to numeric types."""
    
    @pytest.mark.parametrize(
        "target_type,string_values,expected_values,expected_polars_type",
        [
            # Integer types
            (int, ["1", "2", "3"], [1, 2, 3], pl.Int64),
            # Signed integers
            ("int8", ["-128", "127", "0"], [-128, 127, 0], pl.Int8),
            ("int16", ["-32768", "32767", "0"], [-32768, 32767, 0], pl.Int16), 
            ("int32", ["-2147483648", "2147483647", "0"], [-2147483648, 2147483647, 0], pl.Int32),
            ("int128", ["170141183460469231731687303715884105727", "-170141183460469231731687303715884105728", "0"], 
             [170141183460469231731687303715884105727, -170141183460469231731687303715884105728, 0], pl.Int128),
            # Unsigned integers
            ("uint8", ["0", "255", "100"], [0, 255, 100], pl.UInt8),
            ("uint16", ["0", "65535", "100"], [0, 65535, 100], pl.UInt16),
            ("uint32", ["0", "4294967295", "100"], [0, 4294967295, 100], pl.UInt32),
            ("uint64", ["0", "18446744073709551615", "100"], [0, 18446744073709551615, 100], pl.UInt64),
            # Float types
            (float, ["1.5", "2.7", "3.14"], [1.5, 2.7, 3.14], pl.Float64),
            ("float32", ["1.5", "2.7", "3.14"], [1.5, 2.7, 3.14], pl.Float32),
        ]
    )
    def test_string_to_numeric_casting(self, target_type, string_values, expected_values, expected_polars_type):
        """Test casting strings to various numeric types."""
        
        if target_type == int:
            class TestModel(PolarsFastDataframeModel):
                value: int
        elif target_type == float:
            class TestModel(PolarsFastDataframeModel):
                value: float
        elif target_type == "int8":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.Int8]
        elif target_type == "int16":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.Int16]
        elif target_type == "int32":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.Int32]
        elif target_type == "int128":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.Int128]
        elif target_type == "uint8":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.UInt8]
        elif target_type == "uint16":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.UInt16]
        elif target_type == "uint32":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.UInt32]
        elif target_type == "uint64":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.UInt64]
        elif target_type == "float32":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[float, pl.Float32]
        
        df = pl.DataFrame({"value": string_values})
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["value"] == expected_polars_type
        actual_values = df_collected["value"].to_list()
        
        # For Float32, use approximate comparison due to precision differences
        if expected_polars_type == pl.Float32:
            import math
            for actual, expected in zip(actual_values, expected_values):
                assert math.isclose(actual, expected, rel_tol=1e-6)
        else:
            assert actual_values == expected_values

    @pytest.mark.parametrize(
        "target_type,invalid_values",
        [
            (int, ["abc", "1.5", ""]),
            ("int8", ["128", "-129", "abc"]),
            ("uint8", ["-1", "256", "abc"]),
            (float, ["abc", ""]),
        ]
    )
    def test_string_to_numeric_casting_errors(self, target_type, invalid_values):
        """Test that invalid string values raise errors during casting."""
        
        if target_type == int:
            class TestModel(PolarsFastDataframeModel):
                value: int
        elif target_type == "int8":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.Int8]
        elif target_type == "uint8":
            class TestModel(PolarsFastDataframeModel):
                value: Annotated[int, pl.UInt8]
        elif target_type == float:
            class TestModel(PolarsFastDataframeModel):
                value: float
        
        df = pl.DataFrame({"value": invalid_values})
        with pytest.raises(pl.exceptions.InvalidOperationError):
            TestModel.cast(df)


class TestStringToTemporalCasting:
    """Test string casting to temporal types."""

    def test_string_to_datetime_default_format(self):
        """Test string to datetime conversion with default format."""
        class TestModel(PolarsFastDataframeModel):
            timestamp: dt.datetime
        
        df = pl.DataFrame({
            "timestamp": ["2023-01-01T10:30:00", "2023-12-31T23:59:59"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["timestamp"] == pl.Datetime("us")
        values = df_collected["timestamp"].to_list()
        assert values[0] == dt.datetime(2023, 1, 1, 10, 30, 0)
        assert values[1] == dt.datetime(2023, 12, 31, 23, 59, 59)

    def test_string_to_datetime_custom_format(self):
        """Test string to datetime conversion with custom format."""
        class TestModel(PolarsFastDataframeModel):
            timestamp: Annotated[dt.datetime, ColumnInfo(date_format="%Y/%m/%d %H:%M")]
        
        df = pl.DataFrame({
            "timestamp": ["2023/01/01 10:30", "2023/12/31 23:59"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["timestamp"] == pl.Datetime("us")
        values = df_collected["timestamp"].to_list()
        assert values[0] == dt.datetime(2023, 1, 1, 10, 30, 0)
        assert values[1] == dt.datetime(2023, 12, 31, 23, 59, 0)

    def test_string_to_time_default_format(self):
        """Test string to time conversion with default format."""
        class TestModel(PolarsFastDataframeModel):
            time_value: dt.time
        
        df = pl.DataFrame({
            "time_value": ["10:30:45", "23:59:59"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["time_value"] == pl.Time
        values = df_collected["time_value"].to_list()
        assert values[0] == dt.time(10, 30, 45)
        assert values[1] == dt.time(23, 59, 59)

    def test_string_to_time_custom_format(self):
        """Test string to time conversion with custom format.""" 
        class TestModel(PolarsFastDataframeModel):
            time_value: Annotated[dt.time, ColumnInfo(date_format="%H:%M")]
        
        df = pl.DataFrame({
            "time_value": ["10:30", "23:59"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["time_value"] == pl.Time
        values = df_collected["time_value"].to_list()
        assert values[0] == dt.time(10, 30, 0)
        assert values[1] == dt.time(23, 59, 0)

    def test_string_to_duration(self):
        """Test string to duration conversion."""
        class TestModel(PolarsFastDataframeModel):
            duration: dt.timedelta
        
        df = pl.DataFrame({
            "duration": ["1d 2h 3m", "5h 30m", "45s"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["duration"] == pl.Duration("us")

    @pytest.mark.parametrize(
        "field_type,invalid_values",
        [
            (dt.datetime, ["invalid-date", "2023-13-01", ""]),
            (dt.time, ["25:00:00", "invalid-time", ""]),
            (dt.timedelta, ["invalid-duration", ""]),
        ]
    )
    def test_string_to_temporal_casting_errors(self, field_type, invalid_values):
        """Test that invalid temporal strings raise errors during casting."""
        if field_type == dt.datetime:
            class TestModel(PolarsFastDataframeModel):
                value: dt.datetime
        elif field_type == dt.time:
            class TestModel(PolarsFastDataframeModel):
                value: dt.time
        elif field_type == dt.timedelta:
            class TestModel(PolarsFastDataframeModel):
                value: dt.timedelta
        
        df = pl.DataFrame({"value": invalid_values})
        with pytest.raises(pl.exceptions.InvalidOperationError):
            TestModel.cast(df)


class TestStringToOtherTypesCasting:
    """Test string casting to other types."""

    def test_string_to_binary(self):
        """Test string to binary conversion."""
        class TestModel(PolarsFastDataframeModel):
            data: Annotated[bytes, pl.Binary]
        
        df = pl.DataFrame({
            "data": ["hello", "world", "binary"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["data"] == pl.Binary
        values = df_collected["data"].to_list()
        assert values[0] == b"hello"
        assert values[1] == b"world" 
        assert values[2] == b"binary"

    def test_string_to_categorical(self):
        """Test string to categorical conversion."""
        class TestModel(PolarsFastDataframeModel):
            category: Annotated[str, pl.Categorical]
        
        df = pl.DataFrame({
            "category": ["A", "B", "A", "C", "B"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["category"] == pl.Categorical
        values = df_collected["category"].to_list()
        assert values == ["A", "B", "A", "C", "B"]

    def test_string_to_decimal(self):
        """Test string to decimal conversion."""
        class TestModel(PolarsFastDataframeModel):
            amount: Annotated[Decimal, pl.Decimal(10, 2)]
        
        df = pl.DataFrame({
            "amount": ["123.45", "67.89", "999.99"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert isinstance(df_collected.schema["amount"], pl.Decimal)


class TestStringCastingEdgeCases:
    """Test edge cases for string casting."""

    def test_empty_strings(self):
        """Test handling of empty strings."""
        class TestModel(PolarsFastDataframeModel):
            value: str
        
        df = pl.DataFrame({
            "value": ["", "  ", "valid"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["value"] == pl.String
        values = df_collected["value"].to_list()
        assert values == ["", "  ", "valid"]

    def test_null_values_in_optional_fields(self):
        """Test handling of null values in optional fields."""
        from typing import Optional
        
        class TestModel(PolarsFastDataframeModel):
            value: Optional[int] = None
        
        df = pl.DataFrame({
            "value": ["1", None, "3"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["value"] == pl.Int64
        values = df_collected["value"].to_list()
        assert values == [1, None, 3]

    def test_whitespace_handling(self):
        """Test handling of strings with leading/trailing whitespace."""
        class TestModel(PolarsFastDataframeModel):
            value: int
        
        df = pl.DataFrame({
            "value": [" 1 ", "  2", "3  "]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["value"] == pl.Int64
        values = df_collected["value"].to_list()
        assert values == [1, 2, 3]

    def test_scientific_notation_floats(self):
        """Test handling of scientific notation in float strings."""
        class TestModel(PolarsFastDataframeModel):
            value: float
        
        df = pl.DataFrame({
            "value": ["1e2", "2.5e-3", "3.14e+1"]
        })
        result = TestModel.cast(df)
        df_collected = result.collect() if isinstance(result, pl.LazyFrame) else result
        
        assert df_collected.schema["value"] == pl.Float64
        values = df_collected["value"].to_list()
        assert values == [100.0, 0.0025, 31.4]