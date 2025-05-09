"""Tests for FastDataframe data types."""

import pytest
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Type, Union

from fastdataframe.core.datatype import (
    DataType,
    Decimal as FastDecimal,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Date,
    Datetime,
    Duration,
    Time,
    Array,
    List as FastList,
    Field,
    Struct,
    String,
    Categorical,
    Enum,
    Binary,
    Boolean,
    Null,
    Object,
    Unknown,
)

def test_numeric_types():
    """Test numeric data types."""
    # Decimal
    decimal_type = FastDecimal(precision=10, scale=2)
    assert decimal_type.to_python() == Decimal
    
    # Float types
    assert Float32().to_python() == float
    assert Float64().to_python() == float
    
    # Signed integer types
    assert Int8().to_python() == int
    assert Int16().to_python() == int
    assert Int32().to_python() == int
    assert Int64().to_python() == int
    assert Int128().to_python() == int
    
    # Unsigned integer types
    assert UInt8().to_python() == int
    assert UInt16().to_python() == int
    assert UInt32().to_python() == int
    assert UInt64().to_python() == int

def test_temporal_types():
    """Test temporal data types."""
    # Date
    assert Date().to_python() == date
    
    # Datetime
    datetime_type = Datetime(time_unit="us", time_zone="UTC")
    assert datetime_type.to_python() == datetime
    assert datetime_type.time_unit == "us"
    assert datetime_type.time_zone == "UTC"
    
    # Duration
    duration_type = Duration(time_unit="us")
    assert duration_type.to_python() == timedelta
    assert duration_type.time_unit == "us"
    
    # Time
    assert Time().to_python() == time

def test_nested_types():
    """Test nested data types."""
    # Array
    array_type = Array(inner=Int32(), shape=(2, 3), width=6)
    assert array_type.to_python() == list[int]
    assert array_type.shape == (2, 3)
    assert array_type.width == 6
    
    # List
    list_type = FastList(inner=String())
    assert list_type.to_python() == list[str]
    
    # Field
    field_type = Field(name="age", dtype=Int32())
    assert field_type.to_python() == Dict[str, int]
    assert field_type.name == "age"
    
    # Struct
    fields = [
        Field(name="age", dtype=Int32()),
        Field(name="name", dtype=String()),
    ]
    struct_type = Struct(fields=fields)
    assert struct_type.to_python() == Dict[str, Any]
    assert len(struct_type.fields) == 2

def test_string_types():
    """Test string data types."""
    # String
    assert String().to_python() == str
    
    # Categorical
    assert Categorical().to_python() == str
    
    # Enum
    enum_type = Enum(values=["A", "B", "C"])
    assert enum_type.to_python() == str
    assert enum_type.values == ["A", "B", "C"]

def test_other_types():
    """Test other data types."""
    # Binary
    assert Binary().to_python() == bytes
    
    # Boolean
    assert Boolean().to_python() == bool
    
    # Null
    assert Null().to_python() == type(None)
    
    # Object
    assert Object().to_python() == object
    
    # Unknown
    assert Unknown().to_python() == Any 