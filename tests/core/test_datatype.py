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

@pytest.mark.parametrize("dtype_class,expected_type,init_params", [
    # Decimal type
    (FastDecimal, Decimal, {"precision": 10, "scale": 2}),
    # Float types
    (Float32, float, {}),
    (Float64, float, {}),
    # Signed integer types
    (Int8, int, {}),
    (Int16, int, {}),
    (Int32, int, {}),
    (Int64, int, {}),
    (Int128, int, {}),
    # Unsigned integer types
    (UInt8, int, {}),
    (UInt16, int, {}),
    (UInt32, int, {}),
    (UInt64, int, {}),
])
def test_numeric_types(dtype_class, expected_type, init_params):
    """Test numeric data types.
    
    Args:
        dtype_class: The data type class to test
        expected_type: The expected Python type
        init_params: Parameters to initialize the data type class
    """
    dtype = dtype_class(**init_params)
    assert dtype.to_python() == expected_type

@pytest.mark.parametrize("dtype_class,expected_type,init_params,extra_checks", [
    # Date type
    (Date, date, {}, []),
    # Datetime type
    (Datetime, datetime, {"time_unit": "us", "time_zone": "UTC"}, [
        lambda dtype: dtype.time_unit == "us",
        lambda dtype: dtype.time_zone == "UTC"
    ]),
    # Duration type
    (Duration, timedelta, {"time_unit": "us"}, [
        lambda dtype: dtype.time_unit == "us"
    ]),
    # Time type
    (Time, time, {}, [])
])
def test_temporal_types(dtype_class, expected_type, init_params, extra_checks):
    """Test temporal data types.
    
    Args:
        dtype_class: The data type class to test
        expected_type: The expected Python type
        init_params: Parameters to initialize the data type class
        extra_checks: Additional assertions to run on the data type instance
    """
    dtype = dtype_class(**init_params)
    assert dtype.to_python() == expected_type
    
    # Run any additional checks
    for check in extra_checks:
        assert check(dtype)

@pytest.mark.parametrize("dtype_class,inner_type,init_params,expected_type,extra_checks", [
    # Array type
    (Array, Int32, {"shape": (2, 3), "width": 6}, list[int], [
        lambda dtype: dtype.shape == (2, 3),
        lambda dtype: dtype.width == 6
    ]),
    # List type
    (FastList, String, {}, list[str], []),
    # Field type
    (Field, Int32, {"name": "age", "dtype": Int32()}, Dict[str, int], [
        lambda dtype: dtype.name == "age"
    ]),
    # Struct type
    (Struct, None, {
        "fields": [
            Field(name="age", dtype=Int32()),
            Field(name="name", dtype=String())
        ]
    }, Dict[str, Any], [
        lambda dtype: len(dtype.fields) == 2
    ])
])
def test_nested_types(dtype_class, inner_type, init_params, expected_type, extra_checks):
    """Test nested data types.
    
    Args:
        dtype_class: The data type class to test
        inner_type: The inner type class for nested types
        init_params: Parameters to initialize the data type class
        expected_type: The expected Python type
        extra_checks: Additional assertions to run on the data type instance
    """
    if inner_type is not None and "dtype" not in init_params:
        init_params["inner"] = inner_type()
    dtype = dtype_class(**init_params)
    assert dtype.to_python() == expected_type
    
    # Run any additional checks
    for check in extra_checks:
        assert check(dtype)

@pytest.mark.parametrize("dtype_class,init_params,expected_type,extra_checks", [
    # String type
    (String, {}, str, []),
    # Categorical type
    (Categorical, {}, str, []),
    # Enum type
    (Enum, {"values": ["A", "B", "C"]}, str, [
        lambda dtype: dtype.values == ["A", "B", "C"]
    ])
])
def test_string_types(dtype_class, init_params, expected_type, extra_checks):
    """Test string data types.
    
    Args:
        dtype_class: The data type class to test
        init_params: Parameters to initialize the data type class
        expected_type: The expected Python type
        extra_checks: Additional assertions to run on the data type instance
    """
    dtype = dtype_class(**init_params)
    assert dtype.to_python() == expected_type
    
    # Run any additional checks
    for check in extra_checks:
        assert check(dtype)

@pytest.mark.parametrize("dtype_class,expected_type,init_params", [
    # Binary type
    (Binary, bytes, {}),
    # Boolean type
    (Boolean, bool, {}),
    # Null type
    (Null, type(None), {}),
    # Object type
    (Object, object, {}),
    # Unknown type
    (Unknown, Any, {})
])
def test_other_types(dtype_class, expected_type, init_params):
    """Test other data types.
    
    Args:
        dtype_class: The data type class to test
        expected_type: The expected Python type
        init_params: Parameters to initialize the data type class
    """
    dtype = dtype_class(**init_params)
    assert dtype.to_python() == expected_type 