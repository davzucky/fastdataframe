"""Custom data types for FastDataframe.

This module provides custom data types that mirror the Polars data types.
Each data type has a to_python method that returns the corresponding Python type.
"""

from abc import ABC, abstractmethod
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Type, Union

class DataType(ABC):
    """Base class for all data types."""
    
    @abstractmethod
    def to_python(self) -> Type:
        """Convert to Python type."""
        pass

# Numeric types
class Decimal(DataType):
    """Decimal type with precision and scale."""
    
    def __init__(self, precision: int, scale: int):
        self.precision = precision
        self.scale = scale
    
    def to_python(self) -> Type[Decimal]:
        from decimal import Decimal as PyDecimal
        return PyDecimal

class Float32(DataType):
    """32-bit floating point number."""
    
    def to_python(self) -> Type[float]:
        return float

class Float64(DataType):
    """64-bit floating point number."""
    
    def to_python(self) -> Type[float]:
        return float

class Int8(DataType):
    """8-bit signed integer."""
    
    def to_python(self) -> Type[int]:
        return int

class Int16(DataType):
    """16-bit signed integer."""
    
    def to_python(self) -> Type[int]:
        return int

class Int32(DataType):
    """32-bit signed integer."""
    
    def to_python(self) -> Type[int]:
        return int

class Int64(DataType):
    """64-bit signed integer."""
    
    def to_python(self) -> Type[int]:
        return int

class Int128(DataType):
    """128-bit signed integer."""
    
    def to_python(self) -> Type[int]:
        return int

class UInt8(DataType):
    """8-bit unsigned integer."""
    
    def to_python(self) -> Type[int]:
        return int

class UInt16(DataType):
    """16-bit unsigned integer."""
    
    def to_python(self) -> Type[int]:
        return int

class UInt32(DataType):
    """32-bit unsigned integer."""
    
    def to_python(self) -> Type[int]:
        return int

class UInt64(DataType):
    """64-bit unsigned integer."""
    
    def to_python(self) -> Type[int]:
        return int

# Temporal types
class Date(DataType):
    """Date type."""
    
    def to_python(self) -> Type[date]:
        return date

class Datetime(DataType):
    """Datetime type with time unit and time zone."""
    
    def __init__(self, time_unit: str = "us", time_zone: Optional[str] = None):
        self.time_unit = time_unit
        self.time_zone = time_zone
    
    def to_python(self) -> Type[datetime]:
        return datetime

class Duration(DataType):
    """Duration type with time unit."""
    
    def __init__(self, time_unit: str = "us"):
        self.time_unit = time_unit
    
    def to_python(self) -> Type[timedelta]:
        return timedelta

class Time(DataType):
    """Time type."""
    
    def to_python(self) -> Type[time]:
        return time

# Nested types
class Array(DataType):
    """Array type with inner type, shape, and width."""
    
    def __init__(self, inner: DataType, shape: Optional[tuple[int, ...]] = None, width: Optional[int] = None):
        self.inner = inner
        self.shape = shape
        self.width = width
    
    def to_python(self) -> Type[list]:
        return list[self.inner.to_python()]

class List(DataType):
    """List type with inner type."""
    
    def __init__(self, inner: DataType):
        self.inner = inner
    
    def to_python(self) -> Type[list]:
        return list[self.inner.to_python()]

class Field(DataType):
    """Field type with name and data type."""
    
    def __init__(self, name: str, dtype: DataType):
        self.name = name
        self.dtype = dtype
    
    def to_python(self) -> Type[Dict]:
        return Dict[str, self.dtype.to_python()]

class Struct(DataType):
    """Struct type with fields."""
    
    def __init__(self, fields: list[Field]):
        self.fields = fields
    
    def to_python(self) -> Type[Dict]:
        return Dict[str, Any]

# String types
class String(DataType):
    """String type."""
    
    def to_python(self) -> Type[str]:
        return str

class Categorical(DataType):
    """Categorical type."""
    
    def to_python(self) -> Type[str]:
        return str

class Enum(DataType):
    """Enum type with values."""
    
    def __init__(self, values: list[str]):
        self.values = values
    
    def to_python(self) -> Type[str]:
        return str

# Other types
class Binary(DataType):
    """Binary type."""
    
    def to_python(self) -> Type[bytes]:
        return bytes

class Boolean(DataType):
    """Boolean type."""
    
    def to_python(self) -> Type[bool]:
        return bool

class Null(DataType):
    """Null type."""
    
    def to_python(self) -> Type[None]:
        return type(None)

class Object(DataType):
    """Object type."""
    
    def to_python(self) -> Type[object]:
        return object

class Unknown(DataType):
    """Unknown type."""
    
    def to_python(self) -> Type[Any]:
        return Any

# Union type of all data types
fdtypes = Union[
    Decimal,
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
    List,
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
]

__all__ = [
    "DataType",
    "Decimal",
    "Float32",
    "Float64",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Date",
    "Datetime",
    "Duration",
    "Time",
    "Array",
    "List",
    "Field",
    "Struct",
    "String",
    "Categorical",
    "Enum",
    "Binary",
    "Boolean",
    "Null",
    "Object",
    "Unknown",
    "fdtypes",
] 