"""Backend-neutral dtype refinements for FastDataFrame columns."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal as PythonDecimal
from typing import Any, ClassVar


class Dtype:
    """Base class for backend-neutral logical column type refinements."""

    python_types: ClassVar[tuple[type[Any], ...]] = ()

    def is_compatible_annotation(self, annotation: Any) -> bool:
        """Return whether this dtype can refine the given Python annotation."""
        from fastdataframe.core.types_helper import unwrap_annotated_optional

        annotation = unwrap_annotated_optional(annotation)
        return isinstance(annotation, type) and issubclass(
            annotation, self.python_types
        )


@dataclass(frozen=True)
class Boolean(Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (bool,)


@dataclass(frozen=True)
class String(Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (str,)


@dataclass(frozen=True)
class Binary(Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (bytes,)


@dataclass(frozen=True)
class Int8(Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)


@dataclass(frozen=True)
class Int16(Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)


@dataclass(frozen=True)
class Int32(Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)


@dataclass(frozen=True)
class Int64(Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)


class StrictFloatCompatibility:
    """Mixin for dtypes that require strict float annotation identity."""

    def is_compatible_annotation(self, annotation: Any) -> bool:
        from fastdataframe.core.types_helper import unwrap_annotated_optional

        annotation = unwrap_annotated_optional(annotation)
        return annotation is float


@dataclass(frozen=True)
class Float32(StrictFloatCompatibility, Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (float,)


@dataclass(frozen=True)
class Float64(StrictFloatCompatibility, Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (float,)


@dataclass(frozen=True)
class Date(Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (dt.date,)

    def is_compatible_annotation(self, annotation: Any) -> bool:
        from fastdataframe.core.types_helper import unwrap_annotated_optional

        annotation = unwrap_annotated_optional(annotation)
        return annotation is dt.date


@dataclass(frozen=True)
class Time(Dtype):
    python_types: ClassVar[tuple[type[Any], ...]] = (dt.time,)


@dataclass(frozen=True)
class Timestamp(Dtype):
    timezone: str | None = None
    python_types: ClassVar[tuple[type[Any], ...]] = (dt.datetime,)


@dataclass(frozen=True)
class Decimal(Dtype):
    precision: int
    scale: int
    python_types: ClassVar[tuple[type[Any], ...]] = (PythonDecimal,)

    def __post_init__(self) -> None:
        if self.precision <= 0:
            raise ValueError("Decimal precision must be greater than zero")
        if self.scale < 0:
            raise ValueError("Decimal scale must be greater than or equal to zero")
        if self.scale > self.precision:
            raise ValueError("Decimal scale must be less than or equal to precision")


__all__ = [
    "Binary",
    "Boolean",
    "Date",
    "Decimal",
    "Dtype",
    "Float32",
    "Float64",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "String",
    "Time",
    "Timestamp",
]
