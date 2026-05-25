"""Column definitions and name accessors for FastDataFrame models."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Iterator, Mapping

from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from fastdataframe.core.annotation import ColumnInfo
from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)
from fastdataframe.core.types_helper import is_optional_type


@dataclass(frozen=True)
class ColumnDefinition:
    """Backend-neutral normalized representation of a dataframe column."""

    python_name: str
    validation_name: str
    serialization_name: str
    storage_name: str
    annotation: Any
    nullable: bool
    required_input: bool
    has_default: bool
    default: Any
    field_info: FieldInfo
    info: ColumnInfo

    @property
    def deprecated(self) -> bool:
        """Whether this field is still present but deprecated."""
        return self.info.deprecated


class NameAccessor(Mapping[str, str]):
    """Immutable accessor for resolved column names.

    Values are keyed by Python field name and available through both attribute and
    item access where possible.
    """

    _names: Mapping[str, str]

    def __init__(self, names: Mapping[str, str]) -> None:
        object.__setattr__(self, "_names", MappingProxyType(dict(names)))

    def __getitem__(self, key: str) -> str:
        return self._names[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)

    def __getattr__(self, name: str) -> str:
        try:
            return self._names[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: str) -> None:
        raise AttributeError("NameAccessor is immutable")

    def __repr__(self) -> str:
        return f"NameAccessor({dict(self._names)!r})"


def get_column_info(field_info: FieldInfo) -> ColumnInfo:
    """Extract ColumnInfo metadata from a Pydantic field, or return defaults."""
    for metadata in field_info.metadata:
        if isinstance(metadata, ColumnInfo):
            return metadata
    return ColumnInfo.from_field_type(field_info)


def build_column_definition(field_name: str, field_info: FieldInfo) -> ColumnDefinition:
    """Build and validate one ColumnDefinition from a Pydantic field."""
    column_info = get_column_info(field_info)
    annotation = field_info.annotation
    nullable = is_optional_type(annotation)
    has_default = field_info.default is not PydanticUndefined
    has_default_factory = field_info.default_factory is not None
    required_input = not has_default and not has_default_factory

    if column_info.deprecated and not nullable:
        raise ValueError(f"Deprecated field '{field_name}' must be nullable")

    if column_info.dtype is not None and not column_info.dtype.is_compatible_annotation(
        annotation
    ):
        raise ValueError(
            f"Column '{field_name}' dtype {column_info.dtype!r} is not compatible "
            f"with annotation {annotation!r}"
        )

    serialization_name = get_serialization_alias(field_info, field_name)
    validation_name = get_validation_alias(field_info, field_name)

    return ColumnDefinition(
        python_name=field_name,
        validation_name=validation_name,
        serialization_name=serialization_name,
        storage_name=serialization_name,
        annotation=annotation,
        nullable=nullable,
        required_input=required_input,
        has_default=has_default or has_default_factory,
        default=field_info.default,
        field_info=field_info,
        info=column_info,
    )


__all__ = [
    "ColumnDefinition",
    "NameAccessor",
    "build_column_definition",
    "get_column_info",
]
