"""FastDataFrame model implementation."""

from __future__ import annotations

from types import MappingProxyType
from collections.abc import Callable
from typing import Any, ClassVar, Generic, Literal, Mapping, Type, TypeVar, cast

from pydantic import BaseModel, ConfigDict, create_model
from pydantic.fields import FieldInfo

from fastdataframe.core.column import (
    ColumnDefinition,
    NameAccessor,
    build_column_definition,
    get_column_info,
)
from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)

from .annotation import ColumnInfo

T = TypeVar("T", bound="FastDataFrameModel")
TBaseModel = TypeVar("TBaseModel", bound=BaseModel)
AliasType = Literal["serialization", "validation"]
NameType = Literal["storage", "serialization", "validation", "python"]


TValue = TypeVar("TValue")


class classproperty(Generic[TValue]):
    """Read-only class-level property descriptor."""

    def __init__(self, func: Callable[[type[Any]], TValue]) -> None:
        self.func = func

    def __get__(self, instance: object, owner: type | None = None) -> TValue:
        if owner is None:
            owner = type(instance)
        return self.func(owner)


def _get_column_info(field_info: FieldInfo) -> ColumnInfo:
    """Backward-compatible helper for extracting ColumnInfo."""
    return get_column_info(field_info)


class FastDataFrameModel(BaseModel):
    """Base model that owns FastDataFrame column definitions."""

    model_config = ConfigDict(ignored_types=(classproperty,))

    __fastdataframe_column_definitions__: ClassVar[
        tuple[ColumnDefinition, ...] | None
    ] = None
    __fastdataframe_column_map__: ClassVar[Mapping[str, ColumnDefinition] | None] = None
    __fastdataframe_serialization_names__: ClassVar[NameAccessor | None] = None
    __fastdataframe_validation_names__: ClassVar[NameAccessor | None] = None
    __fastdataframe_storage_names__: ClassVar[NameAccessor | None] = None
    __fastdataframe_python_names__: ClassVar[NameAccessor | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        eager_columns = bool(kwargs.pop("eager_columns", False))
        super().__init_subclass__(**kwargs)
        cls._clear_fastdataframe_cache()
        config_eager = bool(
            getattr(cls, "model_config", {}).get("fastdataframe_eager_columns", False)
        )
        if eager_columns or config_eager:
            cls._build_column_definitions()

    @classmethod
    def model_rebuild(cls, *args: Any, **kwargs: Any) -> bool | None:
        """Rebuild the Pydantic model and clear derived FastDataFrame metadata."""
        result = super().model_rebuild(*args, **kwargs)
        cls._clear_fastdataframe_cache()
        return result

    @classmethod
    def _clear_fastdataframe_cache(cls) -> None:
        cls.__fastdataframe_column_definitions__ = None
        cls.__fastdataframe_column_map__ = None
        cls.__fastdataframe_serialization_names__ = None
        cls.__fastdataframe_validation_names__ = None
        cls.__fastdataframe_storage_names__ = None
        cls.__fastdataframe_python_names__ = None

    @classmethod
    def _configured_names(cls, key: str) -> frozenset[str]:
        raw_value = getattr(cls, "model_config", {}).get(key, frozenset())
        return frozenset(raw_value or frozenset())

    @classmethod
    def deprecated_column_names(cls) -> frozenset[str]:
        """Column names removed from the model but reserved from reuse."""
        return cls._configured_names("fastdataframe_deprecated_column_names")

    @classmethod
    def removed_column_names(cls) -> frozenset[str]:
        """Column names eligible for explicit destructive backend deletion."""
        return cls._configured_names("fastdataframe_removed_column_names")

    @classmethod
    def _build_column_definitions(cls) -> tuple[ColumnDefinition, ...]:
        columns = tuple(
            build_column_definition(field_name, field_info)
            for field_name, field_info in cls.model_fields.items()
        )

        seen_storage_names: set[str] = set()
        for column in columns:
            if column.storage_name in seen_storage_names:
                raise ValueError(
                    f"Duplicate storage column name: {column.storage_name}"
                )
            seen_storage_names.add(column.storage_name)

        reserved_names = cls.deprecated_column_names() | cls.removed_column_names()
        reused_reserved = seen_storage_names & reserved_names
        if reused_reserved:
            names = ", ".join(sorted(reused_reserved))
            raise ValueError(f"Reserved column names cannot be reused: {names}")

        column_map = {column.storage_name: column for column in columns}
        cls.__fastdataframe_column_definitions__ = columns
        cls.__fastdataframe_column_map__ = MappingProxyType(column_map)
        cls.__fastdataframe_serialization_names__ = NameAccessor(
            {column.python_name: column.serialization_name for column in columns}
        )
        cls.__fastdataframe_validation_names__ = NameAccessor(
            {column.python_name: column.validation_name for column in columns}
        )
        cls.__fastdataframe_storage_names__ = NameAccessor(
            {column.python_name: column.storage_name for column in columns}
        )
        cls.__fastdataframe_python_names__ = NameAccessor(
            {column.python_name: column.python_name for column in columns}
        )
        return columns

    @classproperty
    def column_definitions(cls) -> tuple[ColumnDefinition, ...]:
        """Ordered immutable ColumnDefinitions for this model."""
        if cls.__fastdataframe_column_definitions__ is None:
            return cls._build_column_definitions()
        return cls.__fastdataframe_column_definitions__

    @classproperty
    def column_map(cls) -> Mapping[str, ColumnDefinition]:
        """Read-only mapping from storage name to ColumnDefinition."""
        if cls.__fastdataframe_column_map__ is None:
            cls._build_column_definitions()
        assert cls.__fastdataframe_column_map__ is not None
        return cls.__fastdataframe_column_map__

    @classproperty
    def serialization_names(cls) -> NameAccessor:
        """Resolved serialization names keyed by Python field name."""
        if cls.__fastdataframe_serialization_names__ is None:
            cls._build_column_definitions()
        assert cls.__fastdataframe_serialization_names__ is not None
        return cls.__fastdataframe_serialization_names__

    @classproperty
    def validation_names(cls) -> NameAccessor:
        """Resolved validation names keyed by Python field name."""
        if cls.__fastdataframe_validation_names__ is None:
            cls._build_column_definitions()
        assert cls.__fastdataframe_validation_names__ is not None
        return cls.__fastdataframe_validation_names__

    @classproperty
    def storage_names(cls) -> NameAccessor:
        """Resolved storage names keyed by Python field name."""
        if cls.__fastdataframe_storage_names__ is None:
            cls._build_column_definitions()
        assert cls.__fastdataframe_storage_names__ is not None
        return cls.__fastdataframe_storage_names__

    @classproperty
    def python_names(cls) -> NameAccessor:
        """Python field names keyed by Python field name."""
        if cls.__fastdataframe_python_names__ is None:
            cls._build_column_definitions()
        assert cls.__fastdataframe_python_names__ is not None
        return cls.__fastdataframe_python_names__

    @classmethod
    def from_base_model(cls: Type[T], model: type[TBaseModel]) -> type[T]:
        """Create a schema-only FastDataFrame model from a Pydantic model."""
        field_definitions = {
            field_name: (field_type.annotation, field_type)
            for field_name, field_type in model.model_fields.items()
        }
        new_model = create_model(  # type: ignore[no-matching-overload]
            f"{model.__name__}FastDataFrame",
            __base__=cls,
            __doc__=f"FastDataFrame version of {model.__name__}",
            **field_definitions,
        )
        return cast(type[T], new_model)

    @classmethod
    def model_columns(
        cls, alias_type: AliasType = "serialization"
    ) -> dict[str, ColumnInfo]:
        """Extract column information with backwards-compatible alias support."""
        name_attr = (
            "serialization_name" if alias_type == "serialization" else "validation_name"
        )
        return {
            getattr(column, name_attr): column.info for column in cls.column_definitions
        }

    @classmethod
    def columns_by_name(
        cls, name_type: NameType = "storage"
    ) -> dict[str, ColumnDefinition]:
        """Return ColumnDefinitions keyed by a selected resolved name."""
        if name_type == "storage":
            return {column.storage_name: column for column in cls.column_definitions}
        if name_type == "serialization":
            return {
                column.serialization_name: column for column in cls.column_definitions
            }
        if name_type == "validation":
            return {column.validation_name: column for column in cls.column_definitions}
        return {column.python_name: column for column in cls.column_definitions}


# Backwards-compatible spelling used by the existing package.
FastDataframeModel = FastDataFrameModel


__all__ = [
    "AliasType",
    "FastDataFrameModel",
    "FastDataframeModel",
    "NameType",
    "_get_column_info",
    "get_serialization_alias",
    "get_validation_alias",
]
