from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Annotated

import pytest
from pydantic import BaseModel, Field

from fastdataframe import ColumnInfo, FastDataFrameModel, Int16, Int32, String


class TestColumnDefinitions:
    def test_model_owns_immutable_column_definitions(self) -> None:
        class User(FastDataFrameModel):
            user_id: Annotated[int, Field(serialization_alias="user_id")]
            name: str

        columns = User.column_definitions

        assert isinstance(columns, tuple)
        assert columns[0].python_name == "user_id"
        assert columns[0].storage_name == "user_id"
        assert columns[0].nullable is False
        with pytest.raises(FrozenInstanceError):
            columns[0].python_name = "other"  # type: ignore[misc]

    def test_from_base_model_creates_backend_neutral_model(self) -> None:
        class User(BaseModel):
            user_id: Annotated[int, Field(serialization_alias="user_id")]

        FastUser = FastDataFrameModel.from_base_model(User)

        assert issubclass(FastUser, FastDataFrameModel)
        assert FastUser.__name__ == "UserFastDataFrame"
        assert not hasattr(User, "column_definitions")
        assert FastUser.column_definitions[0].storage_name == "user_id"

    def test_column_info_is_optional_and_defaulted(self) -> None:
        class User(FastDataFrameModel):
            user_id: int

        column = User.column_definitions[0]

        assert isinstance(column.info, ColumnInfo)
        assert column.info.dtype is None

    def test_dtype_must_be_compatible_with_annotation(self) -> None:
        class Valid(FastDataFrameModel):
            user_id: Annotated[int, ColumnInfo(dtype=Int32())]

        assert Valid.column_definitions[0].info.dtype == Int32()

        class Invalid(FastDataFrameModel):
            user_id: Annotated[str, ColumnInfo(dtype=Int32())]

        with pytest.raises(ValueError, match="not compatible"):
            _ = Invalid.column_definitions

    def test_defaults_and_nullability_are_separate(self) -> None:
        class User(FastDataFrameModel):
            required_non_null: int
            default_non_null: int = 0
            nullable_required: int | None
            nullable_default: int | None = None

        columns = {column.python_name: column for column in User.column_definitions}

        assert columns["required_non_null"].required_input is True
        assert columns["required_non_null"].nullable is False
        assert columns["default_non_null"].required_input is False
        assert columns["default_non_null"].nullable is False
        assert columns["nullable_required"].required_input is True
        assert columns["nullable_required"].nullable is True
        assert columns["nullable_default"].required_input is False
        assert columns["nullable_default"].nullable is True


class TestNameAccessors:
    def test_name_accessors_use_python_field_names(self) -> None:
        class User(FastDataFrameModel):
            user_id: Annotated[
                int,
                Field(validation_alias="userId", serialization_alias="USER_ID"),
            ]

        assert User.serialization_names.user_id == "USER_ID"
        assert User.validation_names.user_id == "userId"
        assert User.storage_names.user_id == "USER_ID"
        assert User.serialization_names["user_id"] == "USER_ID"

    def test_name_accessors_are_immutable(self) -> None:
        class User(FastDataFrameModel):
            user_id: int

        with pytest.raises(AttributeError):
            User.serialization_names.user_id = "other"  # type: ignore[misc]


class TestColumnLifecycle:
    def test_deprecated_field_must_be_nullable(self) -> None:
        class Valid(FastDataFrameModel):
            old_value: Annotated[int | None, ColumnInfo(deprecated=True)]

        assert Valid.column_definitions[0].deprecated is True
        assert Valid.serialization_names.old_value == "old_value"

        class Invalid(FastDataFrameModel):
            old_value: Annotated[int, ColumnInfo(deprecated=True)]

        with pytest.raises(ValueError, match="Deprecated field"):
            _ = Invalid.column_definitions

    def test_reserved_removed_names_cannot_be_reused(self) -> None:
        class DeprecatedName(FastDataFrameModel):
            model_config = {"fastdataframe_deprecated_column_names": {"old_value"}}
            old_value: int

        with pytest.raises(ValueError, match="Reserved column names"):
            _ = DeprecatedName.column_definitions

        class RemovedName(FastDataFrameModel):
            model_config = {"fastdataframe_removed_column_names": {"old_value"}}
            old_value: int

        with pytest.raises(ValueError, match="Reserved column names"):
            _ = RemovedName.column_definitions


def test_different_dtype_can_refine_same_python_annotation() -> None:
    class User(FastDataFrameModel):
        small: Annotated[int, ColumnInfo(dtype=Int16())]
        normal: Annotated[int, ColumnInfo(dtype=Int32())]
        label: Annotated[str, ColumnInfo(dtype=String())]

    columns = {column.python_name: column for column in User.column_definitions}

    assert columns["small"].info.dtype == Int16()
    assert columns["normal"].info.dtype == Int32()
    assert columns["label"].info.dtype == String()
