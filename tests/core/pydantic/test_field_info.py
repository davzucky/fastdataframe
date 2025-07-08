import pytest
from pydantic import AliasChoices, AliasPath
from pydantic.fields import FieldInfo
from fastdataframe.core.pydantic.field_info import (
    get_serialization_alias,
    get_validation_alias,
)


class TestGetSerializationAlias:
    @pytest.mark.parametrize(
        "serialization_alias,alias,field_name,expected",
        [
            ("ser", None, "foo", "ser"),
            (None, "alias", "foo", "alias"),
            (None, None, "foo", "foo"),
        ],
    )
    def test_get_serialization_alias(
        self,
        serialization_alias: str | None,
        alias: str | None,
        field_name: str,
        expected: str,
    ) -> None:
        field_info = FieldInfo(serialization_alias=serialization_alias, alias=alias)
        assert get_serialization_alias(field_info, field_name) == expected


class TestGetValidationAlias:
    @pytest.mark.parametrize(
        "validation_alias,alias,field_name,expected",
        [
            ("val", None, "foo", "val"),
            (None, "alias", "foo", "alias"),
            (None, None, "foo", "foo"),
        ],
    )
    def test_get_validation_alias_string(
        self,
        validation_alias: str | None,
        alias: str | None,
        field_name: str,
        expected: str,
    ) -> None:
        field_info = FieldInfo(validation_alias=validation_alias, alias=alias)
        assert get_validation_alias(field_info, field_name) == expected

    def test_get_validation_alias_list_and_serialization(self) -> None:
        # validation_alias is AliasChoices with two aliases, one is serialization_alias
        field_info = FieldInfo(
            validation_alias=AliasChoices(AliasPath("val1"), AliasPath("val2")),
            serialization_alias="val1",
        )
        # Should return the non-serialization alias
        assert get_validation_alias(field_info, "foo") == "val2"

    def test_get_validation_alias_list_and_serialization_single(self) -> None:
        # validation_alias is AliasChoices with one alias, not serialization_alias
        field_info = FieldInfo(
            validation_alias=AliasChoices(AliasPath("val2")),
            serialization_alias="val1",
        )
        # Should return the only alias
        assert get_validation_alias(field_info, "foo") == "val2"

    def test_get_validation_alias_list_and_serialization_empty(self) -> None:
        # validation_alias is AliasChoices with only serialization_alias
        field_info = FieldInfo(
            validation_alias=AliasChoices(AliasPath("val1")),
            serialization_alias="val1",
        )
        # Should return serialization_alias
        assert get_validation_alias(field_info, "foo") == "val1"

    def test_get_validation_alias_invalid(self) -> None:
        # validation_alias is AliasChoices with more than one non-serialization alias
        field_info = FieldInfo(
            validation_alias=AliasChoices(AliasPath("val2"), AliasPath("val3")),
            serialization_alias="val1",
        )
        with pytest.raises(ValueError):
            get_validation_alias(field_info, "foo")
