"""Tests for FastDataframe metadata implementation."""

import dataclasses
import pytest
from fastdataframe import ColumnInfo


def test_fastdataframe_default_values() -> None:
    """Test that FastDataframe has correct default values."""
    metadata = ColumnInfo()
    assert metadata.is_unique is False


def test_fastdataframe_custom_values() -> None:
    """Test that FastDataframe accepts custom values."""
    metadata = ColumnInfo(is_unique=True)
    assert metadata.is_unique is True


def test_fastdataframe_schema_generation() -> None:
    """Test that FastDataframe generates correct schema."""
    metadata = ColumnInfo(is_unique=True)
    schema = metadata.__get_pydantic_core_schema__(int, lambda x: {"type": "integer"})

    # Check that json_schema_extra contains our metadata
    assert "json_schema_extra" in schema
    extra = schema["json_schema_extra"]
    assert extra["is_unique"] is True

    # Check that _fastdataframe document is present and correct
    assert "_fastdataframe" in extra
    doc = extra["_fastdataframe"]
    assert doc["type"] == "FastDataframe"
    assert doc["version"] == "1.0"
    assert doc["properties"] == {"is_unique": True}


def test_fastdataframe_from_schema() -> None:
    """Test that FastDataframe can be reconstructed from schema."""
    # Create a schema with FastDataframe metadata
    schema = {
        "json_schema_extra": {
            "is_unique": True,
            "_fastdataframe": {
                "type": "FastDataframe",
                "version": "1.0",
                "properties": {"is_unique": True},
            },
        }
    }

    # Reconstruct FastDataframe from schema
    metadata = ColumnInfo.from_schema(schema)
    assert metadata.is_unique is True


def test_fastdataframe_from_schema_invalid_type() -> None:
    """Test that FastDataframe.from_schema raises error for invalid type."""
    schema = {
        "json_schema_extra": {
            "_fastdataframe": {
                "type": "InvalidType",
                "version": "1.0",
                "properties": {"is_unique": True},
            }
        }
    }

    with pytest.raises(
        ValueError, match="Schema does not contain FastDataframe information"
    ):
        ColumnInfo.from_schema(schema)


def test_fastdataframe_from_schema_invalid_version() -> None:
    """Test that FastDataframe.from_schema raises error for invalid version."""
    schema = {
        "json_schema_extra": {
            "_fastdataframe": {
                "type": "FastDataframe",
                "version": "2.0",
                "properties": {"is_unique": True},
            }
        }
    }

    with pytest.raises(ValueError, match="Unsupported FastDataframe version: 2.0"):
        ColumnInfo.from_schema(schema)


def test_fastdataframe_from_schema_missing_properties() -> None:
    """Test that FastDataframe.from_schema raises error for missing properties."""
    schema = {
        "json_schema_extra": {
            "_fastdataframe": {
                "type": "FastDataframe",
                "version": "1.0",
                "properties": {
                    # is_unique is missing
                },
            }
        }
    }

    with pytest.raises(ValueError, match="Missing required properties: {'is_unique'}"):
        ColumnInfo.from_schema(schema)


def test_fastdataframe_immutability() -> None:
    """Test that FastDataframe instances are immutable."""
    metadata = ColumnInfo()

    with pytest.raises(dataclasses.FrozenInstanceError):
        metadata.is_unique = True  # type: ignore[misc]
