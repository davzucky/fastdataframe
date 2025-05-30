"""Annotation classes for FastDataframe."""

from dataclasses import dataclass
from typing import Any, Self, cast
from pydantic._internal._fields import PydanticMetadata
from annotated_types import BaseMetadata


@dataclass(frozen=True)
class ColumnInfo(PydanticMetadata, BaseMetadata):
    """Custom annotation for FastDataframe fields.

    This annotation class is used to store additional information about fields
    that are used in FastDataframe operations.

    Attributes:
        is_unique: Whether the field values must be unique
    """

    is_unique: bool = False

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: Any
    ) -> dict[str, Any]:
        """Implement the core schema generation method.

        This method is called by Pydantic to generate the validation schema.
        We use json_schema_extra to store our metadata in a way that can be
        easily reconstructed.

        Args:
            source_type: The type being validated
            handler: The handler function for the type

        Returns:
            A dictionary containing the schema with our metadata
        """
        schema = cast(dict[str, Any], handler(source_type))
        # Add both the properties and a reconstruction document
        schema["json_schema_extra"] = {
            "is_unique": self.is_unique,
            # Add a document that can be used to reconstruct the FastDataframe
            "_fastdataframe": {
                "type": "FastDataframe",
                "version": "1.0",
                "properties": {
                    "is_unique": self.is_unique,
                },
            },
        }
        return schema

    def __get_pydantic_json_schema__(
        self, core_schema: dict[str, Any], handler: Any
    ) -> dict[str, Any]:
        """Implement the JSON schema generation method.

        This method is called by Pydantic to generate the JSON schema.
        We ensure our metadata is included in the schema.

        Args:
            core_schema: The core schema
            handler: The handler function for the type

        Returns:
            A dictionary containing the JSON schema with our metadata
        """
        json_schema = cast(dict[str, Any], handler(core_schema))
        if "json_schema_extra" in core_schema:
            json_schema.update(core_schema["json_schema_extra"])
        return json_schema

    @classmethod
    def from_field_type(cls, field_type: Any) -> Self:
        """Create a FastDataframe instance from field metadata.

        Args:
            field_type: The field type containing FastDataframe information

        Returns:
            A new FastDataframe instance
        """
        return cls()

    @classmethod
    def from_schema(cls, schema: dict[str, Any]) -> "ColumnInfo":
        """Create a FastDataframe instance from a schema.

        Args:
            schema: The JSON schema containing FastDataframe information

        Returns:
            A new FastDataframe instance

        Raises:
            ValueError: If the schema doesn't contain valid FastDataframe information
        """
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a dictionary")

        json_schema_extra = schema.get("json_schema_extra", {})
        fastdataframe_doc = json_schema_extra.get("_fastdataframe", {})

        if fastdataframe_doc.get("type") != "FastDataframe":
            raise ValueError("Schema does not contain FastDataframe information")

        version = fastdataframe_doc.get("version")
        if version != "1.0":
            raise ValueError(f"Unsupported FastDataframe version: {version}")

        properties = fastdataframe_doc.get("properties", {})
        if not isinstance(properties, dict):
            raise ValueError("Invalid properties in FastDataframe document")

        # Validate required properties
        required_props = {"is_unique"}
        if not all(prop in properties for prop in required_props):
            raise ValueError(
                f"Missing required properties: {required_props - set(properties.keys())}"
            )

        return cls(**properties)

    def as_field_metadata(self) -> dict[str, Any]:
        """Return a dictionary suitable for use as Pydantic Field(json_schema_extra=...)."""
        return {
            "_fastdataframe": {
                "type": "FastDataframe",
                "version": "1.0",
                "properties": {
                    "is_unique": self.is_unique,
                },
            },
            "is_unique": self.is_unique,
        }
