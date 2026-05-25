"""Annotation classes for FastDataframe."""

from dataclasses import dataclass
from typing import Any, Self, cast

from annotated_types import BaseMetadata
from pydantic._internal._fields import PydanticMetadata

from fastdataframe.core import dtypes as fdt
from fastdataframe.core.dtypes import Dtype


@dataclass(frozen=True)
class ColumnInfo(PydanticMetadata, BaseMetadata):
    """Custom annotation for FastDataframe fields.

    This annotation class is used to store additional information about fields
    that are used in FastDataframe operations.

    Attributes:
        is_unique: Whether the field values must be unique
    """

    is_unique: bool = False
    bool_true_string: str = "true"
    bool_false_string: str = "false"
    date_format: str = "%Y-%m-%d"
    dtype: Dtype | None = None
    deprecated: bool = False
    iceberg_id: int | None = None

    def _dtype_metadata(self) -> dict[str, Any] | None:
        if self.dtype is None:
            return None
        metadata: dict[str, Any] = {"name": type(self.dtype).__name__}
        if isinstance(self.dtype, fdt.Decimal):
            metadata.update(
                {"precision": self.dtype.precision, "scale": self.dtype.scale}
            )
        elif isinstance(self.dtype, fdt.Timestamp):
            metadata.update({"timezone": self.dtype.timezone})
        return metadata

    @staticmethod
    def _dtype_from_metadata(metadata: Any) -> Dtype | None:
        if metadata is None:
            return None
        if isinstance(metadata, Dtype):
            return metadata
        if not isinstance(metadata, dict):
            raise ValueError("Invalid dtype metadata")
        name = metadata.get("name")
        dtype_classes: dict[str, type[Dtype]] = {
            "Boolean": fdt.Boolean,
            "String": fdt.String,
            "Binary": fdt.Binary,
            "Int8": fdt.Int8,
            "Int16": fdt.Int16,
            "Int32": fdt.Int32,
            "Int64": fdt.Int64,
            "Float32": fdt.Float32,
            "Float64": fdt.Float64,
            "Date": fdt.Date,
            "Time": fdt.Time,
            "Timestamp": fdt.Timestamp,
            "Decimal": fdt.Decimal,
        }
        dtype_cls = dtype_classes.get(name)
        if dtype_cls is None:
            raise ValueError(f"Unknown dtype metadata: {name}")
        if dtype_cls is fdt.Decimal:
            return fdt.Decimal(
                precision=int(metadata["precision"]), scale=int(metadata["scale"])
            )
        if dtype_cls is fdt.Timestamp:
            return fdt.Timestamp(timezone=metadata.get("timezone"))
        return dtype_cls()

    def _metadata_properties(self) -> dict[str, Any]:
        return {
            "is_unique": self.is_unique,
            "bool_true_string": self.bool_true_string,
            "bool_false_string": self.bool_false_string,
            "date_format": self.date_format,
            "dtype": self._dtype_metadata(),
            "deprecated": self.deprecated,
            "iceberg_id": self.iceberg_id,
        }

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
        properties = self._metadata_properties()
        schema["json_schema_extra"] = {
            **properties,
            # Add a document that can be used to reconstruct the FastDataframe
            "_fastdataframe": {
                "type": "FastDataframe",
                "version": "1.0",
                "properties": properties,
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

        properties = dict(properties)
        properties["dtype"] = cls._dtype_from_metadata(properties.get("dtype"))
        return cls(**properties)

    @classmethod
    def from_field_metadata(cls, metadata: dict[str, Any]) -> "ColumnInfo":
        """Create a ColumnInfo from Field(json_schema_extra=...) metadata."""
        return cls.from_schema({"json_schema_extra": metadata})

    def as_field_metadata(self) -> dict[str, Any]:
        """Return a dictionary suitable for use as Pydantic Field(json_schema_extra=...)."""
        properties = self._metadata_properties()
        return {
            "_fastdataframe": {
                "type": "FastDataframe",
                "version": "1.0",
                "properties": properties,
            },
            **properties,
        }
