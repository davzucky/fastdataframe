"""Iceberg integration for FastDataFrame."""

from .model import (
    IcebergFastDataframeModel,
    append_polars,
    apply_additive_migration,
    schema,
    validate_schema,
)

__all__ = [
    "IcebergFastDataframeModel",
    "append_polars",
    "apply_additive_migration",
    "schema",
    "validate_schema",
]
