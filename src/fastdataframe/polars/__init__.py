"""Polars integration for FastDataFrame."""

try:
    import polars as pl  # noqa: F401
except ImportError as e:
    raise ImportError(
        "Polars package is not available. Please install it using 'pip install fastdataframe[polars]'"
    ) from e

from .model import (
    PolarsFastDataframeModel,
    cast,
    rename,
    schema,
    string_schema,
    validate_schema,
)

__all__ = [
    "PolarsFastDataframeModel",
    "cast",
    "rename",
    "schema",
    "string_schema",
    "validate_schema",
]
