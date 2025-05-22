"""Polars integration for FastDataframe."""

try:
    import polars as pl  # noqa: F401
except ImportError as e:
    raise ImportError(
        "Polars package is not available. Please install it using 'pip install fastdataframe[polars]'"
    ) from e

from .model import PolarsFastDataframeModel

__all__ = ["PolarsFastDataframeModel"]
