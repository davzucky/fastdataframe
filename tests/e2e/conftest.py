"""Fixtures for end-to-end tests."""

from pathlib import Path

import pytest
from pyiceberg.catalog import Catalog, load_in_memory


@pytest.fixture
def iceberg_catalog(tmp_path: Path) -> Catalog:
    """Provide an in-memory Iceberg catalog for testing.

    The catalog uses a temporary directory for warehouse storage,
    ensuring test isolation.

    Args:
        tmp_path: Pytest fixture providing a temporary directory path.

    Returns:
        Catalog: An in-memory Iceberg catalog with a test namespace.
    """
    catalog = load_in_memory(
        "test_catalog",
        {"warehouse": f"file://{tmp_path}/warehouse"},
    )
    catalog.create_namespace("test_db")
    return catalog
