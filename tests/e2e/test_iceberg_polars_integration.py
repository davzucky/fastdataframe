"""End-to-end test: Pydantic -> Polars -> Iceberg -> Polars.

This module tests the complete integration flow:
1. Define a Pydantic model with optional columns
2. Generate an Iceberg schema from the model
3. Create a Polars DataFrame with test data
4. Write the DataFrame to an Iceberg table
5. Read the Iceberg table back to Polars
6. Verify data integrity
"""

from typing import Optional

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

from fastdataframe.iceberg.model import IcebergFastDataframeModel


class ProductModel(IcebergFastDataframeModel):
    """Test model with optional columns for E2E testing."""

    id: int
    name: str
    price: float
    description: Optional[str] = None
    quantity: Optional[int] = None
    is_active: bool


class TestIcebergPolarsIntegration:
    """E2E tests for Pydantic model to Iceberg table via Polars."""

    def test_full_roundtrip_with_optional_columns(
        self, iceberg_catalog: Catalog
    ) -> None:
        """Test complete flow: model -> polars -> iceberg -> polars.

        This test verifies:
        - Iceberg schema generation from Pydantic model
        - Table creation with the generated schema
        - Writing Polars DataFrame to Iceberg table
        - Reading data back from Iceberg to Polars
        - Data integrity including null values in optional columns
        """
        # 1. Generate Iceberg schema from Pydantic model
        iceberg_schema = ProductModel.iceberg_schema()

        # Verify schema has expected fields
        field_names = {field.name for field in iceberg_schema.fields}
        assert field_names == {
            "id",
            "name",
            "price",
            "description",
            "quantity",
            "is_active",
        }

        # Verify optional fields are not required
        fields_by_name = {field.name: field for field in iceberg_schema.fields}
        assert fields_by_name["description"].required is False
        assert fields_by_name["quantity"].required is False
        assert fields_by_name["id"].required is True
        assert fields_by_name["name"].required is True

        # 2. Create Iceberg table using the fixture-provided catalog
        table = iceberg_catalog.create_table(
            "test_db.products",
            schema=iceberg_schema,
        )

        # 3. Get PyArrow schema from Iceberg schema for proper type compatibility
        pa_schema = schema_to_pyarrow(iceberg_schema)

        # 4. Create Polars DataFrame with test data (including nulls for optional)
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Widget", "Gadget", "Gizmo"],
                "price": [9.99, 19.99, 29.99],
                "description": ["A widget", None, "A gizmo"],
                "quantity": [100, None, 50],
                "is_active": [True, True, False],
            }
        )

        # 5. Convert to PyArrow with the correct schema for Iceberg compatibility
        arrow_table = df.to_arrow().cast(pa_schema)

        # 6. Write to Iceberg table
        table.append(arrow_table)

        # 7. Read Iceberg table back to Polars
        df_read = pl.from_arrow(table.scan().to_arrow())

        # 8. Assert data integrity
        # Check shape matches
        assert df_read.shape == df.shape

        # Sort both DataFrames by id for consistent comparison
        df_sorted = df.sort("id")
        df_read_sorted = df_read.sort("id")

        # Verify all columns match
        assert df_read_sorted["id"].to_list() == df_sorted["id"].to_list()
        assert df_read_sorted["name"].to_list() == df_sorted["name"].to_list()
        assert df_read_sorted["price"].to_list() == df_sorted["price"].to_list()
        assert (
            df_read_sorted["is_active"].to_list() == df_sorted["is_active"].to_list()
        )

        # Verify null handling for optional columns
        assert (
            df_read_sorted["description"].to_list()
            == df_sorted["description"].to_list()
        )
        assert df_read_sorted["quantity"].to_list() == df_sorted["quantity"].to_list()

        # Explicitly check that nulls are preserved
        assert df_read_sorted["description"][1] is None
        assert df_read_sorted["quantity"][1] is None

    def test_empty_dataframe_roundtrip(self, iceberg_catalog: Catalog) -> None:
        """Test roundtrip with an empty DataFrame."""
        iceberg_schema = ProductModel.iceberg_schema()

        table = iceberg_catalog.create_table(
            "test_db.empty_products",
            schema=iceberg_schema,
        )

        # Get PyArrow schema for proper type compatibility
        pa_schema = schema_to_pyarrow(iceberg_schema)

        # Create empty DataFrame with correct schema using PyArrow
        empty_arrow = pa.table(
            {
                "id": pa.array([], type=pa.int32()),
                "name": pa.array([], type=pa.string()),
                "price": pa.array([], type=pa.float64()),
                "description": pa.array([], type=pa.string()),
                "quantity": pa.array([], type=pa.int32()),
                "is_active": pa.array([], type=pa.bool_()),
            },
            schema=pa_schema,
        )

        table.append(empty_arrow)

        df_read = pl.from_arrow(table.scan().to_arrow())

        assert df_read.shape[0] == 0
        expected_columns = {"id", "name", "price", "description", "quantity", "is_active"}
        assert set(df_read.columns) == expected_columns

    def test_all_optional_columns_null(self, iceberg_catalog: Catalog) -> None:
        """Test with all optional columns containing null values."""
        iceberg_schema = ProductModel.iceberg_schema()

        table = iceberg_catalog.create_table(
            "test_db.all_nulls",
            schema=iceberg_schema,
        )

        # Get PyArrow schema for proper type compatibility
        pa_schema = schema_to_pyarrow(iceberg_schema)

        # Create DataFrame with nulls in optional columns
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["Item1", "Item2"],
                "price": [10.0, 20.0],
                "description": pl.Series([None, None], dtype=pl.Utf8),
                "quantity": pl.Series([None, None], dtype=pl.Int64),
                "is_active": [True, False],
            }
        )

        # Convert to PyArrow with proper schema
        arrow_table = df.to_arrow().cast(pa_schema)
        table.append(arrow_table)

        df_read = pl.from_arrow(table.scan().to_arrow())

        # All optional columns should be null
        assert df_read["description"].null_count() == 2
        assert df_read["quantity"].null_count() == 2

    def test_schema_validation_matches_table(self, iceberg_catalog: Catalog) -> None:
        """Test that model's validate_schema works with created table."""
        iceberg_schema = ProductModel.iceberg_schema()

        table = iceberg_catalog.create_table(
            "test_db.validated_products",
            schema=iceberg_schema,
        )

        # Validate that the table schema matches the model
        errors = ProductModel.validate_schema(table)
        assert len(errors) == 0, f"Unexpected validation errors: {errors}"
