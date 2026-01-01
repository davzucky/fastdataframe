"""Test cases for the validate method in PolarsFastDataframeModel."""

import polars as pl
import pytest
from typing import Optional

from fastdataframe.polars.model import PolarsFastDataframeModel


class TestDataValidation:
    """Test data validation functionality."""

    def test_validate_no_errors(self):
        """Test validation with no errors - all required fields have values."""

        class UserModel(PolarsFastDataframeModel):
            id: int
            name: str
            email: Optional[str] = None

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "email": [None, "bob@example.com", "charlie@example.com"],
            }
        )

        result = UserModel.validate_data(df)

        assert not result.has_errors
        assert len(result.errors) == 0
        assert result.total_rows == 3
        assert result.valid_rows == 3
        assert len(result.error_row_indices) == 0
        assert result.success_rate == 1.0
        assert result.error_rate == 0.0
        # Clean data should be the same as original
        assert result.clean_data.equals(df)

    def test_validate_with_null_in_required_field(self):
        """Test validation with null values in required fields."""

        class UserModel(PolarsFastDataframeModel):
            id: int
            name: str
            email: Optional[str] = None

        df = pl.DataFrame(
            {
                "id": [1, None, 3],  # Row 1 has null in required field
                "name": ["Alice", "Bob", None],  # Row 2 has null in required field
                "email": [None, "bob@example.com", "charlie@example.com"],
            }
        )

        result = UserModel.validate_data(df)

        assert result.has_errors
        assert len(result.errors) == 2  # One error for each required field with nulls
        assert result.total_rows == 3
        assert result.valid_rows == 1  # Only row 0 is valid
        assert result.error_row_indices == [1, 2]  # Rows 1 and 2 have errors
        assert result.success_rate == pytest.approx(1 / 3)
        assert result.error_rate == pytest.approx(2 / 3)

        # Check error details
        id_error = next(err for err in result.errors if err.column_name == "id")
        assert id_error.error_type == "null_in_required_field"
        assert id_error.error_rows == [1]

        name_error = next(err for err in result.errors if err.column_name == "name")
        assert name_error.error_type == "null_in_required_field"
        assert name_error.error_rows == [2]

        # Clean data should only have the first row
        expected_clean = pl.DataFrame({"id": [1], "name": ["Alice"], "email": [None]})
        assert result.clean_data.equals(expected_clean)

    def test_validate_all_rows_have_errors(self):
        """Test validation where all rows have errors."""

        class UserModel(PolarsFastDataframeModel):
            id: int
            name: str

        df = pl.DataFrame(
            {
                "id": [None, None, None],  # All rows have null in required field
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        result = UserModel.validate_data(df)

        assert result.has_errors
        assert len(result.errors) == 1
        assert result.total_rows == 3
        assert result.valid_rows == 0
        assert result.error_row_indices == [0, 1, 2]
        assert result.success_rate == 0.0
        assert result.error_rate == 1.0

        # Clean data should be empty but maintain schema
        assert len(result.clean_data) == 0
        assert result.clean_data.schema == df.schema

    def test_validate_optional_fields_with_nulls_allowed(self):
        """Test that optional fields with null values don't cause errors."""

        class UserModel(PolarsFastDataframeModel):
            id: int
            name: Optional[str] = None
            email: Optional[str] = None

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": [None, "Bob", None],  # Optional field can have nulls
                "email": [
                    None,
                    None,
                    "charlie@example.com",
                ],  # Optional field can have nulls
            }
        )

        result = UserModel.validate_data(df)

        assert not result.has_errors
        assert len(result.errors) == 0
        assert result.total_rows == 3
        assert result.valid_rows == 3
        assert result.clean_data.equals(df)

    def test_validate_empty_dataframe(self):
        """Test validation with empty DataFrame."""

        class UserModel(PolarsFastDataframeModel):
            id: int
            name: str

        df = pl.DataFrame(
            {
                "id": pl.Series([], dtype=pl.Int64),
                "name": pl.Series([], dtype=pl.String),
            }
        )

        result = UserModel.validate_data(df)

        assert not result.has_errors
        assert len(result.errors) == 0
        assert result.total_rows == 0
        assert result.valid_rows == 0
        assert len(result.error_row_indices) == 0
        assert result.success_rate == 1.0  # No rows means 100% success rate
        assert result.error_rate == 0.0

    def test_validate_missing_column(self):
        """Test validation when required column is missing from DataFrame."""

        class UserModel(PolarsFastDataframeModel):
            id: int
            name: str
            email: str

        # DataFrame missing the 'email' column
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        result = UserModel.validate_data(df)

        # Missing required column should cause all rows to be invalid
        assert result.has_errors
        assert len(result.errors) == 1
        assert result.total_rows == 3
        assert result.valid_rows == 0
        assert result.error_row_indices == [0, 1, 2]  # All rows are invalid
        assert result.success_rate == 0.0
        assert result.error_rate == 1.0

        # Check error details
        error = result.errors[0]
        assert error.column_name == "email"
        assert error.error_type == "missing_required_column"
        assert error.error_rows == [0, 1, 2]
        assert "missing from DataFrame" in error.error_details

        # Clean data should be empty but maintain original schema
        assert len(result.clean_data) == 0
        assert result.clean_data.schema == df.schema

    def test_validate_multiple_missing_columns(self):
        """Test validation when multiple required columns are missing."""

        class UserModel(PolarsFastDataframeModel):
            id: int
            name: str
            email: str
            age: int

        # DataFrame missing 'email' and 'age' columns
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        result = UserModel.validate_data(df)

        # Multiple missing required columns should cause all rows to be invalid
        assert result.has_errors
        assert len(result.errors) == 2  # One error for each missing column
        assert result.total_rows == 3
        assert result.valid_rows == 0
        assert result.error_row_indices == [0, 1, 2]  # All rows are invalid
        assert result.success_rate == 0.0
        assert result.error_rate == 1.0

        # Check that both missing columns are reported
        missing_columns = {error.column_name for error in result.errors}
        assert missing_columns == {"email", "age"}

        # Each error should report all rows as invalid
        for error in result.errors:
            assert error.error_type == "missing_required_column"
            assert error.error_rows == [0, 1, 2]

        # Clean data should be empty
        assert len(result.clean_data) == 0

    def test_validate_missing_column_empty_dataframe(self):
        """Test validation with missing columns on empty DataFrame."""

        class UserModel(PolarsFastDataframeModel):
            id: int
            name: str
            email: str

        # Empty DataFrame missing the 'email' column
        df = pl.DataFrame(
            {
                "id": pl.Series([], dtype=pl.Int64),
                "name": pl.Series([], dtype=pl.String),
            }
        )

        result = UserModel.validate_data(df)

        # Missing columns on empty DataFrame should not create errors
        # since there are no rows to be invalid
        assert not result.has_errors
        assert len(result.errors) == 0
        assert result.total_rows == 0
        assert result.valid_rows == 0
        assert len(result.error_row_indices) == 0
        assert result.success_rate == 1.0  # No rows means 100% success
        assert result.error_rate == 0.0
