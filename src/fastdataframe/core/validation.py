"""Validation models for FastDataframe."""

from pydantic import BaseModel
from typing import List, Optional, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    import polars as pl


class ValidationError(BaseModel):
    """Model for validation errors."""

    column_name: str
    error_type: str
    error_details: str
    error_rows: Optional[List[int]] = None


@dataclass
class ValidationResult:
    """Result of data validation containing errors and clean data.

    This class encapsulates the results of validating a DataFrame against a model's
    requirements, providing both error details and a cleaned dataset.

    Attributes:
        errors: List of validation errors found during validation
        clean_data: DataFrame with all problematic rows removed
        error_row_indices: Original row indices that contained validation errors
        total_rows: Total number of rows in the original dataset
        valid_rows: Number of rows remaining after removing errors
    """

    errors: List[ValidationError]
    clean_data: "pl.DataFrame"
    error_row_indices: List[int]
    total_rows: int
    valid_rows: int

    @property
    def has_errors(self) -> bool:
        """True if any validation errors were found."""
        return len(self.errors) > 0

    @property
    def error_rate(self) -> float:
        """Fraction of rows that contained errors (0.0 to 1.0)."""
        if self.total_rows == 0:
            return 0.0
        return len(self.error_row_indices) / self.total_rows

    @property
    def success_rate(self) -> float:
        """Fraction of rows that passed validation (0.0 to 1.0)."""
        return 1.0 - self.error_rate
