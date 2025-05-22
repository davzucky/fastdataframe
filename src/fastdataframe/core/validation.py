"""Validation models for FastDataframe."""

from pydantic import BaseModel
from typing import List, Optional


class ValidationError(BaseModel):
    """Model for validation errors."""

    column_name: str
    error_type: str
    error_details: str
    error_rows: Optional[List[int]] = None
