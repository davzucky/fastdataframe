from fastdataframe.core.model import FastDataframeModel
from fastdataframe.core.validation import ValidationError
from typing import List
from pyiceberg.table import Table

class IcebergFastDataframeModel(FastDataframeModel):
    """A model that extends FastDataframeModel for Iceberg integration."""

    @classmethod
    def validate_table(cls, table: Table) -> List[ValidationError]:
        """Validate that the Iceberg table's columns match the model's fields by name only.

        Args:
            table: The Iceberg table to validate.

        Returns:
            List[ValidationError]: A list of validation errors (missing columns).
        """
        model_fields = set(cls.model_fields.keys())
        table_columns = set(field.name for field in table.schema().fields)
        errors = []
        for field in model_fields:
            if field not in table_columns:
                errors.append(
                    ValidationError(
                        column_name=field,
                        error_type="MissingColumn",
                        error_details=f"Column {field} is missing in the Iceberg table.",
                    )
                )
        return errors 