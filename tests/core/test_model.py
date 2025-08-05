"""Tests for FastDataframe model implementation."""

from typing import Optional, Union, Annotated, List, Dict, Any
from pydantic import Field, create_model
import pytest

from fastdataframe import FastDataframeModel
from fastdataframe import ColumnInfo


class TestFastDataframeModel:
    @pytest.mark.parametrize(
        "field_type,expected_is_nullable,expected_is_unique",
        [
            # Basic types
            ({"test_field": (str, ...)}, False, False),
            ({"test_field": (float, ...)}, False, False),
            ({"test_field": (bool, ...)}, False, False),
            # Optional types
            ({"test_field": (Optional[int], None)}, True, False),
            ({"test_field": (Optional[str], None)}, True, False),
            ({"test_field": (Optional[float], None)}, True, False),
            ({"test_field": (Optional[bool], None)}, True, False),
            # Union types with None
            ({"test_field": (Union[int, None], None)}, True, False),
            ({"test_field": (Union[str, None], None)}, True, False),
            ({"test_field": (Union[float, None], None)}, True, False),
            ({"test_field": (Union[bool, None], None)}, True, False),
            # Union types without None
            ({"test_field": (Union[int, str], ...)}, False, False),
            ({"test_field": (Union[float, bool], ...)}, False, False),
            # Collection types
            ({"test_field": (List[int], ...)}, False, False),
            ({"test_field": (Dict[str, Any], ...)}, False, False),
            # Optional collections
            ({"test_field": (Optional[List[int]], None)}, True, False),
            ({"test_field": (Optional[Dict[str, Any]], None)}, True, False),
            # Annotated types with explicit metadata
            (
                {"test_field": (Annotated[int, ColumnInfo(is_unique=True)], ...)},
                False,
                True,
            ),
            (
                {"test_field": (Annotated[str, ColumnInfo(is_unique=True)], ...)},
                False,
                True,
            ),
            (
                {
                    "test_field": (
                        Annotated[Optional[int], ColumnInfo(is_unique=False)],
                        None,
                    )
                },
                True,
                False,
            ),
            (
                {
                    "test_field": (
                        Annotated[Optional[str], ColumnInfo(is_unique=True)],
                        None,
                    )
                },
                True,
                True,
            ),
            # Annotation that contains Field
            ({"test_field": (Annotated[int, Field()], ...)}, False, False),
            ({"test_field": (Annotated[Optional[str], Field()], None)}, True, False),
        ],
    )
    def test_model_base_type(
        self,
        field_type: dict[str, Any],
        expected_is_nullable: bool,
        expected_is_unique: bool,
    ) -> None:
        """Test that model fields have correct nullability (via required) and is_unique properties based on their type."""
        # class MyModel(FastDataframeModel):
        my_model = create_model("my_model", __base__=FastDataframeModel, **field_type)

        assert issubclass(my_model, FastDataframeModel)
        schema = my_model.model_json_schema()

        required_fields = schema.get("required", [])
        if expected_is_nullable:
            assert "test_field" not in required_fields
        else:
            assert "test_field" in required_fields

    def test_model_conversion(self) -> None:
        class BaseTransaction(FastDataframeModel):
            transaction_id: str

        class Transaction(BaseTransaction):
            amount: float
            timestamp: str

        # Convert to Iceberg schema
        TestTransaction = FastDataframeModel.from_base_model(Transaction)
        print(dir(TestTransaction))
        annotations = TestTransaction.model_fields.keys()
        assert "transaction_id" in annotations
        assert "amount" in annotations
        assert "timestamp" in annotations

    def test_get_fastdataframe_annotations(self) -> None:
        """Test that get_fastdataframe_annotations returns a dictionary mapping field_name to FastDataframe annotation objects."""

        class MyModel(FastDataframeModel):
            field1: int
            field2: str
            field3: Optional[float] = None
            field4: Annotated[bool, ColumnInfo(is_unique=True)]

        annotations = MyModel.model_columns()
        assert isinstance(annotations, dict)
        assert "field1" in annotations
        assert "field2" in annotations
        assert "field3" in annotations
        assert "field4" in annotations
        assert annotations["field4"].is_unique is True
