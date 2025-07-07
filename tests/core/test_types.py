import pytest
from fastdataframe.core.types import json_schema_is_subset
from pydantic import TypeAdapter
import datetime as dt
from decimal import Decimal

class TestJsonSchemaIsSubset:
    @pytest.mark.parametrize(
        "left_type, right_type, expected_result",
        [
            (int, int, True),
            (int, float, False),
            (int, object, False),  # int is not a subset of object (empty schema)
            (int | None, int, True),  # Optional[int] vs int
            (int, int | str, False),  # int is a subset of int|str
            (int | str, int, True),  # int|str is not a subset of int
            (str, str, True),
            (str, int, False),
            (str | None, str, True),
            (dt.date, str, False),  # date serializes as string, but should require format
            (
                dt.datetime,
                str,
                False,
            ),  # datetime serializes as string, but should require format
            (dt.date, dt.date, True),
            (dt.date, dt.datetime, False),
            (float, int, False),
            (int, float | int, False),
            (float | int, int, True),  # float|int is a superset of int
            (None, int, False),
            (int, None, False),
            # Additional cases for bool, list, Decimal
            (bool, bool, True),
            (bool, int, False),
            (list[int], list[int], True),
            (list[int], list[str], False),
            (
                list[int],
                list,
                False,
            ),  # list[int] is a subset of list (if list is unconstrained)
            (list, list[int], False),  # unconstrained list is not a subset of list[int]
            (Decimal, float, True),  # Decimal is not a subset of float
            (Decimal, Decimal, True),
            (float, Decimal, False),
            # complex cases
            (int | float | str, int | float, True),
        ],
    )
    def test_json_schema_is_subset_param(self, left_type: type, right_type: type, expected_result: bool) -> None:
        left_schema = TypeAdapter(left_type).json_schema()
        right_schema = TypeAdapter(right_type).json_schema()
        assert json_schema_is_subset(left_schema, right_schema) is expected_result
