from typing import Annotated
import pytest
from pydantic import Field

from fastdataframe.model import FastDataframeModel
from fastdataframe.metadata import FastDataframe


def test_model_with_fastdataframe_annotation():
    class MyModel(FastDataframeModel):
        x: int = Field(json_schema_extra=FastDataframe(is_nullable=False, is_unique=True).as_field_metadata())
        y: Annotated[str, FastDataframe(is_nullable=False, is_unique=False)]
        z: Annotated[str, FastDataframe(is_nullable=False, is_unique=False)]

    m = MyModel(x=1, y="test")
    assert m.x == 1
    assert m.y == "test"
    # Check schema includes _fastdataframe
    schema = MyModel.model_json_schema()
    assert "_fastdataframe" in schema["properties"]["y"]
    assert schema["properties"]["y"]["_fastdataframe"]["properties"]["is_nullable"] is True
    assert schema["properties"]["y"]["_fastdataframe"]["properties"]["is_unique"] is False
    assert "_fastdataframe" in schema["properties"]["x"]
    assert schema["properties"]["x"]["_fastdataframe"]["properties"]["is_nullable"] is False
    assert schema["properties"]["x"]["_fastdataframe"]["properties"]["is_unique"] is True    


# def test_model_missing_fastdataframe_annotation_raises():
#     # Should raise ValueError because 'y' is missing FastDataframe annotation
#     with pytest.raises(ValueError, match="Field 'y' must have FastDataframe annotation"):
#         class BadModel(FastDataframeModel):
#             x: int = Field(json_schema_extra=FastDataframe(is_nullable=False, is_unique=True).as_field_metadata())
#             y: str  # Missing annotation


# def test_model_partial_fastdataframe_annotation_raises():
#     # Should raise ValueError because 'y' is missing FastDataframe annotation
#     with pytest.raises(ValueError, match="Field 'y' must have FastDataframe annotation"):
#         class PartialModel(FastDataframeModel):
#             x: int = Field(json_schema_extra=FastDataframe(is_nullable=False, is_unique=True).as_field_metadata())
#             y: str = Field() 