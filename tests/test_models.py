import datetime as dt
from typing import Optional

from pydantic import AliasChoices, Field
from fastdataframe import FastDataframeModel

class BaseModel(FastDataframeModel):
    """Base model for testing conversion."""

    name: str
    age: int
    is_active: bool = Field(alias=AliasChoices("is_active", "is active"))
    score: Optional[float] = None

class TemporalModel(FastDataframeModel):
    d: dt.date
    dt_: dt.datetime
    t: dt.time
    td: dt.timedelta
