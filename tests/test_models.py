import datetime as dt
from typing import Optional
from fastdataframe import FastDataframeModel


class UserTestModel(FastDataframeModel):
    """Base model for testing conversion."""

    name: str
    age: int
    is_active: bool
    score: Optional[float] = None


class TemporalModel(FastDataframeModel):
    d: dt.date
    dt_: dt.datetime
    t: dt.time
    td: dt.timedelta
