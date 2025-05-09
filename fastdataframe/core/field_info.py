from typing import Any, Optional
from pydantic.fields import FieldInfo as PydanticFieldInfo

class FieldInfo(PydanticFieldInfo):
    """
    Extended FieldInfo class that adds additional attributes for DataFrame column configuration.
    """
    def __init__(
        self,
        *,
        unique: bool = False,
        allow_missing: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.unique = unique
        self.allow_missing = allow_missing 