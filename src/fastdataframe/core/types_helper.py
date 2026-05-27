"""Helper functions for type checking and manipulation."""

import types
from typing import Any, Iterable, Optional, Type, get_origin, get_args, Annotated, Union


def unwrap_annotated(annotation: Any) -> Any:
    """Unwrap Annotated[T, ...] to T."""
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Annotated and args:
        return args[0]
    return annotation


def unwrap_annotated_optional(annotation: Any) -> Any:
    """Unwrap Annotated and Optional/None unions to the non-None annotation.

    If a union contains multiple non-None types, the original unwrapped annotation is
    returned because there is no single semantic type to refine.
    """
    annotation = unwrap_annotated(annotation)
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (Union, types.UnionType):
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            return unwrap_annotated(non_none_args[0])
    return annotation


def is_optional_type(field_type: Any) -> bool:
    """Check if a type is optional (can be None).

    Args:
        field_type: The type to check

    Returns:
        bool: True if the type is optional, False otherwise

    Notes:
        Handles both typing.Union (Optional[T]) and PEP 604 syntax (T | None).
    """
    origin = get_origin(field_type)
    args = get_args(field_type)

    # Handle Annotated types by recursing into the first argument
    if origin is Annotated and args:
        return is_optional_type(args[0])

    # Handle Union types (including Optional which is Union[T, None])
    # and PEP 604 syntax (T | None) which uses types.UnionType
    if origin in (Union, types.UnionType):
        return type(None) in args

    # Handle direct None type
    return field_type is type(None)


def contains_type(list_args: list[Any], type: Type) -> bool:
    """Check if a list contains a specific type.

    Args:
        list_args: The list to check
        type: The type to check for

    Returns:
        bool: True if the list contains the type, False otherwise
    """
    for arg in list_args:
        if isinstance(arg, type):
            return True
    return False


def filter_type(list_args: Iterable[Any], type: Type) -> list[Any]:
    """Check if a list contains a specific type.

    Args:
        list_args: The list to check
        type: The type to check for

    Returns:
        bool: True if the list contains the type, False otherwise
    """
    result = []
    for arg in list_args:
        if not isinstance(arg, type):
            result.append(arg)
    return result


def get_item_of_type(list_args: Iterable[Any], type: Type) -> Optional[Any]:
    """Check if a list contains a specific type.

    Args:
        list_args: The list to check
        type: The type to check for

    Returns:
        bool: True if the list contains the type, False otherwise
    """
    for arg in list_args:
        if isinstance(arg, type):
            return arg
    return None
