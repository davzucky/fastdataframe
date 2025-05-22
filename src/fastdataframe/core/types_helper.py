"""Helper functions for type checking and manipulation."""

from typing import Any, Iterable, Optional, Type, get_origin, get_args, Annotated, Union


def is_optional_type(field_type: Any) -> bool:
    """Check if a type is optional (can be None).

    Args:
        field_type: The type to check

    Returns:
        bool: True if the type is optional, False otherwise
    """
    origin = get_origin(field_type)
    args = get_args(field_type)

    # Handle Annotated types by recursing into the first argument
    if origin is Annotated and args:
        return is_optional_type(args[0])

    # Handle Union types (including Optional which is Union[T, None])
    if origin is Union:
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
