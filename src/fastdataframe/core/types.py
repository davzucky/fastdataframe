import logging
from typing import Any

logger = logging.getLogger(__name__)


# Helper: normalize type field to a set
def normalize_type(t: Any) -> set[Any]:
    if isinstance(t, list):
        return set(t)
    if t is None:
        return set()
    return {t}


def get_type_name(t: dict[str, Any]) -> str:
    if "anyOf" in t or "oneOf" in t:
        return f"union[{', '.join([get_type_name(o) for o in t.get('anyOf', t.get('oneOf', []))])}]"
    return str(t.get("format", t.get("type")))


# Helper: filter out ignorable types from a list of options
def filter_options(options: list[dict], ignorable_types: set) -> list[dict]:
    return [opt for opt in options if opt.get("type") not in ignorable_types]


# Helper: check if schema is a union (anyOf/oneOf)
def is_union(schema: dict) -> bool:
    return any(k in schema for k in ("anyOf", "oneOf"))


# Helper: get union options from schema
def get_union_options(schema: dict) -> Any:
    for k in ("anyOf", "oneOf"):
        if k in schema:
            return schema[k]
    return None


def constraints_are_superset(left: dict, right: dict, keys: list[str]) -> bool:
    """
    Returns True if all constraints in left are not more restrictive than those in right.
    For min-like constraints: left[min] <= right[min] (left allows more)
    For max-like constraints: left[max] >= right[max] (left allows more)
    For pattern: left must be None or equal to right (left allows more)
    For uniqueItems: left must be False if right is False (left allows more)
    """
    for key in keys:
        l_value = left.get(key)
        r_value = right.get(key)
        if r_value is None:
            continue  # right is unconstrained, left can be anything
        if key in ("minimum", "exclusiveMinimum", "minLength", "minItems"):
            if l_value is not None and l_value > r_value:
                logger.debug(
                    f"Constraint fail: {key}: left {l_value} > right {r_value}"
                )
                return False
        elif key in ("maximum", "exclusiveMaximum", "maxLength", "maxItems"):
            if l_value is not None and l_value < r_value:
                logger.debug(
                    f"Constraint fail: {key}: left {l_value} < right {r_value}"
                )
                return False
        elif key == "multipleOf":
            if l_value is not None and (r_value % l_value != 0):
                logger.debug(
                    f"Constraint fail: multipleOf: right {r_value} % left {l_value} != 0"
                )
                return False
        elif key == "pattern":
            if l_value is not None and l_value != r_value:
                logger.debug(
                    f"Constraint fail: pattern: left {l_value} != right {r_value}"
                )
                return False
        elif key == "uniqueItems":
            if r_value is False and l_value is not False:
                logger.debug(
                    "Constraint fail: uniqueItems: right is False, left is not False"
                )
                return False
    return True


def array_schema_is_subset(left: dict, right: dict) -> bool:
    """
    Returns True if the left array schema is a superset of the right array schema.
    - If right's items is unconstrained, left's must also be unconstrained.
    - Otherwise, left's items must be a superset of right's items.
    - Checks array constraints: minItems, maxItems, uniqueItems
    """
    if not constraints_are_superset(
        left, right, ["minItems", "maxItems", "uniqueItems"]
    ):
        logger.debug("Array constraint check failed.")
        return False
    left_items = left.get("items", {})
    right_items = right.get("items", {})
    if not right_items:
        if left_items:
            logger.debug("Right array items unconstrained, but left is constrained.")
        return not left_items
    return json_schema_is_subset(left_items, right_items)


def object_schema_is_subset(left: dict, right: dict) -> bool:
    """
    Returns True if the left object schema is a superset of the right object schema.
    - Left can have more properties, but must cover all right properties.
    - Each left property must be a superset of the right property.
    """
    left_props = left.get("properties", {})
    right_props = right.get("properties", {})
    for prop in right_props:
        if prop not in left_props:
            logger.debug(f"Object property '{prop}' missing in left.")
            return False
        if not json_schema_is_subset(left_props[prop], right_props[prop]):
            logger.debug(f"Object property '{prop}' is not a superset.")
            return False
    return True


def format_is_superset(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """
    Returns True if the left format is a superset of the right format.
    - If right has a format, left must have the same format.
    - If right does not have a format, left must not be more restrictive (i.e., left must not have a format).
    """
    left_format = left.get("format")
    right_format = right.get("format")
    if right_format is None:
        if left_format is not None:
            logger.debug(f"Format fail: right has no format, left has {left_format}")
        return left_format is None
    if left_format != right_format:
        logger.debug(f"Format fail: left {left_format} != right {right_format}")
        return False
    return True


def json_schema_is_subset(left: dict, right: dict) -> bool:
    """
    Returns True if the left JSON schema is a superset of the right schema (i.e., all values valid under right are also valid under left).

    Subset Logic Details:
    - 'type': Left type set must be a superset of right type set.
    - 'format': If right has a format, left must have the same format. If right does not have a format, left must not be more restrictive (i.e., left must not have a format).
    - 'anyOf'/'oneOf' (Unions):
        * If right is a union: For every right option, there must be a left option that is a superset of it.
        * If left is a union and right is not: If any left option is a superset of right, return True. (Covers optionals, e.g., Optional[int] vs int, and permissive unions like float|int vs int.)
    - Optionals: Handled as unions with 'null' type. E.g., Optional[int] is {'anyOf': [{'type': 'integer'}, {'type': 'null'}]}.
    - Decimals: Often represented as unions (e.g., {'anyOf': [{'type': 'number'}, {'type': 'string'}]}). Subset logic for unions applies.
    - Arrays: If right's items is unconstrained, left's must also be unconstrained. Otherwise, left's items must be a superset of right's items. Checks minItems, maxItems, uniqueItems.
    - Objects: Left can have more properties, but must cover all right properties, and each left property must be a superset of the right property.
    - Numeric: Checks minimum, maximum, exclusiveMinimum, exclusiveMaximum, multipleOf.
    - String: Checks minLength, maxLength, pattern.

    Args:
        left (dict): The candidate superset JSON schema.
        right (dict): The candidate subset JSON schema.
    Returns:
        bool: True if left is a superset of right, False otherwise.
    """
    # Early return for identical schemas
    if left == right:
        return True

    # If right is empty (accepts anything), only an empty left is a superset
    if not right:
        if left:
            logger.debug("Right is unconstrained, but left is not.")
        return not left

    left_union = get_union_options(left)
    right_union = get_union_options(right)

    # --- Union/anyOf/oneOf Handling ---
    if right_union is not None:
        for r_opt in right_union:
            if left_union is None:
                if not json_schema_is_subset(left, r_opt):
                    logger.debug("Union: left is not a superset of right option.")
                    return False
            else:
                if not any(json_schema_is_subset(l_opt, r_opt) for l_opt in left_union):
                    logger.debug("Union: no left option is a superset of right option.")
                    return False
        return True
    elif left_union is not None:
        if not any(json_schema_is_subset(l_opt, right) for l_opt in left_union):
            logger.debug("Union: no left option is a superset of right.")
            return False
        return True

    # --- Type Handling ---
    left_types = normalize_type(left.get("type"))
    right_types = normalize_type(right.get("type"))
    if not left_types.issuperset(right_types):
        logger.debug(
            f"Type fail: left {left_types} is not a superset of right {right_types}"
        )
        return False

    # --- Numeric Constraints ---
    if not constraints_are_superset(
        left,
        right,
        ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"],
    ):
        logger.debug("Numeric constraint check failed.")
        return False

    # --- String Constraints ---
    if not constraints_are_superset(left, right, ["minLength", "maxLength", "pattern"]):
        logger.debug("String constraint check failed.")
        return False

    # --- Format Handling ---
    if not format_is_superset(left, right):
        return False

    # --- Array Handling ---
    if left.get("type") == "array" and right.get("type") == "array":
        return array_schema_is_subset(left, right)

    # --- Object Handling ---
    if left.get("type") == "object" and right.get("type") == "object":
        return object_schema_is_subset(left, right)

    return True
