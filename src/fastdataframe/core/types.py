# Helper: normalize type field to a set
def normalize_type(t):
    if isinstance(t, list):
        return set(t)
    if t is None:
        return set()
    return {t}

# Helper: filter out ignorable types from a list of options
def filter_options(options, ignorable_types):
    return [opt for opt in options if opt.get("type") not in ignorable_types]

# Helper: check if schema is a union (anyOf/oneOf)
def is_union(schema):
    return any(k in schema for k in ("anyOf", "oneOf"))

# Helper: get union options from schema
def get_union_options(schema):
    for k in ("anyOf", "oneOf"):
        if k in schema:
            return schema[k]
    return None

def array_schema_is_subset(left: dict, right: dict) -> bool:
    """
    Returns True if the left array schema is a superset of the right array schema.
    - If right's items is unconstrained, left's must also be unconstrained.
    - Otherwise, left's items must be a superset of right's items.
    """
    left_items = left.get("items", {})
    right_items = right.get("items", {})
    if not right_items:
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
            return False
        if not json_schema_is_subset(left_props[prop], right_props[prop]):
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
    - Arrays: If right's items is unconstrained, left's must also be unconstrained. Otherwise, left's items must be a superset of right's items.
    - Objects: Left can have more properties, but must cover all right properties, and each left property must be a superset of the right property.

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
        return not left

    left_union = get_union_options(left)
    right_union = get_union_options(right)

    # --- Union/anyOf/oneOf Handling ---
    # If right is a union: For every right option, there must be a left option that is a superset of it.
    if right_union is not None:
        for r_opt in right_union:
            if left_union is None:
                # Left must cover all right options
                if not json_schema_is_subset(left, r_opt):
                    return False
            else:
                # For each right option, there must be a left option that is a superset
                if not any(json_schema_is_subset(l_opt, r_opt) for l_opt in left_union):
                    return False
        return True
    # If left is a union and right is not: If any left option is a superset of right, return True.
    elif left_union is not None:
        return any(json_schema_is_subset(l_opt, right) for l_opt in left_union)

    # --- Type Handling ---
    left_types = normalize_type(left.get("type"))
    right_types = normalize_type(right.get("type"))
    if not left_types.issuperset(right_types):
        return False

    # --- Format Handling ---
    left_format = left.get("format")
    right_format = right.get("format")
    if right_format is None:
        if left_format is not None:
            return False
    elif left_format != right_format:
        return False

    # --- Array Handling ---
    if left.get("type") == "array" and right.get("type") == "array":
        return array_schema_is_subset(left, right)

    # --- Object Handling ---
    if left.get("type") == "object" and right.get("type") == "object":
        return object_schema_is_subset(left, right)

    return True 