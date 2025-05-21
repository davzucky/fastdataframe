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

def json_schema_is_subset(left: dict, right: dict) -> bool:
    """
    Returns True if the left JSON schema is a superset of the right schema (i.e., all values valid under right are also valid under left).
    Handles:
    - 'type' (including list of types)
    - 'format' (for temporal types)
    - 'anyOf'/'oneOf' (recursively, superset logic)
    - nullability (e.g., Optional types)
    - arrays and objects (recursively checks 'items' and 'properties')
    """
    # Early return for identical schemas
    if left == right:
        return True

    # If right is empty (accepts anything), only an empty left is a superset
    if not right:
        return not left

    left_union = get_union_options(left)
    right_union = get_union_options(right)

    # Superset logic for unions (anyOf/oneOf)
    if right_union is not None:
        if left_union is None:
            # Left must cover all right options
            return all(json_schema_is_subset(left, r_opt) for r_opt in right_union)
        # For each right option, there must be a left option that is a superset
        for r_opt in right_union:
            if not any(json_schema_is_subset(l_opt, r_opt) for l_opt in left_union):
                return False
        return True
    elif left_union is not None:
        # Left is a union, right is not: left is a superset if any left option is a superset of right
        return any(json_schema_is_subset(l_opt, right) for l_opt in left_union)

    # Superset logic for types
    left_types = normalize_type(left.get("type"))
    right_types = normalize_type(right.get("type"))
    if not left_types.issuperset(right_types):
        return False

    # Format: left must not be more restrictive than right
    left_format = left.get("format")
    right_format = right.get("format")
    if right_format is None:
        if left_format is not None:
            return False
    elif left_format != right_format:
        return False

    # Arrays: left must be a superset of right in 'items'
    if left.get("type") == "array" and right.get("type") == "array":
        left_items = left.get("items", {})
        right_items = right.get("items", {})
        # If right's items is unconstrained, left's must also be unconstrained
        if not right_items:
            return not left_items
        if not json_schema_is_subset(left_items, right_items):
            return False

    # Objects: left must be a superset of right in 'properties'
    if left.get("type") == "object" and right.get("type") == "object":
        left_props = left.get("properties", {})
        right_props = right.get("properties", {})
        # Left can have more properties, but must cover all right properties
        for prop in right_props:
            if prop not in left_props:
                return False
            if not json_schema_is_subset(left_props[prop], right_props[prop]):
                return False

    return True 