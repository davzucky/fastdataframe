def json_schema_is_subset(left: dict, right: dict) -> bool:
    """
    Returns True if the left JSON schema is a subset of (compatible with) the right schema.
    Handles:
    - 'type' (including list of types)
    - 'format' (for temporal types)
    - 'anyOf'/'oneOf' (recursively)
    - nullability (e.g., Optional types)
    - arrays and objects (recursively checks 'items' and 'properties')
    """
    # Early return for identical schemas
    if left == right:
        return True
    def normalize_type(t):
        if isinstance(t, list):
            return set(t)
        if t is None:
            return set()
        return {t}

    # If both are empty, they are equal
    if not left and not right:
        return True
    # If right is empty but left is not, not a subset
    if not right and left:
        return False

    # Handle anyOf/oneOf in left: nuanced logic for optionals and Decimal
    for key in ("anyOf", "oneOf"):
        if key in left:
            options = left[key]
            # Optionals: ignore 'null'
            non_null_options = [opt for opt in options if opt.get("type") != "null"]
            null_options = [opt for opt in options if opt.get("type") == "null"]
            if all(json_schema_is_subset(opt, right) for opt in non_null_options):
                if len(non_null_options) + len(null_options) == len(options):
                    return True
            # Decimals: ignore 'string'
            non_string_options = [opt for opt in options if opt.get("type") != "string"]
            string_options = [opt for opt in options if opt.get("type") == "string"]
            if all(json_schema_is_subset(opt, right) for opt in non_string_options):
                if len(non_string_options) + len(string_options) == len(options):
                    return True
            # If right has anyOf/oneOf, for each left option, there must be at least one right option that matches
            if key in right:
                if all(any(json_schema_is_subset(opt, r_opt) for r_opt in right[key]) for opt in options):
                    return True
            # Otherwise, require all options to be a subset of right
            if all(json_schema_is_subset(opt, right) for opt in options):
                return True
            return False
        if key in right:
            # All right options must accept left (strict subset logic)
            return all(json_schema_is_subset(left, opt) for opt in right[key])

    # Compare types
    left_types = normalize_type(left.get("type"))
    right_types = normalize_type(right.get("type"))
    if left_types:
        # If left is a subset of right, or left is more permissive (e.g., includes 'null')
        if not left_types.issubset(right_types) and not ("null" in left_types and left_types - {"null"} == right_types):
            return False

    # Handle arrays: recursively check 'items'
    if left.get("type") == "array" and right.get("type") == "array":
        left_items = left.get("items", {})
        right_items = right.get("items", {})
        # If right_items is empty, only allow if left_items is also empty
        if not right_items:
            return not left_items
        # If left_items is empty but right_items is not, not a subset
        if not left_items and right_items:
            return False
        # If both are non-empty, recursively check
        if not json_schema_is_subset(left_items, right_items):
            return False

    # Handle objects: recursively check 'properties'
    if left.get("type") == "object" and right.get("type") == "object":
        left_props = left.get("properties", {})
        right_props = right.get("properties", {})
        for prop, left_prop_schema in left_props.items():
            right_prop_schema = right_props.get(prop)
            if right_prop_schema is None:
                return False
            if not json_schema_is_subset(left_prop_schema, right_prop_schema):
                return False

    # Strict format check: if left has a format, right must have the same format
    left_format = left.get("format")
    right_format = right.get("format")
    if left_format:
        if left_format != right_format:
            return False
    # If right has a format but left does not, left is not a subset
    if right_format and not left_format:
        return False
    # If left has a format but right does not, that's now not allowed (already handled above)
    return True 