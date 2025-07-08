from pydantic import AliasChoices, AliasPath
from pydantic.fields import FieldInfo


def get_serialization_alias(field_info: FieldInfo, field_name: str) -> str:
    """Get a mapping of field names to their serialization aliases."""

    # Get the effective serialization alias
    serialization_alias = field_info.serialization_alias
    if serialization_alias is None:
        # Fall back to alias if no serialization_alias
        serialization_alias = field_info.alias
    if serialization_alias is None:
        # Fall back to field name
        serialization_alias = field_name
    return serialization_alias


def get_validation_alias(field_info: FieldInfo, field_name: str) -> str:
    """Get a mapping of field names to their serialization aliases."""

    validation_alias = field_info.validation_alias
    if validation_alias is None:
        validation_alias = field_info.alias
    if validation_alias is None:
        validation_alias = field_name
    if validation_alias is None:
        raise ValueError(f"Invalid validation alias: {validation_alias}")

    if isinstance(validation_alias, str):
        return validation_alias

    serialization_alias = get_serialization_alias(field_info, field_name)
    if isinstance(validation_alias, AliasPath):
        validation_alias = AliasChoices(validation_alias)

    aliases = [
        str_alias
        for alias in validation_alias.convert_to_aliases()
        if (str_alias := ".".join([str(a) for a in alias])) != serialization_alias
    ]
    if len(aliases) == 0:
        return serialization_alias
    if len(aliases) == 1:
        return aliases[0]

    raise ValueError(f"Invalid validation alias: {aliases}")
