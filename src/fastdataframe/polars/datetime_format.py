"""Datetime format conversion utilities for Polars integration.

This module provides functions to convert Python datetime format codes
to Rust chrono format codes for use with Polars datetime operations.
"""

from typing import Dict


# Mapping from Python datetime format codes to Rust chrono format codes
PYTHON_TO_CHRONO_FORMAT_MAP: Dict[str, str] = {
    # Date specifiers
    "%Y": "%Y",  # Year with century as a decimal number (0001, 0002, ..., 2013, 2014, ..., 9998, 9999)
    "%y": "%y",  # Year without century as a zero-padded decimal number (00, 01, ..., 99)
    "%m": "%m",  # Month as a zero-padded decimal number (01, 02, ..., 12)
    "%b": "%b",  # Month as locale's abbreviated name (Jan, Dec)
    "%B": "%B",  # Month as locale's full name (January, December)
    "%d": "%d",  # Day of the month as a zero-padded decimal number (01, 02, ..., 31)
    "%a": "%a",  # Weekday as locale's abbreviated name (Sun, Mon)
    "%A": "%A",  # Weekday as locale's full name (Sunday, Monday)
    "%w": "%w",  # Weekday as a decimal number, where 0 is Sunday and 6 is Saturday (0, 1, ..., 6)
    "%u": "%u",  # Weekday as a decimal number, where 1 is Monday and 7 is Sunday (1, 2, ..., 7)
    "%U": "%U",  # Week number of the year (Sunday as the first day of the week) as a zero-padded decimal number (00, 01, ..., 53)
    "%W": "%W",  # Week number of the year (Monday as the first day of the week) as a zero-padded decimal number (00, 01, ..., 53)
    "%j": "%j",  # Day of the year as a zero-padded decimal number (001, 002, ..., 366)
    "%D": "%m/%d/%y",  # Month/day/year format (01/01/01)
    "%x": "%x",  # Locale's appropriate date representation
    "%F": "%Y-%m-%d",  # Year-month-day format (2001-01-01)
    # Time specifiers
    "%H": "%H",  # Hour (24-hour clock) as a zero-padded decimal number (00, 01, ..., 23)
    "%I": "%I",  # Hour (12-hour clock) as a zero-padded decimal number (01, 02, ..., 12)
    "%M": "%M",  # Minute as a zero-padded decimal number (00, 01, ..., 59)
    "%S": "%S",  # Second as a zero-padded decimal number (00, 01, ..., 59)
    "%f": "%f",  # Microsecond as a decimal number, zero-padded on the left (000000, 000001, ..., 999999)
    "%p": "%p",  # Locale's equivalent of either AM or PM
    "%R": "%H:%M",  # Hour:minute format (00:00)
    "%T": "%H:%M:%S",  # Hour:minute:second format (00:00:00)
    "%X": "%X",  # Locale's appropriate time representation
    "%r": "%I:%M:%S %p",  # Locale's 12-hour clock time (11:11:04 PM)
    # Timezone specifiers
    "%Z": "%Z",  # Time zone name (empty string if the object is naive)
    "%z": "%z",  # UTC offset in the form ±HHMM[SS[.ffffff]] (empty string if the object is naive)
    # Combined date and time specifiers
    "%c": "%c",  # Locale's appropriate date and time representation
    "%s": "%s",  # Unix timestamp (seconds since the epoch)
    # Special characters
    "%%": "%%",  # A literal '%' character
    "%t": "%t",  # Tab character
    "%n": "%n",  # Newline character
}

# Additional chrono-specific format codes that don't have direct Python equivalents
CHRONO_SPECIFIC_FORMATS: Dict[str, str] = {
    "%C": "%C",  # Century (year/100) as a decimal number
    "%q": "%q",  # Quarter of year (1-4)
    "%h": "%h",  # Same as %b
    "%e": "%e",  # Day of the month as a space-padded decimal number ( 1, 2, ..., 31)
    "%k": "%k",  # Hour (24-hour clock) as a space-padded decimal number ( 0, 1, ..., 23)
    "%l": "%l",  # Hour (12-hour clock) as a space-padded decimal number ( 1, 2, ..., 12)
    "%P": "%P",  # am or pm in 12-hour clocks
    "%.f": "%.f",  # Decimal fraction of a second
    "%3f": "%3f",  # Decimal fraction of a second with fixed length of 3
    "%6f": "%6f",  # Decimal fraction of a second with fixed length of 6
    "%9f": "%9f",  # Decimal fraction of a second with fixed length of 9
    "%:z": "%:z",  # Offset with colon
    "%::z": "%::z",  # Offset with seconds
    "%:::z": "%:::z",  # Offset without minutes
    "%#z": "%#z",  # Parsing only: allows minutes to be missing or present
    "%+": "%+",  # ISO 8601 / RFC 3339 date & time format
    "%G": "%G",  # Same as %Y but uses the year number in ISO 8601 week date
    "%g": "%g",  # Same as %y but uses the year number in ISO 8601 week date
    "%V": "%V",  # Same as %U but uses the week number in ISO 8601 week date
    "%v": "%v",  # Day-month-year format
}


def convert_python_to_chrono_format(python_format: str) -> str:
    """Convert Python datetime format string to Rust chrono format string.

    This function converts Python's strftime/strptime format codes to
    the equivalent Rust chrono format codes. The conversion handles
    the most common format codes used in datetime formatting.

    Args:
        python_format: A Python datetime format string using strftime codes

    Returns:
        A Rust chrono format string with equivalent format codes

    Raises:
        ValueError: If the format string contains unsupported format codes

    Examples:
        >>> convert_python_to_chrono_format("%Y-%m-%d %H:%M:%S")
        "%Y-%m-%d %H:%M:%S"
        >>> convert_python_to_chrono_format("%Y-%m-%dT%H:%M:%S.%f")
        "%Y-%m-%dT%H:%M:%S.%f"
        >>> convert_python_to_chrono_format("%Y-%m-%d %H:%M:%S %z")
        "%Y-%m-%d %H:%M:%S %z"
    """
    if not isinstance(python_format, str):
        raise ValueError("Format string must be a string")

    result = python_format

    # Handle the most common format codes
    for py_code, chrono_code in PYTHON_TO_CHRONO_FORMAT_MAP.items():
        result = result.replace(py_code, chrono_code)

    # Check for unsupported format codes
    unsupported_codes = []
    i = 0
    while i < len(result):
        if result[i] == "%":
            if i + 1 < len(result):
                code = result[i : i + 2]
                if (
                    code not in PYTHON_TO_CHRONO_FORMAT_MAP
                    and code not in CHRONO_SPECIFIC_FORMATS
                ):
                    unsupported_codes.append(code)
                i += 2
            else:
                # Incomplete format code at end of string
                unsupported_codes.append("%")
                break
        else:
            i += 1

    if unsupported_codes:
        raise ValueError(f"Unsupported format codes: {', '.join(unsupported_codes)}")

    return result


def is_chrono_format_supported(format_code: str) -> bool:
    """Check if a format code is supported by Rust chrono.

    Args:
        format_code: A format code (e.g., "%Y", "%H")

    Returns:
        True if the format code is supported by chrono, False otherwise
    """
    return (
        format_code in PYTHON_TO_CHRONO_FORMAT_MAP
        or format_code in CHRONO_SPECIFIC_FORMATS
    )


def get_supported_format_codes() -> Dict[str, str]:
    """Get all supported format codes and their descriptions.

    Returns:
        A dictionary mapping format codes to their descriptions
    """
    return {**PYTHON_TO_CHRONO_FORMAT_MAP, **CHRONO_SPECIFIC_FORMATS}


def validate_python_format(python_format: str) -> bool:
    """Validate that a Python format string can be converted to chrono format.

    Args:
        python_format: A Python datetime format string

    Returns:
        True if the format can be converted, False otherwise
    """
    try:
        convert_python_to_chrono_format(python_format)
        return True
    except ValueError:
        return False
