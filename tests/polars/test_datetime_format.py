"""Tests for datetime_format module."""

import pytest

from fastdataframe.polars.datetime_format import (
    convert_python_to_chrono_format,
    is_chrono_format_supported,
    get_supported_format_codes,
    validate_python_format,
)


class TestDatetimeFormatConversion:
    """Test datetime format conversion functionality."""

    def test_basic_date_formats(self):
        """Test basic date format conversions."""
        # Test common date formats
        assert convert_python_to_chrono_format("%Y-%m-%d") == "%Y-%m-%d"
        assert convert_python_to_chrono_format("%d/%m/%Y") == "%d/%m/%Y"
        assert (
            convert_python_to_chrono_format("%Y-%m-%d %H:%M:%S") == "%Y-%m-%d %H:%M:%S"
        )
        assert (
            convert_python_to_chrono_format("%Y-%m-%dT%H:%M:%S") == "%Y-%m-%dT%H:%M:%S"
        )

    def test_time_formats(self):
        """Test time format conversions."""
        assert convert_python_to_chrono_format("%H:%M:%S") == "%H:%M:%S"
        assert convert_python_to_chrono_format("%I:%M:%S %p") == "%I:%M:%S %p"
        assert convert_python_to_chrono_format("%H:%M") == "%H:%M"

    def test_timezone_formats(self):
        """Test timezone format conversions."""
        assert (
            convert_python_to_chrono_format("%Y-%m-%d %H:%M:%S %z")
            == "%Y-%m-%d %H:%M:%S %z"
        )
        assert (
            convert_python_to_chrono_format("%Y-%m-%d %H:%M:%S %Z")
            == "%Y-%m-%d %H:%M:%S %Z"
        )

    def test_weekday_formats(self):
        """Test weekday format conversions."""
        assert convert_python_to_chrono_format("%A, %B %d, %Y") == "%A, %B %d, %Y"
        assert convert_python_to_chrono_format("%a %b %d") == "%a %b %d"

    def test_special_formats(self):
        """Test special format conversions."""
        assert (
            convert_python_to_chrono_format("%Y-%m-%d %H:%M:%S.%f")
            == "%Y-%m-%d %H:%M:%S.%f"
        )
        assert convert_python_to_chrono_format("%s") == "%s"  # Unix timestamp
        assert convert_python_to_chrono_format("%c") == "%c"  # Locale date/time

    def test_escape_sequences(self):
        """Test escape sequence handling."""
        assert convert_python_to_chrono_format("%%") == "%%"  # Literal percent
        assert convert_python_to_chrono_format("%t") == "%t"  # Tab
        assert convert_python_to_chrono_format("%n") == "%n"  # Newline

    def test_complex_formats(self):
        """Test complex format strings."""
        complex_format = "%Y-%m-%dT%H:%M:%S.%f%z"
        expected = "%Y-%m-%dT%H:%M:%S.%f%z"
        assert convert_python_to_chrono_format(complex_format) == expected

        complex_format2 = "%A, %B %d, %Y at %I:%M %p"
        expected2 = "%A, %B %d, %Y at %I:%M %p"
        assert convert_python_to_chrono_format(complex_format2) == expected2

    def test_unsupported_formats(self):
        """Test handling of unsupported format codes."""
        with pytest.raises(ValueError, match="Unsupported format codes: %Q"):
            convert_python_to_chrono_format("%Y-%m-%d %Q")

        with pytest.raises(ValueError, match="Unsupported format codes: %L"):
            convert_python_to_chrono_format("%L")

    def test_invalid_input(self):
        """Test handling of invalid input."""
        with pytest.raises(ValueError, match="Format string must be a string"):
            convert_python_to_chrono_format(123)

        with pytest.raises(ValueError, match="Unsupported format codes: %"):
            convert_python_to_chrono_format("%")

    def test_is_chrono_format_supported(self):
        """Test format code support checking."""
        assert is_chrono_format_supported("%Y") is True
        assert is_chrono_format_supported("%H") is True
        assert is_chrono_format_supported("%m") is True
        assert is_chrono_format_supported("%d") is True
        assert is_chrono_format_supported("%f") is True
        assert is_chrono_format_supported("%z") is True
        assert is_chrono_format_supported("%Z") is True

        # Test chrono-specific formats
        assert is_chrono_format_supported("%C") is True
        assert is_chrono_format_supported("%q") is True
        assert is_chrono_format_supported("%e") is True
        assert is_chrono_format_supported("%k") is True
        assert is_chrono_format_supported("%l") is True
        assert is_chrono_format_supported("%P") is True
        assert is_chrono_format_supported("%:z") is True
        assert is_chrono_format_supported("%+") is True

        # Test unsupported formats
        assert is_chrono_format_supported("%Q") is False
        assert is_chrono_format_supported("%L") is False

    def test_get_supported_format_codes(self):
        """Test getting supported format codes."""
        supported_codes = get_supported_format_codes()

        # Check that common formats are included
        assert "%Y" in supported_codes
        assert "%H" in supported_codes
        assert "%m" in supported_codes
        assert "%d" in supported_codes
        assert "%f" in supported_codes
        assert "%z" in supported_codes

        # Check that chrono-specific formats are included
        assert "%C" in supported_codes
        assert "%q" in supported_codes
        assert "%e" in supported_codes
        assert "%k" in supported_codes
        assert "%l" in supported_codes
        assert "%P" in supported_codes
        assert "%:z" in supported_codes
        assert "%+" in supported_codes

    def test_validate_python_format(self):
        """Test format validation."""
        # Valid formats
        assert validate_python_format("%Y-%m-%d") is True
        assert validate_python_format("%H:%M:%S") is True
        assert validate_python_format("%Y-%m-%d %H:%M:%S %z") is True
        assert validate_python_format("%A, %B %d, %Y") is True

        # Invalid formats
        assert validate_python_format("%Y-%m-%d %Q") is False
        assert validate_python_format("%L") is False
        assert validate_python_format("%") is False

    def test_edge_cases(self):
        """Test edge cases and boundary conditions."""
        # Empty string
        assert convert_python_to_chrono_format("") == ""

        # String with no format codes
        assert convert_python_to_chrono_format("Hello World") == "Hello World"

        # String with literal percent signs
        assert convert_python_to_chrono_format("100%% complete") == "100%% complete"

        # Multiple consecutive format codes
        assert convert_python_to_chrono_format("%Y%m%d") == "%Y%m%d"
