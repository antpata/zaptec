"""Tests for zaptec/utils.py."""

from custom_components.zaptec.zaptec.utils import (
    get_ocmf_max_reader_value,
    to_under,
)


def test_utils_get_ocmf_max_reader_value() -> None:
    """Test the get_ocmf_max_reader_value function."""
    data = {
        "RD": [
            {"RV": 100.0},
            {"RV": 150.5},
            {"RV": 120.3},
        ]
    }
    assert get_ocmf_max_reader_value(data) == 150.5  # noqa: PLR2004

    data_none = None
    assert get_ocmf_max_reader_value(data_none) == 0.0

    data_empty = {"RD": []}
    assert get_ocmf_max_reader_value(data_empty) == 0.0

    data_no_rd = {}
    assert get_ocmf_max_reader_value(data_no_rd) == 0.0

    data_missing_rv = {"RD": [{"RI": "1-0:1.8.0"}]}
    assert get_ocmf_max_reader_value(data_missing_rv) == 0.0


def test_utils_to_under() -> None:
    """Test the to_under function."""
    assert to_under("HelloWorld") == "hello_world"
    assert to_under("helloWorld") == "hello_world"
    assert to_under("Hello") == "hello"
    assert to_under("H") == "h"
    assert to_under("") == ""
    assert to_under("ThisIsATest") == "this_is_a_test"
    assert to_under("already_under") == "already_under"
    assert to_under("with123Numbers") == "with123_numbers"
    assert to_under("with_Special$Chars!") == "with_special$chars!"
