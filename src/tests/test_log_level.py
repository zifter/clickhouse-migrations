import logging

import pytest

from clickhouse_migrations.command_line import log_level


@pytest.mark.parametrize(
    "good_input,expected",
    (
        *((v.lower(), v) for v in logging._nameToLevel),  # pylint: disable=W0212
        *((v.upper(), v) for v in logging._nameToLevel),  # pylint: disable=W0212
        ("wArN", "WARN"),
        ("errOR", "ERROR"),
        ("InFO", "INFO"),
    ),
)
def test_valid_log_levels(good_input, expected):
    assert log_level(good_input) == expected


@pytest.mark.parametrize(
    "bad_input",
    (
        "",
        " ",
        "FOO",
        "BAR",
        "BAZ",
        " WARNINGG",
        "WARNIN",
        "WARN ",
        " WARN",
        " WARN ",
    ),
)
def test_invalid_log_levels(bad_input):
    with pytest.raises(ValueError):
        log_level(bad_input)
