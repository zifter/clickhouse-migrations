import logging

import pytest

from clickhouse_migrations.cmd import log_level


def test_valid_log_levels():
    # Test upper (canonical) case input
    for level in logging._nameToLevel:  # pylint: disable=W0212
        assert log_level(level.upper()) == level
    # Test lower case input
    for level in logging._nameToLevel:  # pylint: disable=W0212
        assert log_level(level.lower()) == level
    # Test mixed case input
    for level in logging._nameToLevel:  # pylint: disable=W0212
        level_char_list = []
        for i, ch in enumerate(level):
            ch = chr(ord(ch) | 0b0010_0000) if (i % 2) == 0 else ch
            level_char_list.append(ch)
        mixed_case_level = "".join(level_char_list)
        assert log_level(mixed_case_level) == level
    # Test mixed case input
    for level in logging._nameToLevel:  # pylint: disable=W0212
        level_char_list = []
        for i, ch in enumerate(level):
            ch = chr(ord(ch) | 0b0010_0000) if (i % 2) != 0 else ch
            level_char_list.append(ch)
        mixed_case_level = "".join(level_char_list)
        assert log_level(mixed_case_level) == level


def test_invalid_log_levels():
    bad_input = (
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
    )
    with pytest.raises(ValueError):
        for value in bad_input:
            log_level(value)
