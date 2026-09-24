"""Argument value types and the parsing of environment-variable defaults."""

import argparse
import logging
import math
import re
from typing import List, Tuple

from clickhouse_migrations.migrator import MIGRATION_LOG_FORMATS
from clickhouse_migrations.substitution import parse_assignment

SETTING_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def log_level(value: str) -> str:
    if hasattr(logging, "getLevelNamesMapping"):
        # New api in python 3.11
        level_list = logging.getLevelNamesMapping().keys()
    else:
        level_list = logging._nameToLevel.keys()  # pylint: disable=W0212

    if value.upper() in level_list:
        return value.upper()

    raise ValueError


def migration_log_format(value: str) -> str:
    normalized_value = value.lower()
    if normalized_value in MIGRATION_LOG_FORMATS:
        return normalized_value
    raise ValueError(
        f"Unknown migration log format: {value}. Expected one of: {', '.join(MIGRATION_LOG_FORMATS)}"
    )


def cast_to_bool(value: str):
    return value.lower() in ("1", "true", "yes", "y")


def var_assignment(value: str) -> Tuple[str, str]:
    try:
        return parse_assignment(value)
    except ValueError as exc:
        # ArgumentTypeError prints only this message, never the value itself,
        # which may be a secret.
        raise argparse.ArgumentTypeError(str(exc)) from exc


def positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        number = math.nan
    if not (math.isfinite(number) and number > 0):
        raise argparse.ArgumentTypeError(f"expected a positive number, got {value!r}")
    return number


def parse_setting(item: str) -> Tuple[str, str]:
    """Parse one ``name=value`` ClickHouse setting; the value is kept as text."""
    name, sep, value = item.partition("=")
    name = name.strip()
    if not sep or not SETTING_NAME.fullmatch(name):
        raise argparse.ArgumentTypeError(
            f"expected a ClickHouse setting as name=value, got {item!r}"
        )
    return name, value.strip()


def parse_settings(text: str) -> List[Tuple[str, str]]:
    """Parse the comma-separated ``name=value`` list of CLICKHOUSE_SETTINGS."""
    return [parse_setting(item) for item in text.split(",") if item.strip()]
