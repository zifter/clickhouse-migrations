"""Compatibility module: the command line lives in ``clickhouse_migrations.cli``.

Everything that used to be importable from here still is, including the
``clickhouse_migrations.command_line:main`` console script and the names the
old single module imported. Assigning one of these attributes (for example
``monkeypatch.setattr(command_line, "create_cluster", fake)``) also rebinds
it in every ``cli`` module that holds the same object, so the patch reaches
the code that looks the name up, as it did when all of it lived here.
"""

import argparse
import importlib
import json
import logging
import math
import os
import pkgutil
import re
import sys
import types
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional, Tuple

from clickhouse_migrations import cli
from clickhouse_migrations.cli.app import SUBCOMMANDS, get_context, main
from clickhouse_migrations.cli.argtypes import (
    SETTING_NAME,
    cast_to_bool,
    log_level,
    migration_log_format,
    parse_setting,
    parse_settings,
    positive_float,
    var_assignment,
)
from clickhouse_migrations.cli.baseline import (
    BASELINE_COMMON_OPTIONS,
    do_baseline,
    run_baseline,
)
from clickhouse_migrations.cli.common import create_cluster
from clickhouse_migrations.cli.diff import DIFF_COMMON_OPTIONS, run_diff
from clickhouse_migrations.cli.down import do_rollback, rollback
from clickhouse_migrations.cli.dump import DUMP_COMMON_OPTIONS, run_dump
from clickhouse_migrations.cli.migrate import (
    do_migrate,
    do_query_applied_migrations,
    migrate,
)
from clickhouse_migrations.cli.new import create_migration
from clickhouse_migrations.cli.options import (
    MIGRATION_VARS_ENV,
    warn_debug_with_substitution,
)
from clickhouse_migrations.cli.render import (
    STATUS_FORMAT_JSON,
    STATUS_FORMAT_TABLE,
    STATUS_FORMATS,
    format_status,
    format_status_json,
)
from clickhouse_migrations.cli.repair import (
    REPAIR_COMMON_OPTIONS,
    do_repair,
    run_repair,
)
from clickhouse_migrations.cli.status import do_status, show_status, status_exit_code
from clickhouse_migrations.cli.unlock import unlock
from clickhouse_migrations.cli.validate import run_validate
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.connection import CLICKHOUSE_DRIVER, DRIVERS
from clickhouse_migrations.defaults import (
    DB_HOST,
    DB_PASSWORD,
    DB_USER,
    LOCK_TIMEOUT,
    LOCK_TTL,
    MIGRATIONS_DIR,
    MIGRATIONS_TABLE,
    MIGRATIONS_TABLE_ENGINE,
)
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration, MigrationStorage, slugify
from clickhouse_migrations.migrator import (
    MIGRATION_LOG_FORMATS,
    STATUS_MD5_MISMATCH,
    STATUS_PENDING,
    STATUS_UNKNOWN,
    Migrator,
    StatusRow,
)
from clickhouse_migrations.schema_diff import write_diff_migration
from clickhouse_migrations.schema_dump import diff_dumps, write_text_atomic
from clickhouse_migrations.substitution import parse_assignment
from clickhouse_migrations.validate import (
    format_validate,
    format_validate_json,
    validate_exit_code,
    validate_migrations,
)

# The public names of the old single module, i.e. what "import *" gave.
__all__ = [
    "argparse",
    "json",
    "logging",
    "math",
    "os",
    "re",
    "sys",
    "types",
    "ArgumentParser",
    "Path",
    "List",
    "Optional",
    "Tuple",
    "ClickhouseCluster",
    "CLICKHOUSE_DRIVER",
    "DRIVERS",
    "DB_HOST",
    "DB_PASSWORD",
    "DB_USER",
    "LOCK_TIMEOUT",
    "LOCK_TTL",
    "MIGRATIONS_DIR",
    "MIGRATIONS_TABLE",
    "MIGRATIONS_TABLE_ENGINE",
    "MigrationException",
    "Migration",
    "MigrationStorage",
    "slugify",
    "MIGRATION_LOG_FORMATS",
    "STATUS_MD5_MISMATCH",
    "STATUS_PENDING",
    "STATUS_UNKNOWN",
    "Migrator",
    "StatusRow",
    "write_diff_migration",
    "diff_dumps",
    "write_text_atomic",
    "parse_assignment",
    "format_validate",
    "format_validate_json",
    "validate_exit_code",
    "validate_migrations",
    "log_level",
    "migration_log_format",
    "cast_to_bool",
    "STATUS_FORMAT_TABLE",
    "STATUS_FORMAT_JSON",
    "STATUS_FORMATS",
    "SUBCOMMANDS",
    "SETTING_NAME",
    "var_assignment",
    "MIGRATION_VARS_ENV",
    "positive_float",
    "parse_setting",
    "parse_settings",
    "DUMP_COMMON_OPTIONS",
    "BASELINE_COMMON_OPTIONS",
    "REPAIR_COMMON_OPTIONS",
    "DIFF_COMMON_OPTIONS",
    "get_context",
    "create_cluster",
    "do_migrate",
    "do_query_applied_migrations",
    "do_status",
    "do_rollback",
    "do_baseline",
    "do_repair",
    "format_status",
    "format_status_json",
    "status_exit_code",
    "warn_debug_with_substitution",
    "migrate",
    "show_status",
    "run_baseline",
    "run_repair",
    "rollback",
    "unlock",
    "create_migration",
    "run_dump",
    "run_validate",
    "run_diff",
    "main",
]

# Every module of the cli package: where an assignment here is forwarded to.
CLI_MODULES = tuple(
    importlib.import_module(f"{cli.__name__}.{info.name}")
    for info in pkgutil.iter_modules(cli.__path__)
)

# The underscore names of the old module: (cli module, name there).
PRIVATE_NAMES = {
    "__version__": ("version", "__version__"),
    "_add_lock_arguments": ("options", "add_lock_arguments"),
    "_add_substitution_arguments": ("options", "add_substitution_arguments"),
    "_resolve_var_arguments": ("options", "resolve_var_arguments"),
    "_add_format_argument": ("options", "add_format_argument"),
    "_add_common_arguments": ("common", "add_common_arguments"),
    "_add_transport_arguments": ("common", "add_transport_arguments"),
    "_only_options": ("common", "only_options"),
    "_add_migrate_arguments": ("migrate", "add_arguments"),
    "_add_migrate_target_argument": ("migrate", "add_target_argument"),
    "_add_status_arguments": ("status", "add_arguments"),
    "_add_down_arguments": ("down", "add_arguments"),
    "_add_new_arguments": ("new", "add_arguments"),
    "_add_validate_arguments": ("validate", "add_arguments"),
    "_add_baseline_arguments": ("baseline", "add_arguments"),
    "_add_repair_arguments": ("repair", "add_arguments"),
    "_add_dump_arguments": ("dump", "add_arguments"),
    "_add_diff_arguments": ("diff", "add_arguments"),
    "_isoformat": ("render", "_isoformat"),
    "_print_rows": ("render", "print_rows"),
    "_generate_diff": ("diff", "_generate_diff"),
}
globals().update(
    {
        old: getattr(importlib.import_module(f"{cli.__name__}.{module}"), name)
        for old, (module, name) in PRIVATE_NAMES.items()
    }
)

_MISSING = object()


class _CommandLineModule(types.ModuleType):  # pylint: disable=too-few-public-methods
    """This module, forwarding attribute assignments to the cli modules."""

    def __setattr__(self, name, value):
        old = self.__dict__.get(name, _MISSING)
        if old is not _MISSING:
            names = {name, PRIVATE_NAMES.get(name, (None, name))[1]}
            for module in CLI_MODULES:
                for attr in names:
                    if module.__dict__.get(attr, _MISSING) is old:
                        setattr(module, attr, value)
        super().__setattr__(name, value)


sys.modules[__name__].__class__ = _CommandLineModule
