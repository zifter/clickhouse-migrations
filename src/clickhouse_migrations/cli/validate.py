"""``validate``: check the migrations directory offline, without any database."""

import argparse
import logging
import os
from pathlib import Path

from clickhouse_migrations.cli.render import (
    STATUS_FORMAT_JSON,
    STATUS_FORMAT_TABLE,
    STATUS_FORMATS,
)
from clickhouse_migrations.defaults import MIGRATIONS_DIR
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.validate import (
    format_validate,
    format_validate_json,
    validate_exit_code,
    validate_migrations,
)


def add_arguments(parser):
    # Like "new", "validate" never connects to ClickHouse: no database options.
    parser.add_argument(
        "--dir",
        "--migrations-dir",
        dest="migrations_dir",
        default=os.environ.get("MIGRATIONS_DIR", MIGRATIONS_DIR),
        type=Path,
        help="Path to the directory with migration files",
    )
    parser.add_argument(
        "--strict",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Exit with code 1 on warnings too, not only on errors",
    )
    parser.add_argument(
        "--require-down",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Report a migration without a paired .down.sql as an error",
    )
    parser.add_argument(
        "--format",
        default=STATUS_FORMAT_TABLE,
        choices=STATUS_FORMATS,
        help="Output format: table or json (json prints only the JSON document to stdout)",
    )


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "validate",
        help="Check the migrations directory offline, without any database",
    )
    add_arguments(parser)
    return parser


def run_validate(ctx) -> int:
    """Print the validate report; exit code 1 on errors (or warnings with --strict)."""
    try:
        report = validate_migrations(ctx.migrations_dir, require_down=ctx.require_down)
    except MigrationException as exc:
        logging.error("Validation failed: %s", exc)
        return 1

    if ctx.format == STATUS_FORMAT_JSON:
        print(format_validate_json(report))
    else:
        print(format_validate(report))
    return validate_exit_code(report, ctx.strict)
