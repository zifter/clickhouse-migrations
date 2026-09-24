"""Lock, variable-substitution and output-format options shared by subcommands."""

import argparse
import logging
import os

from clickhouse_migrations.cli.argtypes import cast_to_bool, var_assignment
from clickhouse_migrations.cli.render import STATUS_FORMAT_TABLE, STATUS_FORMATS
from clickhouse_migrations.defaults import LOCK_TIMEOUT, LOCK_TTL
from clickhouse_migrations.substitution import parse_assignment

# Env counterpart of the repeatable --var: comma-separated NAME=VALUE pairs.
MIGRATION_VARS_ENV = "MIGRATION_VARS"


def add_lock_arguments(parser):
    parser.add_argument(
        "--lock",
        default=cast_to_bool(os.environ.get("LOCK", "0")),
        action=argparse.BooleanOptionalAction,
        help="Take a migration lock so concurrent runs cannot interleave "
        "(opt-in; needs a server with the KeeperMap engine and "
        "<keeper_map_path_prefix>, and fails if it is unavailable)",
    )
    parser.add_argument(
        "--lock-timeout",
        default=int(os.environ.get("LOCK_TIMEOUT", LOCK_TIMEOUT)),
        type=int,
        help="Seconds to wait for a lock held by another run, with --lock "
        f"(default: {LOCK_TIMEOUT}; 0 fails immediately)",
    )
    parser.add_argument(
        "--lock-ttl",
        default=int(os.environ.get("LOCK_TTL", LOCK_TTL)),
        type=int,
        help="Seconds after which a lock is considered stale and may be taken "
        f"over by another run, with --lock (default: {LOCK_TTL})",
    )


def add_substitution_arguments(parser):
    parser.add_argument(
        "--var",
        dest="variables",
        action="append",
        default=None,
        type=var_assignment,
        metavar="NAME=VALUE",
        help="Substitute ${NAME} in migration files with VALUE (repeatable). "
        "Enables substitution on its own; wins over --substitute-env. "
        f"Env: {MIGRATION_VARS_ENV}=A=1,B=2 (ignored when --var is given)",
    )
    parser.add_argument(
        "--substitute-env",
        default=cast_to_bool(os.environ.get("SUBSTITUTE_ENV", "0")),
        action=argparse.BooleanOptionalAction,
        help="Substitute ${NAME} in migration files from the process environment",
    )


def resolve_var_arguments(parser, ctx) -> None:
    """Turn the --var pairs (or MIGRATION_VARS) into a dict, None if unset."""
    pairs = ctx.variables
    if pairs is None:
        raw = os.environ.get(MIGRATION_VARS_ENV, "")
        items = [item for item in raw.split(",") if item]
        if items:
            try:
                pairs = [parse_assignment(item) for item in items]
            except ValueError as exc:
                parser.error(f"{MIGRATION_VARS_ENV}: {exc}")

    ctx.variables = dict(pairs) if pairs is not None else None


def warn_debug_with_substitution(ctx) -> None:
    """Our own logs never show substituted values, but the drivers' do."""
    if ctx.log_level == "DEBUG" and (ctx.variables is not None or ctx.substitute_env):
        logging.warning(
            "--log-level DEBUG with variable substitution: the driver's own "
            "debug log prints every statement as sent, substituted values "
            "included. Avoid DEBUG when a variable holds a secret."
        )


def add_format_argument(parser):
    parser.add_argument(
        "--format",
        default=os.environ.get("STATUS_FORMAT", STATUS_FORMAT_TABLE),
        choices=STATUS_FORMATS,
        help="Output format: table or json (json prints only the JSON document to stdout)",
    )
