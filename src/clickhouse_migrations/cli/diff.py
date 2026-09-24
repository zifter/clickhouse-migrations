"""``diff``: generate a migration that turns the database into a target schema."""

import argparse
import logging
import os
import sys
from pathlib import Path

from clickhouse_migrations.cli import common
from clickhouse_migrations.cli.argtypes import cast_to_bool
from clickhouse_migrations.cli.dump import DUMP_COMMON_OPTIONS
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import slugify
from clickhouse_migrations.schema_diff import write_diff_migration

# "diff" connects like "dump" and writes into the migrations directory.
DIFF_COMMON_OPTIONS = DUMP_COMMON_OPTIONS + ("--migrations-dir",)


def add_arguments(parser):
    common.add_common_arguments(common.only_options(parser, DIFF_COMMON_OPTIONS))
    parser.add_argument(
        "--to",
        dest="target",
        required=True,
        type=Path,
        help="Target schema file (dump output or equivalent CREATE statements)",
    )
    parser.add_argument(
        "--name",
        default=os.environ.get("DIFF_NAME", "diff"),
        help="Name of the generated migration file, NNN_<name>.sql (default: diff)",
    )
    parser.add_argument(
        "--allow-destructive",
        default=cast_to_bool(os.environ.get("ALLOW_DESTRUCTIVE", "0")),
        action=argparse.BooleanOptionalAction,
        help="Emit DROP TABLE/VIEW/DICTIONARY/COLUMN/INDEX as real statements "
        "(by default they are commented out)",
    )
    parser.add_argument(
        "--dry-run",
        default=cast_to_bool(os.environ.get("DRY_RUN", "0")),
        action=argparse.BooleanOptionalAction,
        help="Print the migration to stdout instead of writing a file",
    )


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "diff",
        help="Generate a migration that turns the database into a target "
        "schema file",
    )
    add_arguments(parser)
    return parser


def _generate_diff(ctx) -> int:
    slugify(ctx.name)
    try:
        schema = ctx.target.read_text(encoding="utf8")
    except OSError as exc:
        raise MigrationException(
            f"Cannot read the schema file {ctx.target}: {exc}"
        ) from exc

    cluster = common.create_cluster(ctx)
    db_name = ctx.db_name if ctx.db_name is not None else cluster.default_db_name
    plan = cluster.diff_plan(schema, db_name=db_name)
    if plan.is_empty:
        print(f"No changes: {db_name} matches {ctx.target}", file=sys.stderr)
        return 0

    for refusal in plan.refusals:
        logging.warning("Refused: %s", refusal)
    if not plan.changes:
        print("No migration generated: every change was refused", file=sys.stderr)
    elif ctx.dry_run:
        sys.stdout.write(plan.render(ctx.allow_destructive))
    else:
        path = write_diff_migration(
            ctx.migrations_dir, plan.render(ctx.allow_destructive), ctx.name
        )
        print(f"Created {path}")
        if plan.has_destructive and not ctx.allow_destructive:
            print(
                "Destructive statements are commented out "
                "(review them, or rerun with --allow-destructive)",
                file=sys.stderr,
            )
    return 1 if plan.refusals else 0


def run_diff(ctx) -> int:
    """Generate (or, with --dry-run, print) the migration towards ``--to``.

    Exit code: 0 when there is nothing to do or everything was generated; 1
    when a change was refused (the file, if any, then holds only the
    supported part) or on any error. Nothing is ever applied.
    """
    common.configure_logging(ctx)
    try:
        return _generate_diff(ctx)
    except MigrationException as exc:
        logging.error("Diff failed: %s", exc)
        return 1
