"""``dump``: print the schema as portable, diffable SQL (or check it for drift)."""

import argparse
import logging
import os
import sys
from pathlib import Path

from clickhouse_migrations.cli import common
from clickhouse_migrations.cli.argtypes import cast_to_bool
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.schema_dump import diff_dumps, write_text_atomic

# Connection options "dump" shares with the other subcommands. It is read-only,
# so it takes none of the migrate/down options (dry-run, migrations-dir, ...).
DUMP_COMMON_OPTIONS = (
    "--db-url",
    "--db-host",
    "--db-port",
    "--driver",
    "--db-user",
    "--db-password",
    "--db-name",
    "--migrations-table",
    "--log-level",
    "--secure",
    "--ca-cert",
    "--cert",
    "--key",
    "--verify",
    "--connect-timeout",
    "--query-timeout",
    "--setting",
)


def add_arguments(parser):
    common.add_common_arguments(common.only_options(parser, DUMP_COMMON_OPTIONS))
    default_tables = os.environ.get("DUMP_TABLES", "")
    parser.add_argument(
        "--tables",
        default=default_tables.split(",") if default_tables else [],
        type=str,
        nargs="+",
        help="Dump only these tables/views/dictionaries (by name)",
    )
    parser.add_argument(
        "--keep-replicated-paths",
        default=cast_to_bool(os.environ.get("KEEP_REPLICATED_PATHS", "0")),
        action=argparse.BooleanOptionalAction,
        help="Keep the ZooKeeper path and replica name arguments of Replicated*MergeTree "
        "engines (by default they are removed so the dump is portable across clusters)",
    )
    parser.add_argument(
        "--include-migrations-table",
        default=cast_to_bool(os.environ.get("INCLUDE_MIGRATIONS_TABLE", "0")),
        action=argparse.BooleanOptionalAction,
        help="Also dump the migrations bookkeeping (and lock) table",
    )
    target = parser.add_mutually_exclusive_group()
    target.add_argument(
        "--out",
        default=None,
        type=Path,
        help="Write the dump to this file instead of stdout",
    )
    target.add_argument(
        "--check",
        default=None,
        type=Path,
        help="Compare the dump with this file; exit code 1 and a diff on stderr on drift",
    )


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "dump",
        help="Print the schema (tables, views, dictionaries) as portable, diffable SQL",
    )
    add_arguments(parser)
    return parser


def run_dump(ctx) -> int:
    """Print, write (--out) or verify (--check) the schema dump.

    Exit code: 0 on success (and, with --check, when there is no drift); 1 on
    drift or on any error. stdout only ever carries the SQL.
    """
    common.configure_logging(ctx)

    try:
        expected = None
        if ctx.check is not None:
            try:
                expected = ctx.check.read_text(encoding="utf8")
            except OSError as exc:
                raise MigrationException(
                    f"Cannot read the schema file {ctx.check}: {exc}"
                ) from exc

        cluster = common.create_cluster(ctx)
        db_name = ctx.db_name if ctx.db_name is not None else cluster.default_db_name
        text = cluster.dump(
            db_name=db_name,
            tables=ctx.tables,
            keep_replicated_paths=ctx.keep_replicated_paths,
            include_migrations_table=ctx.include_migrations_table,
        )

        if expected is not None:
            diff = diff_dumps(expected, text, str(ctx.check), f"database {db_name}")
            if diff:
                print(diff, file=sys.stderr)
                print(
                    f"Schema drift: {db_name} differs from {ctx.check}", file=sys.stderr
                )
                return 1
            print(f"Schema of {db_name} matches {ctx.check}", file=sys.stderr)
        elif ctx.out is not None:
            try:
                write_text_atomic(ctx.out, text)
            except OSError as exc:
                raise MigrationException(f"Cannot write {ctx.out}: {exc}") from exc
            print(f"Wrote the schema of {db_name} to {ctx.out}", file=sys.stderr)
        else:
            sys.stdout.write(text)
    except MigrationException as exc:
        logging.error("Dump failed: %s", exc)
        return 1
    return 0
