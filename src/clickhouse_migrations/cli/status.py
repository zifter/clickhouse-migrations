"""``status``: show applied vs pending migrations without applying anything."""

import argparse
import os
from typing import List

from clickhouse_migrations.cli import common
from clickhouse_migrations.cli.argtypes import cast_to_bool
from clickhouse_migrations.cli.options import add_format_argument
from clickhouse_migrations.cli.render import (
    STATUS_FORMAT_JSON,
    format_status,
    format_status_json,
)
from clickhouse_migrations.migrator import (
    STATUS_MD5_MISMATCH,
    STATUS_PENDING,
    STATUS_UNKNOWN,
    StatusRow,
)


def add_arguments(parser):
    parser.add_argument(
        "--strict",
        default=cast_to_bool(os.environ.get("STRICT", "0")),
        action=argparse.BooleanOptionalAction,
        help="Exit with code 1 if any migration is md5-mismatch or unknown",
    )
    parser.add_argument(
        "--exit-code-pending",
        default=cast_to_bool(os.environ.get("EXIT_CODE_PENDING", "0")),
        action=argparse.BooleanOptionalAction,
        help="Exit with code 1 if any migration is pending",
    )
    add_format_argument(parser)


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "status", help="Show applied vs pending migrations without applying anything"
    )
    common.add_common_arguments(parser)
    add_arguments(parser)
    return parser


def do_status(cluster, ctx) -> List[StatusRow]:
    return cluster.status(
        db_name=ctx.db_name,
        migration_path=ctx.migrations_dir,
        explicit_migrations=ctx.migrations,
    )


def status_exit_code(
    rows: List[StatusRow], strict: bool = False, exit_code_pending: bool = False
) -> int:
    failing = set()
    if strict:
        failing.update((STATUS_MD5_MISMATCH, STATUS_UNKNOWN))
    if exit_code_pending:
        failing.add(STATUS_PENDING)

    return 1 if any(row.state in failing for row in rows) else 0


def show_status(ctx) -> int:
    common.configure_logging(ctx)

    cluster = common.create_cluster(ctx)
    rows = do_status(cluster, ctx)
    if ctx.format == STATUS_FORMAT_JSON:
        db_name = ctx.db_name if ctx.db_name is not None else cluster.default_db_name
        print(format_status_json(db_name, rows))
    else:
        print(format_status(rows))
    return status_exit_code(rows, ctx.strict, ctx.exit_code_pending)
