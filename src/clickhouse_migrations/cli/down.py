"""``down``: roll back applied migrations using their .down.sql files."""

import argparse
import os
from typing import List

from clickhouse_migrations.cli import common
from clickhouse_migrations.cli.argtypes import cast_to_bool
from clickhouse_migrations.cli.options import (
    add_lock_arguments,
    add_substitution_arguments,
    warn_debug_with_substitution,
)


def add_arguments(parser):
    parser.add_argument(
        "--steps",
        default=1,
        type=int,
        help="Number of most recent applied migrations to roll back (default: 1)",
    )
    parser.add_argument(
        "--to",
        dest="to_version",
        default=None,
        type=int,
        help="Roll back every applied migration with a version greater than this. "
        "Takes precedence over --steps.",
    )
    parser.add_argument(
        "--dry-run",
        default=cast_to_bool(os.environ.get("DRY_RUN", "0")),
        action=argparse.BooleanOptionalAction,
        help="Dry run mode",
    )
    parser.add_argument(
        "--multi-statement",
        default=cast_to_bool(os.environ.get("MULTI_STATEMENT", "1")),
        action=argparse.BooleanOptionalAction,
        help="Treat each down migration file as multiple ';'-separated statements",
    )


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "down",
        help="Roll back applied migrations using their .down.sql files",
    )
    common.add_common_arguments(parser)
    add_arguments(parser)
    add_lock_arguments(parser)
    add_substitution_arguments(parser)
    return parser


def do_rollback(cluster, ctx) -> List[int]:
    return cluster.rollback(
        db_name=ctx.db_name,
        migration_path=ctx.migrations_dir,
        steps=ctx.steps,
        to_version=ctx.to_version,
        dryrun=ctx.dry_run,
        multi_statement=ctx.multi_statement,
        lock=ctx.lock,
        lock_timeout=ctx.lock_timeout,
        lock_ttl=ctx.lock_ttl,
        variables=ctx.variables,
        substitute_env=ctx.substitute_env,
    )


def rollback(ctx) -> List[int]:
    common.configure_logging(ctx)
    warn_debug_with_substitution(ctx)

    cluster = common.create_cluster(ctx)
    return do_rollback(cluster, ctx)
