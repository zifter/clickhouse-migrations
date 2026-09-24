"""``migrate``: apply pending migrations (the default subcommand)."""

import argparse
import os
from typing import List

from clickhouse_migrations.cli import common
from clickhouse_migrations.cli.argtypes import cast_to_bool, migration_log_format
from clickhouse_migrations.cli.options import (
    add_lock_arguments,
    add_substitution_arguments,
    warn_debug_with_substitution,
)
from clickhouse_migrations.migration import Migration
from clickhouse_migrations.migrator import Migrator


def add_arguments(parser):
    parser.add_argument(
        "--multi-statement",
        default=cast_to_bool(os.environ.get("MULTI_STATEMENT", "1")),
        action=argparse.BooleanOptionalAction,
        help="Treat each migration file as multiple ';'-separated statements",
    )
    parser.add_argument(
        "--migration-log-format",
        default=os.environ.get("MIGRATION_LOG_FORMAT", "full"),
        type=migration_log_format,
        help="Migration log format: full or compact",
    )
    parser.add_argument(
        "--dry-run",
        default=cast_to_bool(os.environ.get("DRY_RUN", "0")),
        action=argparse.BooleanOptionalAction,
        help="Dry run mode",
    )
    parser.add_argument(
        "--fake",
        default=cast_to_bool(os.environ.get("FAKE", "0")),
        action=argparse.BooleanOptionalAction,
        help="Marks the migrations as applied, "
        "but without actually running the SQL to change your database schema.",
    )
    parser.add_argument(
        "--create-db-if-not-exists",
        default=cast_to_bool(os.environ.get("CREATE_DB_IF_NOT_EXISTS", "1")),
        action=argparse.BooleanOptionalAction,
        help="Create database if it does not exist",
    )


def add_target_argument(parser):
    # No environment variable, same as "down --to".
    parser.add_argument(
        "--to",
        dest="to_version",
        default=None,
        type=int,
        help="Apply pending migrations only up to and including this version. "
        "Fails if it is below the highest applied version (use down) "
        "and cannot be combined with --migrations.",
    )


def add_parser(subparsers):
    parser = subparsers.add_parser("migrate", help="Apply pending migrations (default)")
    common.add_common_arguments(parser)
    add_arguments(parser)
    add_target_argument(parser)
    add_lock_arguments(parser)
    add_substitution_arguments(parser)
    return parser


def do_migrate(cluster, ctx) -> List[Migration]:
    return cluster.migrate(
        db_name=ctx.db_name,
        migration_path=ctx.migrations_dir,
        explicit_migrations=ctx.migrations,
        cluster_name=ctx.cluster_name,
        create_db_if_no_exists=ctx.create_db_if_not_exists,
        multi_statement=ctx.multi_statement,
        dryrun=ctx.dry_run,
        fake=ctx.fake,
        migration_log_format=ctx.migration_log_format,
        to_version=ctx.to_version,
        lock=ctx.lock,
        lock_timeout=ctx.lock_timeout,
        lock_ttl=ctx.lock_ttl,
        variables=ctx.variables,
        substitute_env=ctx.substitute_env,
    )


def do_query_applied_migrations(cluster, ctx) -> List[Migration]:
    with cluster.connection(ctx.db_name) as conn:
        migrator = Migrator(conn, True, migrations_table=ctx.migrations_table)
        return migrator.query_applied_migrations()


def migrate(ctx) -> List[Migration]:
    common.configure_logging(ctx)
    warn_debug_with_substitution(ctx)

    cluster = common.create_cluster(ctx)
    migrations = do_migrate(cluster, ctx)
    return migrations
