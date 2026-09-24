"""``baseline``: record migrations up to --to as applied without executing them."""

import argparse
import logging
import os
from typing import List

from clickhouse_migrations.cli import common
from clickhouse_migrations.cli.argtypes import cast_to_bool
from clickhouse_migrations.cli.dump import DUMP_COMMON_OPTIONS
from clickhouse_migrations.cli.options import add_format_argument, add_lock_arguments
from clickhouse_migrations.cli.render import print_rows
from clickhouse_migrations.migrator import StatusRow

# baseline and repair work on the whole migrations directory, so they take the
# common options except --migrations (a partial list would make every other
# applied migration look "unknown" to repair).
BASELINE_COMMON_OPTIONS = DUMP_COMMON_OPTIONS + (
    "--migrations-dir",
    "--cluster-name",
    "--migrations-table-engine",
)


def add_arguments(parser):
    common.add_common_arguments(common.only_options(parser, BASELINE_COMMON_OPTIONS))
    # Required and without an environment variable, like "migrate --to".
    parser.add_argument(
        "--to",
        dest="to_version",
        required=True,
        type=int,
        help="Record every local migration up to and including this version "
        "as applied, without executing anything",
    )
    parser.add_argument(
        "--dry-run",
        default=cast_to_bool(os.environ.get("DRY_RUN", "0")),
        action=argparse.BooleanOptionalAction,
        help="Print what would be recorded and write nothing",
    )
    parser.add_argument(
        "--create-db-if-not-exists",
        default=cast_to_bool(os.environ.get("CREATE_DB_IF_NOT_EXISTS", "1")),
        action=argparse.BooleanOptionalAction,
        help="Create database if it does not exist",
    )
    add_format_argument(parser)
    add_lock_arguments(parser)


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "baseline",
        help="Adopt an existing database: record migrations up to --to as applied "
        "without executing them",
    )
    add_arguments(parser)
    return parser


def do_baseline(cluster, ctx) -> List[StatusRow]:
    return cluster.baseline(
        db_name=ctx.db_name,
        migration_path=ctx.migrations_dir,
        to_version=ctx.to_version,
        cluster_name=ctx.cluster_name,
        create_db_if_no_exists=ctx.create_db_if_not_exists,
        dryrun=ctx.dry_run,
        lock=ctx.lock,
        lock_timeout=ctx.lock_timeout,
        lock_ttl=ctx.lock_ttl,
    )


def run_baseline(ctx) -> int:
    """Record migrations up to --to as applied; print the resulting status."""
    common.configure_logging(ctx)

    cluster = common.create_cluster(ctx)
    rows = do_baseline(cluster, ctx)
    if ctx.dry_run:
        logging.warning("Dry run: nothing was recorded, the status would become:")
    print_rows(ctx, cluster, rows, "No migrations found.")
    return 0
