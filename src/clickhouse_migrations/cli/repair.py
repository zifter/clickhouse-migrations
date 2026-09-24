"""``repair``: report (or with --write fix) applied migrations whose file changed."""

import logging
from typing import List

from clickhouse_migrations.cli import common
from clickhouse_migrations.cli.dump import DUMP_COMMON_OPTIONS
from clickhouse_migrations.cli.options import add_format_argument, add_lock_arguments
from clickhouse_migrations.cli.render import print_rows
from clickhouse_migrations.migrator import StatusRow

# Like baseline (see BASELINE_COMMON_OPTIONS): the whole directory, no --migrations.
REPAIR_COMMON_OPTIONS = DUMP_COMMON_OPTIONS + ("--migrations-dir",)


def add_arguments(parser):
    common.add_common_arguments(common.only_options(parser, REPAIR_COMMON_OPTIONS))
    # Deliberately no environment variables: rewriting the bookkeeping must
    # be asked for on the command line.
    parser.add_argument(
        "--write",
        default=False,
        action="store_true",
        help="Apply the repair; without it only report what is out of sync",
    )
    parser.add_argument(
        "--version",
        dest="versions",
        default=[],
        action="append",
        type=int,
        help="Repair only this version (repeatable); it must be md5-mismatch or unknown",
    )
    parser.add_argument(
        "--prune",
        default=False,
        action="store_true",
        help="With --write, also delete the rows of unknown migrations "
        "(applied, but no local file any more)",
    )
    add_format_argument(parser)
    add_lock_arguments(parser)


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "repair",
        help="Report (or with --write fix) applied migrations whose file changed",
    )
    add_arguments(parser)
    return parser


def do_repair(cluster, ctx) -> List[StatusRow]:
    return cluster.repair(
        db_name=ctx.db_name,
        migration_path=ctx.migrations_dir,
        versions=ctx.versions,
        write=ctx.write,
        prune=ctx.prune,
        lock=ctx.lock,
        lock_timeout=ctx.lock_timeout,
        lock_ttl=ctx.lock_ttl,
    )


def run_repair(ctx) -> int:
    """Report (exit code 1 if anything is out of sync) or fix with --write."""
    common.configure_logging(ctx)

    cluster = common.create_cluster(ctx)
    rows = do_repair(cluster, ctx)
    print_rows(
        ctx,
        cluster,
        rows,
        "Nothing to repair: every applied migration matches its local file.",
    )
    if ctx.write:
        return 0
    if rows:
        logging.warning("Run again with --write to repair (and --prune for unknown).")
        return 1
    return 0
