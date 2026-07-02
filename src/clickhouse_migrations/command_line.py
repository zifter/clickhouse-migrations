import argparse
import logging
import os
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import List

from clickhouse_migrations import __version__
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.defaults import (
    DB_HOST,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
    MIGRATIONS_DIR,
)
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration
from clickhouse_migrations.migrator import MIGRATION_LOG_FORMATS, Migrator, StatusRow


def log_level(value: str) -> str:
    if hasattr(logging, "getLevelNamesMapping"):
        # New api in python 3.11
        level_list = logging.getLevelNamesMapping().keys()
    else:
        level_list = logging._nameToLevel.keys()  # pylint: disable=W0212

    if value.upper() in level_list:
        return value.upper()

    raise ValueError


def migration_log_format(value: str) -> str:
    normalized_value = value.lower()
    if normalized_value in MIGRATION_LOG_FORMATS:
        return normalized_value
    raise ValueError(
        f"Unknown migration log format: {value}. Expected one of: {', '.join(MIGRATION_LOG_FORMATS)}"
    )


def cast_to_bool(value: str):
    return value.lower() in ("1", "true", "yes", "y")


SUBCOMMANDS = ("migrate", "status")


def _add_common_arguments(parser):
    parser.add_argument(
        "--db-url",
        default=os.environ.get("DB_URL", None),
        help="Clickhouse connection URL (clickhouse://user:password@host:port/db)",
    )
    parser.add_argument(
        "--db-host",
        default=os.environ.get("DB_HOST", DB_HOST),
        help="Clickhouse database hostname",
    )
    parser.add_argument(
        "--db-port",
        default=os.environ.get("DB_PORT", DB_PORT),
        help="Clickhouse database port",
    )
    parser.add_argument(
        "--db-user",
        default=os.environ.get("DB_USER", DB_USER),
        help="Clickhouse user",
    )
    parser.add_argument(
        "--db-password",
        default=os.environ.get("DB_PASSWORD", DB_PASSWORD),
        help="Clickhouse password",
    )
    parser.add_argument(
        "--db-name",
        default=os.environ.get("DB_NAME", None),
        help="Clickhouse database name",
    )
    parser.add_argument(
        "--migrations-dir",
        default=os.environ.get("MIGRATIONS_DIR", MIGRATIONS_DIR),
        type=Path,
        help="Path to the directory with migration files",
    )
    parser.add_argument(
        "--cluster-name",
        default=os.environ.get("CLUSTER_NAME", None),
        help="Clickhouse topology cluster",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "WARNING"),
        type=log_level,
        help="Log level",
    )
    parser.add_argument(
        "--secure",
        default=cast_to_bool(os.environ.get("SECURE", "0")),
        action=argparse.BooleanOptionalAction,
        help="Use secure connection",
    )
    default_migrations = os.environ.get("MIGRATIONS", "")
    parser.add_argument(
        "--migrations",
        default=default_migrations.split(",") if default_migrations else [],
        type=str,
        nargs="+",
        help="Explicit list of migrations to apply. "
        "Specify file name, file stem or migration version like 001_init.sql, 002_test2, 003, 4",
    )


def _add_migrate_arguments(parser):
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


def get_context(args):
    parser = ArgumentParser(prog="clickhouse-migrations")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command")
    migrate_parser = subparsers.add_parser(
        "migrate", help="Apply pending migrations (default)"
    )
    _add_common_arguments(migrate_parser)
    _add_migrate_arguments(migrate_parser)

    status_parser = subparsers.add_parser(
        "status", help="Show applied vs pending migrations without applying anything"
    )
    _add_common_arguments(status_parser)

    # Default to the "migrate" subcommand so existing invocations
    # (clickhouse-migrations <flags>) keep working unchanged.
    args = list(args)
    if not args or (
        args[0] not in SUBCOMMANDS and args[0] not in ("-h", "--help", "--version")
    ):
        args = ["migrate", *args]

    return parser.parse_args(args)


def create_cluster(ctx) -> ClickhouseCluster:
    return ClickhouseCluster(
        db_host=ctx.db_host,
        db_port=ctx.db_port,
        db_user=ctx.db_user,
        db_password=ctx.db_password,
        db_url=ctx.db_url,
        secure=ctx.secure,
    )


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
    )


def do_query_applied_migrations(cluster, ctx) -> List[Migration]:
    with cluster.connection(ctx.db_name) as conn:
        migrator = Migrator(conn, True)
        return migrator.query_applied_migrations()


def do_status(cluster, ctx) -> List[StatusRow]:
    return cluster.status(
        db_name=ctx.db_name,
        migration_path=ctx.migrations_dir,
        explicit_migrations=ctx.migrations,
    )


def format_status(rows: List[StatusRow]) -> str:
    if not rows:
        return "No migrations found."

    table = [("VERSION", "STATUS", "MD5", "APPLIED AT")]
    for row in rows:
        table.append(
            (
                str(row.version),
                row.state,
                row.md5 or "",
                str(row.applied_at) if row.applied_at is not None else "",
            )
        )

    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    return "\n".join(
        "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)) for row in table
    )


def migrate(ctx) -> List[Migration]:
    logging.basicConfig(level=ctx.log_level, style="{", format="{levelname}:{message}")

    cluster = create_cluster(ctx)
    migrations = do_migrate(cluster, ctx)
    return migrations


def show_status(ctx) -> List[StatusRow]:
    logging.basicConfig(level=ctx.log_level, style="{", format="{levelname}:{message}")

    cluster = create_cluster(ctx)
    rows = do_status(cluster, ctx)
    print(format_status(rows))
    return rows


def main() -> int:
    ctx = get_context(sys.argv[1:])
    try:
        if ctx.command == "status":
            show_status(ctx)
        else:
            migrate(ctx)
    except MigrationException as exc:
        logging.error("Migration failed: %s", exc)
        return 1
    return 0
