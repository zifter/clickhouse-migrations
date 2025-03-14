import argparse
import logging
import os
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import List

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.defaults import (
    DB_HOST,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
    MIGRATIONS_DIR,
)
from clickhouse_migrations.migration import Migration
from clickhouse_migrations.migrator import Migrator


def log_level(value: str) -> str:
    if hasattr(logging, "getLevelNamesMapping"):
        # New api in python 3.11
        level_list = logging.getLevelNamesMapping().keys()
    else:
        level_list = logging._nameToLevel.keys()  # pylint: disable=W0212

    if value.upper() in level_list:
        return value.upper()

    raise ValueError


def cast_to_bool(value: str):
    return value.lower() in ("1", "true", "yes", "y")


def get_context(args):
    parser = ArgumentParser()
    parser.register("type", bool, cast_to_bool)

    default_migrations = os.environ.get("MIGRATIONS", "")
    # detect configuration
    parser.add_argument(
        "--db-url",
        default=os.environ.get("DB_URL", None),
        help="Clickhouse database hostname",
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
        help="Path to list of migration files",
    )
    parser.add_argument(
        "--multi-statement",
        default=cast_to_bool(os.environ.get("MULTI_STATEMENT", "1")),
        type=bool,
        action=argparse.BooleanOptionalAction,
        help="Path to list of migration files",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "WARNING"),
        type=log_level,
        help="Log level",
    )
    parser.add_argument(
        "--cluster-name",
        default=os.environ.get("CLUSTER_NAME", None),
        help="Clickhouse topology cluster",
    )
    parser.add_argument(
        "--dry-run",
        default=cast_to_bool(os.environ.get("DRY_RUN", "0")),
        type=bool,
        action=argparse.BooleanOptionalAction,
        help="Dry run mode",
    )
    parser.add_argument(
        "--fake",
        default=cast_to_bool(os.environ.get("FAKE", "0")),
        type=bool,
        action=argparse.BooleanOptionalAction,
        help="Marks the migrations as applied, "
        "but without actually running the SQL to change your database schema.",
    )
    parser.add_argument(
        "--migrations",
        default=default_migrations.split(",") if default_migrations else [],
        type=str,
        nargs="+",
        help="Explicit list of migrations to apply. "
        "Specify file name, file stem or migration version like 001_init.sql, 002_test2, 003, 4",
    )
    parser.add_argument(
        "--secure",
        default=cast_to_bool(os.environ.get("SECURE", "0")),
        type=bool,
        action=argparse.BooleanOptionalAction,
        help="Use secure connection",
    )
    parser.add_argument(
        "--create-db-if-not-exists",
        default=cast_to_bool(os.environ.get("CREATE_DB_IF_NOT_EXISTS", "1")),
        type=bool,
        action=argparse.BooleanOptionalAction,
        help="Create database if it does not exist",
    )

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
    )


def do_query_applied_migrations(cluster, ctx) -> List[Migration]:
    with cluster.connection(ctx.db_name) as conn:
        migrator = Migrator(conn, True)
        return migrator.query_applied_migrations()


def migrate(ctx) -> List[Migration]:
    logging.basicConfig(level=ctx.log_level, style="{", format="{levelname}:{message}")

    cluster = create_cluster(ctx)
    migrations = do_migrate(cluster, ctx)
    return migrations


def main() -> int:
    migrate(get_context(sys.argv[1:]))  # pragma: no cover
    return 0  # pragma: no cover
