import os
import sys
from argparse import ArgumentParser
from pathlib import Path

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.defaults import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
    MIGRATIONS_DIR,
)


def cast_to_bool(value: str):
    return value.lower() in ("1", "true", "yes", "y")


def get_context(args):
    parser = ArgumentParser()
    parser.register("type", bool, cast_to_bool)

    # detect configuration
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
        default=os.environ.get("DB_NAME", DB_NAME),
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
        default=os.environ.get("MULTI_STATEMENT", "1"),
        type=bool,
        help="Path to list of migration files",
    )

    return parser.parse_args(args)


def migrate(ctx) -> int:
    cluster = ClickhouseCluster(
        ctx.db_host,
        db_port=ctx.db_port,
        db_user=ctx.db_user,
        db_password=ctx.db_password,
    )
    cluster.migrate(ctx.db_name, ctx.migrations_dir, ctx.multi_statement)
    return 0


def main() -> int:
    return migrate(get_context(sys.argv[1:]))  # pragma: no cover
