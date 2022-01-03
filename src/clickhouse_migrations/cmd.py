import os
import sys
from argparse import ArgumentParser
from pathlib import Path

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.defaults import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    MIGRATIONS_DIR,
)


def get_context(args):
    parser = ArgumentParser()
    # detect configuration
    parser.add_argument(
        "--db-host",
        default=os.environ.get("DB_HOST", DB_HOST),
        help="Clickhouse database hostname",
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
        help="Path to list of migration files",
    )

    return parser.parse_args(args)


def migrate(context) -> int:
    cluster = ClickhouseCluster(context.db_host, context.db_user, context.db_password)
    cluster.migrate(context.db_name, Path(context.migrations_dir))
    return 0


def main() -> int:
    return migrate(get_context(sys.argv[1:]))
