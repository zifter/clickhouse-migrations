import os
import sys
from argparse import ArgumentParser
from os.path import abspath
from pathlib import Path

from clickhouse_migrations.migrator import Migrator

DEFAULT_MIGRATION_PATH = abspath(Path(os.getcwd()) / "migrations")


def get_context(args):
    parser = ArgumentParser()
    # detect configuration
    parser.add_argument(
        "--db-name",
        default=os.environ.get("DB_NAME", "default"),
        help="Clickhouse database name",
    )
    parser.add_argument(
        "--migrations-dir",
        default=os.environ.get("MIGRATION_DIR", DEFAULT_MIGRATION_PATH),
        help="Path to list of migration files",
    )
    parser.add_argument(
        "--db-host",
        default=os.environ.get("DB_HOST", "localhost"),
        help="Clickhouse database hostname",
    )
    parser.add_argument(
        "--db-user",
        default=os.environ.get("DB_USER", "default"),
        help="Clickhouse user",
    )
    parser.add_argument(
        "--db-password",
        default=os.environ.get("DB_PASSWORD", ""),
        help="Clickhouse password",
    )

    return parser.parse_args(args)


def migrate(context) -> int:
    migrator = Migrator(context.db_host, context.db_user, context.db_password)
    migrator.migrate(context.db_name, context.migrations_dir)
    return 0


def main() -> int:
    return migrate(get_context(sys.argv[1:]))
