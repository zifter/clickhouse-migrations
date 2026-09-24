"""``new``: create the next migration file locally, without any database."""

import os
from pathlib import Path
from typing import List

from clickhouse_migrations.defaults import MIGRATIONS_DIR
from clickhouse_migrations.migration import MigrationStorage


def add_arguments(parser):
    parser.add_argument(
        "name",
        help='Human readable migration name, e.g. "add events"',
    )
    # "new" never connects to ClickHouse, so it takes none of the common
    # database arguments - only the directory to scaffold into.
    parser.add_argument(
        "--dir",
        "--migrations-dir",
        dest="migrations_dir",
        default=os.environ.get("MIGRATIONS_DIR", MIGRATIONS_DIR),
        type=Path,
        help="Path to the directory with migration files",
    )
    parser.add_argument(
        "--down",
        default=False,
        action="store_true",
        help="Also create the paired {VERSION}_{name}.down.sql rollback file",
    )
    parser.add_argument(
        "--version",
        default=None,
        type=int,
        help="Use this version instead of the next one; fails if it is taken",
    )


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "new", help="Create the next migration file locally, without any database"
    )
    add_arguments(parser)
    return parser


def create_migration(ctx) -> List[Path]:
    created = MigrationStorage(ctx.migrations_dir).create(
        ctx.name,
        version=ctx.version,
        with_down=ctx.down,
    )
    for path in created:
        print(f"Created {path}")

    return created
