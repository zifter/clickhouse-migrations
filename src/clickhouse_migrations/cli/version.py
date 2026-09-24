"""``version``: show the version and exit."""

from clickhouse_migrations import __version__


def add_parser(subparsers):
    return subparsers.add_parser("version", help="Show the version and exit")


def show_version() -> int:
    print(f"clickhouse-migrations {__version__}")
    return 0
