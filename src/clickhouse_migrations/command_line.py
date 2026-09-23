import argparse
import json
import logging
import math
import os
import re
import sys
import types
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional, Tuple

from clickhouse_migrations import __version__
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.connection import CLICKHOUSE_DRIVER, DRIVERS
from clickhouse_migrations.defaults import (
    DB_HOST,
    DB_PASSWORD,
    DB_USER,
    LOCK_TIMEOUT,
    LOCK_TTL,
    MIGRATIONS_DIR,
    MIGRATIONS_TABLE,
    MIGRATIONS_TABLE_ENGINE,
)
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration, MigrationStorage
from clickhouse_migrations.migrator import (
    MIGRATION_LOG_FORMATS,
    STATUS_MD5_MISMATCH,
    STATUS_PENDING,
    STATUS_UNKNOWN,
    Migrator,
    StatusRow,
)
from clickhouse_migrations.schema_dump import diff_dumps, write_text_atomic
from clickhouse_migrations.substitution import parse_assignment


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


STATUS_FORMAT_TABLE = "table"
STATUS_FORMAT_JSON = "json"
STATUS_FORMATS = (STATUS_FORMAT_TABLE, STATUS_FORMAT_JSON)

SUBCOMMANDS = ("migrate", "status", "down", "new", "dump", "unlock", "version")

SETTING_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _add_lock_arguments(parser):
    parser.add_argument(
        "--lock",
        default=cast_to_bool(os.environ.get("LOCK", "0")),
        action=argparse.BooleanOptionalAction,
        help="Take a migration lock so concurrent runs cannot interleave "
        "(opt-in; needs a server with the KeeperMap engine and "
        "<keeper_map_path_prefix>, and fails if it is unavailable)",
    )
    parser.add_argument(
        "--lock-timeout",
        default=int(os.environ.get("LOCK_TIMEOUT", LOCK_TIMEOUT)),
        type=int,
        help="Seconds to wait for a lock held by another run, with --lock "
        f"(default: {LOCK_TIMEOUT}; 0 fails immediately)",
    )
    parser.add_argument(
        "--lock-ttl",
        default=int(os.environ.get("LOCK_TTL", LOCK_TTL)),
        type=int,
        help="Seconds after which a lock is considered stale and may be taken "
        f"over by another run, with --lock (default: {LOCK_TTL})",
    )


def var_assignment(value: str) -> Tuple[str, str]:
    try:
        return parse_assignment(value)
    except ValueError as exc:
        # ArgumentTypeError prints only this message, never the value itself,
        # which may be a secret.
        raise argparse.ArgumentTypeError(str(exc)) from exc


# Env counterpart of the repeatable --var: comma-separated NAME=VALUE pairs.
MIGRATION_VARS_ENV = "MIGRATION_VARS"


def _add_substitution_arguments(parser):
    parser.add_argument(
        "--var",
        dest="variables",
        action="append",
        default=None,
        type=var_assignment,
        metavar="NAME=VALUE",
        help="Substitute ${NAME} in migration files with VALUE (repeatable). "
        "Enables substitution on its own; wins over --substitute-env. "
        f"Env: {MIGRATION_VARS_ENV}=A=1,B=2 (ignored when --var is given)",
    )
    parser.add_argument(
        "--substitute-env",
        default=cast_to_bool(os.environ.get("SUBSTITUTE_ENV", "0")),
        action=argparse.BooleanOptionalAction,
        help="Substitute ${NAME} in migration files from the process environment",
    )


def _resolve_var_arguments(parser, ctx) -> None:
    """Turn the --var pairs (or MIGRATION_VARS) into a dict, None if unset."""
    pairs = ctx.variables
    if pairs is None:
        raw = os.environ.get(MIGRATION_VARS_ENV, "")
        items = [item for item in raw.split(",") if item]
        if items:
            try:
                pairs = [parse_assignment(item) for item in items]
            except ValueError as exc:
                parser.error(f"{MIGRATION_VARS_ENV}: {exc}")

    ctx.variables = dict(pairs) if pairs is not None else None


def _add_common_arguments(parser):
    parser.add_argument(
        "--db-url",
        default=os.environ.get("DB_URL", None),
        help="Clickhouse connection URL (clickhouse://user:password@host:port/db). "
        "Overrides --db-host/--db-port/--db-user/--db-password. "
        "With clickhouse-driver use clickhouse:// or clickhouses:// (TLS); "
        "with clickhouse-connect clickhouse:// maps to http:// (port 8123) and "
        "clickhouses:// to https:// (port 8443), and http(s):// is accepted as is",
    )
    parser.add_argument(
        "--db-host",
        default=os.environ.get("DB_HOST", DB_HOST),
        help="Clickhouse database hostname",
    )
    parser.add_argument(
        "--db-port",
        default=os.environ.get("DB_PORT", None),
        help="Clickhouse database port "
        "(default: 9000 for clickhouse-driver, 8123 for clickhouse-connect)",
    )
    parser.add_argument(
        "--driver",
        default=os.environ.get("DRIVER", CLICKHOUSE_DRIVER),
        choices=DRIVERS,
        help="ClickHouse driver to use",
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
        "--migrations-table",
        default=os.environ.get("MIGRATIONS_TABLE", MIGRATIONS_TABLE),
        help="Table where applied migrations are recorded. "
        "Accepts a 'database.table' form to keep it in another database "
        f"(default: {MIGRATIONS_TABLE})",
    )
    parser.add_argument(
        "--migrations-table-engine",
        default=os.environ.get("MIGRATIONS_TABLE_ENGINE", MIGRATIONS_TABLE_ENGINE),
        help="Full engine clause for the migrations table, e.g. "
        "\"ReplicatedMergeTree('/ch/{shard}/tables/{database}/{table}', '{replica}')\". "
        "Passed through as is and wins over the --cluster-name default",
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
    _add_transport_arguments(parser)


def positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        number = math.nan
    if not (math.isfinite(number) and number > 0):
        raise argparse.ArgumentTypeError(f"expected a positive number, got {value!r}")
    return number


def parse_setting(item: str) -> Tuple[str, str]:
    """Parse one ``name=value`` ClickHouse setting; the value is kept as text."""
    name, sep, value = item.partition("=")
    name = name.strip()
    if not sep or not SETTING_NAME.fullmatch(name):
        raise argparse.ArgumentTypeError(
            f"expected a ClickHouse setting as name=value, got {item!r}"
        )
    return name, value.strip()


def parse_settings(text: str) -> List[Tuple[str, str]]:
    """Parse the comma-separated ``name=value`` list of CLICKHOUSE_SETTINGS."""
    return [parse_setting(item) for item in text.split(",") if item.strip()]


def _add_transport_arguments(parser):
    parser.add_argument(
        "--ca-cert",
        default=os.environ.get("CLICKHOUSE_CA_CERT", None),
        help="CA certificate file (PEM) to verify the server certificate with TLS",
    )
    parser.add_argument(
        "--cert",
        default=os.environ.get("CLICKHOUSE_CERT", None),
        help="Client certificate file (PEM) for mutual TLS",
    )
    parser.add_argument(
        "--key",
        default=os.environ.get("CLICKHOUSE_KEY", None),
        help="Private key file (PEM) of the client certificate, with --cert",
    )
    parser.add_argument(
        "--verify",
        default=cast_to_bool(os.environ.get("CLICKHOUSE_VERIFY", "1")),
        action=argparse.BooleanOptionalAction,
        help="Verify the TLS certificate of the server (--no-verify logs a warning)",
    )
    parser.add_argument(
        "--connect-timeout",
        default=os.environ.get("CLICKHOUSE_CONNECT_TIMEOUT", None),
        type=positive_float,
        help="Seconds to wait for the connection to be established "
        "(default: the driver's, 10s)",
    )
    parser.add_argument(
        "--query-timeout",
        default=os.environ.get("CLICKHOUSE_QUERY_TIMEOUT", None),
        type=positive_float,
        help="Seconds to wait for the server while a statement runs "
        "(socket read timeout; default: the driver's, 300s)",
    )
    parser.add_argument(
        "--setting",
        dest="settings",
        default=None,
        type=parse_setting,
        action="append",
        metavar="NAME=VALUE",
        help="ClickHouse setting sent with every statement, repeatable "
        "(env CLICKHOUSE_SETTINGS='a=1,b=2'; a flag wins over the variable)",
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


def _add_migrate_target_argument(parser):
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


def _add_status_arguments(parser):
    parser.add_argument(
        "--strict",
        default=cast_to_bool(os.environ.get("STRICT", "0")),
        action=argparse.BooleanOptionalAction,
        help="Exit with code 1 if any migration is md5-mismatch or unknown",
    )
    parser.add_argument(
        "--exit-code-pending",
        default=cast_to_bool(os.environ.get("EXIT_CODE_PENDING", "0")),
        action=argparse.BooleanOptionalAction,
        help="Exit with code 1 if any migration is pending",
    )
    parser.add_argument(
        "--format",
        default=os.environ.get("STATUS_FORMAT", STATUS_FORMAT_TABLE),
        choices=STATUS_FORMATS,
        help="Output format: table or json (json prints only the JSON document to stdout)",
    )


def _add_down_arguments(parser):
    parser.add_argument(
        "--steps",
        default=1,
        type=int,
        help="Number of most recent applied migrations to roll back (default: 1)",
    )
    parser.add_argument(
        "--to",
        dest="to_version",
        default=None,
        type=int,
        help="Roll back every applied migration with a version greater than this. "
        "Takes precedence over --steps.",
    )
    parser.add_argument(
        "--dry-run",
        default=cast_to_bool(os.environ.get("DRY_RUN", "0")),
        action=argparse.BooleanOptionalAction,
        help="Dry run mode",
    )
    parser.add_argument(
        "--multi-statement",
        default=cast_to_bool(os.environ.get("MULTI_STATEMENT", "1")),
        action=argparse.BooleanOptionalAction,
        help="Treat each down migration file as multiple ';'-separated statements",
    )


def _add_new_arguments(parser):
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


# Connection options "dump" shares with the other subcommands. It is read-only,
# so it takes none of the migrate/down options (dry-run, migrations-dir, ...).
DUMP_COMMON_OPTIONS = (
    "--db-url",
    "--db-host",
    "--db-port",
    "--driver",
    "--db-user",
    "--db-password",
    "--db-name",
    "--migrations-table",
    "--log-level",
    "--secure",
    "--ca-cert",
    "--cert",
    "--key",
    "--verify",
    "--connect-timeout",
    "--query-timeout",
    "--setting",
)


def _only_options(parser, allowed):
    """A stand-in for ``parser`` that registers only the ``allowed`` options."""

    def add_argument(*names, **kwargs):
        if names[0] in allowed:
            parser.add_argument(*names, **kwargs)

    return types.SimpleNamespace(add_argument=add_argument)


def _add_dump_arguments(parser):
    _add_common_arguments(_only_options(parser, DUMP_COMMON_OPTIONS))
    default_tables = os.environ.get("DUMP_TABLES", "")
    parser.add_argument(
        "--tables",
        default=default_tables.split(",") if default_tables else [],
        type=str,
        nargs="+",
        help="Dump only these tables/views/dictionaries (by name)",
    )
    parser.add_argument(
        "--keep-replicated-paths",
        default=cast_to_bool(os.environ.get("KEEP_REPLICATED_PATHS", "0")),
        action=argparse.BooleanOptionalAction,
        help="Keep the ZooKeeper path and replica name arguments of Replicated*MergeTree "
        "engines (by default they are removed so the dump is portable across clusters)",
    )
    parser.add_argument(
        "--include-migrations-table",
        default=cast_to_bool(os.environ.get("INCLUDE_MIGRATIONS_TABLE", "0")),
        action=argparse.BooleanOptionalAction,
        help="Also dump the migrations bookkeeping (and lock) table",
    )
    target = parser.add_mutually_exclusive_group()
    target.add_argument(
        "--out",
        default=None,
        type=Path,
        help="Write the dump to this file instead of stdout",
    )
    target.add_argument(
        "--check",
        default=None,
        type=Path,
        help="Compare the dump with this file; exit code 1 and a diff on stderr on drift",
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
    _add_migrate_target_argument(migrate_parser)
    _add_lock_arguments(migrate_parser)
    _add_substitution_arguments(migrate_parser)

    status_parser = subparsers.add_parser(
        "status", help="Show applied vs pending migrations without applying anything"
    )
    _add_common_arguments(status_parser)
    _add_status_arguments(status_parser)

    down_parser = subparsers.add_parser(
        "down",
        help="Roll back applied migrations using their .down.sql files",
    )
    _add_common_arguments(down_parser)
    _add_down_arguments(down_parser)
    _add_lock_arguments(down_parser)
    _add_substitution_arguments(down_parser)

    unlock_parser = subparsers.add_parser(
        "unlock",
        help="Force-release the migration lock left behind by a dead run",
    )
    _add_common_arguments(unlock_parser)

    new_parser = subparsers.add_parser(
        "new", help="Create the next migration file locally, without any database"
    )
    _add_new_arguments(new_parser)

    dump_parser = subparsers.add_parser(
        "dump",
        help="Print the schema (tables, views, dictionaries) as portable, diffable SQL",
    )
    _add_dump_arguments(dump_parser)

    subparsers.add_parser("version", help="Show the version and exit")

    # Default to the "migrate" subcommand so existing invocations
    # (clickhouse-migrations <flags>) keep working unchanged.
    args = list(args)
    if not args or (
        args[0] not in SUBCOMMANDS and args[0] not in ("-h", "--help", "--version")
    ):
        args = ["migrate", *args]

    ctx = parser.parse_args(args)
    if ctx.command == "migrate" and ctx.to_version is not None and ctx.migrations:
        migrate_parser.error("--to cannot be combined with --migrations")
    if hasattr(ctx, "settings"):
        # Every subcommand that connects: the variable first, so a --setting
        # of the same name wins.
        try:
            from_env = parse_settings(os.environ.get("CLICKHOUSE_SETTINGS", ""))
            ctx.settings = dict(from_env + (ctx.settings or []))
        except argparse.ArgumentTypeError as exc:
            subparsers.choices[ctx.command].error(f"CLICKHOUSE_SETTINGS: {exc}")
    if ctx.command in ("migrate", "down"):
        _resolve_var_arguments(subparsers.choices[ctx.command], ctx)

    return ctx


def create_cluster(ctx) -> ClickhouseCluster:
    return ClickhouseCluster(
        ca_cert=ctx.ca_cert,
        cert=ctx.cert,
        key=ctx.key,
        verify=ctx.verify,
        connect_timeout=ctx.connect_timeout,
        query_timeout=ctx.query_timeout,
        settings=ctx.settings or None,
        db_host=ctx.db_host,
        db_port=ctx.db_port,
        db_user=ctx.db_user,
        db_password=ctx.db_password,
        db_url=ctx.db_url,
        secure=ctx.secure,
        driver=ctx.driver,
        migrations_table=ctx.migrations_table,
        migrations_table_engine=getattr(ctx, "migrations_table_engine", None),
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


def do_status(cluster, ctx) -> List[StatusRow]:
    return cluster.status(
        db_name=ctx.db_name,
        migration_path=ctx.migrations_dir,
        explicit_migrations=ctx.migrations,
    )


def do_rollback(cluster, ctx) -> List[int]:
    return cluster.rollback(
        db_name=ctx.db_name,
        migration_path=ctx.migrations_dir,
        steps=ctx.steps,
        to_version=ctx.to_version,
        dryrun=ctx.dry_run,
        multi_statement=ctx.multi_statement,
        lock=ctx.lock,
        lock_timeout=ctx.lock_timeout,
        lock_ttl=ctx.lock_ttl,
        variables=ctx.variables,
        substitute_env=ctx.substitute_env,
    )


def format_status(rows: List[StatusRow]) -> str:
    if not rows:
        return "No migrations found."

    table = [("VERSION", "STATUS", "MD5", "APPLIED AT", "HAS DOWN")]
    for row in rows:
        table.append(
            (
                str(row.version),
                row.state,
                row.md5 or "",
                str(row.applied_at) if row.applied_at is not None else "",
                "yes" if row.has_down else "no",
            )
        )

    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    return "\n".join(
        "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)) for row in table
    )


def _isoformat(value) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def format_status_json(db_name: str, rows: List[StatusRow]) -> str:
    return json.dumps(
        {
            "database": db_name,
            "migrations": [
                {
                    "version": row.version,
                    "state": row.state,
                    "md5": row.md5,
                    "applied_at": _isoformat(row.applied_at),
                    "has_down": bool(row.has_down),
                }
                for row in rows
            ],
        },
        indent=2,
    )


def status_exit_code(
    rows: List[StatusRow], strict: bool = False, exit_code_pending: bool = False
) -> int:
    failing = set()
    if strict:
        failing.update((STATUS_MD5_MISMATCH, STATUS_UNKNOWN))
    if exit_code_pending:
        failing.add(STATUS_PENDING)

    return 1 if any(row.state in failing for row in rows) else 0


def warn_debug_with_substitution(ctx) -> None:
    """Our own logs never show substituted values, but the drivers' do."""
    if ctx.log_level == "DEBUG" and (ctx.variables is not None or ctx.substitute_env):
        logging.warning(
            "--log-level DEBUG with variable substitution: the driver's own "
            "debug log prints every statement as sent, substituted values "
            "included. Avoid DEBUG when a variable holds a secret."
        )


def migrate(ctx) -> List[Migration]:
    logging.basicConfig(level=ctx.log_level, style="{", format="{levelname}:{message}")
    warn_debug_with_substitution(ctx)

    cluster = create_cluster(ctx)
    migrations = do_migrate(cluster, ctx)
    return migrations


def show_status(ctx) -> int:
    # basicConfig logs to stderr, so stdout stays clean for --format json.
    logging.basicConfig(level=ctx.log_level, style="{", format="{levelname}:{message}")

    cluster = create_cluster(ctx)
    rows = do_status(cluster, ctx)
    if ctx.format == STATUS_FORMAT_JSON:
        db_name = ctx.db_name if ctx.db_name is not None else cluster.default_db_name
        print(format_status_json(db_name, rows))
    else:
        print(format_status(rows))
    return status_exit_code(rows, ctx.strict, ctx.exit_code_pending)


def rollback(ctx) -> List[int]:
    logging.basicConfig(level=ctx.log_level, style="{", format="{levelname}:{message}")
    warn_debug_with_substitution(ctx)

    cluster = create_cluster(ctx)
    return do_rollback(cluster, ctx)


def unlock(ctx) -> int:
    logging.basicConfig(level=ctx.log_level, style="{", format="{levelname}:{message}")

    cluster = create_cluster(ctx)
    holder = cluster.force_unlock(db_name=ctx.db_name)
    if holder is None:
        print("No migration lock is held.")
    else:
        print(
            f"Released the migration lock held by {holder.owner} " f"for {holder.age}s."
        )

    return 0


def create_migration(ctx) -> List[Path]:
    created = MigrationStorage(ctx.migrations_dir).create(
        ctx.name,
        version=ctx.version,
        with_down=ctx.down,
    )
    for path in created:
        print(f"Created {path}")

    return created


def run_dump(ctx) -> int:
    """Print, write (--out) or verify (--check) the schema dump.

    Exit code: 0 on success (and, with --check, when there is no drift); 1 on
    drift or on any error. stdout only ever carries the SQL.
    """
    logging.basicConfig(level=ctx.log_level, style="{", format="{levelname}:{message}")

    try:
        expected = None
        if ctx.check is not None:
            try:
                expected = ctx.check.read_text(encoding="utf8")
            except OSError as exc:
                raise MigrationException(
                    f"Cannot read the schema file {ctx.check}: {exc}"
                ) from exc

        cluster = create_cluster(ctx)
        db_name = ctx.db_name if ctx.db_name is not None else cluster.default_db_name
        text = cluster.dump(
            db_name=db_name,
            tables=ctx.tables,
            keep_replicated_paths=ctx.keep_replicated_paths,
            include_migrations_table=ctx.include_migrations_table,
        )

        if expected is not None:
            diff = diff_dumps(expected, text, str(ctx.check), f"database {db_name}")
            if diff:
                print(diff, file=sys.stderr)
                print(
                    f"Schema drift: {db_name} differs from {ctx.check}", file=sys.stderr
                )
                return 1
            print(f"Schema of {db_name} matches {ctx.check}", file=sys.stderr)
        elif ctx.out is not None:
            try:
                write_text_atomic(ctx.out, text)
            except OSError as exc:
                raise MigrationException(f"Cannot write {ctx.out}: {exc}") from exc
            print(f"Wrote the schema of {db_name} to {ctx.out}", file=sys.stderr)
        else:
            sys.stdout.write(text)
    except MigrationException as exc:
        logging.error("Dump failed: %s", exc)
        return 1
    return 0


def main() -> int:
    ctx = get_context(sys.argv[1:])
    if ctx.command == "version":
        print(f"clickhouse-migrations {__version__}")
        return 0
    try:
        if ctx.command == "new":
            create_migration(ctx)
        elif ctx.command == "status":
            return show_status(ctx)
        elif ctx.command == "dump":
            return run_dump(ctx)
        elif ctx.command == "down":
            rollback(ctx)
        elif ctx.command == "unlock":
            return unlock(ctx)
        else:
            migrate(ctx)
    except MigrationException as exc:
        logging.error("Migration failed: %s", exc)
        return 1
    return 0
