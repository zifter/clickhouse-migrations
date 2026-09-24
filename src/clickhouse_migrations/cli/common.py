"""Options shared by the subcommands that connect, and the cluster they build."""

import argparse
import logging
import os
import types
from pathlib import Path

from clickhouse_migrations.cli.argtypes import (
    cast_to_bool,
    log_level,
    parse_setting,
    parse_settings,
    positive_float,
)
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.connection import CLICKHOUSE_DRIVER, DRIVERS
from clickhouse_migrations.defaults import (
    DB_HOST,
    DB_PASSWORD,
    DB_USER,
    MIGRATIONS_DIR,
    MIGRATIONS_TABLE,
    MIGRATIONS_TABLE_ENGINE,
)


def add_common_arguments(parser):
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
    add_transport_arguments(parser)


def add_transport_arguments(parser):
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


def resolve_settings(parser, ctx) -> None:
    """Merge CLICKHOUSE_SETTINGS and the --setting pairs into a dict.

    The variable goes first, so a --setting of the same name wins.
    """
    try:
        from_env = parse_settings(os.environ.get("CLICKHOUSE_SETTINGS", ""))
        ctx.settings = dict(from_env + (ctx.settings or []))
    except argparse.ArgumentTypeError as exc:
        parser.error(f"CLICKHOUSE_SETTINGS: {exc}")


def only_options(parser, allowed):
    """A stand-in for ``parser`` that registers only the ``allowed`` options."""

    def add_argument(*names, **kwargs):
        if names[0] in allowed:
            parser.add_argument(*names, **kwargs)

    return types.SimpleNamespace(add_argument=add_argument)


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


def configure_logging(ctx) -> None:
    # basicConfig logs to stderr, so stdout stays clean for --format json.
    logging.basicConfig(level=ctx.log_level, style="{", format="{levelname}:{message}")
