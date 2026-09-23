import socket
import sys
import time
import uuid

import pytest
from clickhouse_connect.driver.exceptions import OperationalError

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.exceptions import MigrationException

DRIVER = "clickhouse-driver"
CONNECT = "clickhouse-connect"

# Each target: the connection arguments of one driver, with the individual
# host/port arguments or with --db-url.
TARGETS = {
    "driver-host": ["--driver", DRIVER],
    "driver-url": [
        "--driver",
        DRIVER,
        "--db-url",
        "clickhouse://default:@localhost:9000",
    ],
    "connect-host": ["--driver", CONNECT],
    "connect-url": ["--driver", CONNECT, "--db-url", "http://default:@localhost:8123"],
}

# Two statements in one file: over clickhouse-connect each is its own HTTP
# request, so a setting has to reach every one of them.
LOW_CARDINALITY = (
    "CREATE TABLE lc (x LowCardinality(UInt8)) ENGINE = MergeTree ORDER BY x;\n"
    "INSERT INTO lc VALUES (1), (2);\n"
)

RECORD_MAX_THREADS = (
    "CREATE TABLE seen (max_threads UInt64) ENGINE = MergeTree ORDER BY tuple();\n"
    "INSERT INTO seen SELECT toUInt64(getSetting('max_threads'));\n"
)


def _migrations(tmp_path, body):
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "001_first.sql").write_text(body, encoding="utf8")
    return migrations_dir


def _run(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", *args])
    return main()


def _run_migrate(monkeypatch, target, migrations_dir, *extra):
    return _run(
        monkeypatch,
        "migrate",
        *TARGETS[target],
        "--db-name",
        "pytest",
        "--migrations-dir",
        str(migrations_dir),
        *extra,
    )


@pytest.mark.parametrize("target", TARGETS)
def test_setting_is_applied_server_side(monkeypatch, tmp_path, cluster, target):
    migrations_dir = _migrations(tmp_path, RECORD_MAX_THREADS)

    assert (
        _run_migrate(monkeypatch, target, migrations_dir, "--setting", "max_threads=3")
        == 0
    )

    with cluster.connection("pytest") as conn:
        assert conn.query("SELECT max_threads FROM seen") == [{"max_threads": 3}]


@pytest.mark.parametrize("target", TARGETS)
def test_multi_statement_migration_needs_its_setting(
    monkeypatch, tmp_path, cluster, target
):
    migrations_dir = _migrations(tmp_path, LOW_CARDINALITY)

    # Without the setting the first statement is refused by the server ...
    with pytest.raises(Exception, match="LowCardinality"):
        _run_migrate(monkeypatch, target, migrations_dir)

    # ... with it, both statements of the file run (the table was not created).
    assert (
        _run_migrate(
            monkeypatch,
            target,
            migrations_dir,
            "--setting",
            "allow_suspicious_low_cardinality_types=1",
        )
        == 0
    )
    with cluster.connection("pytest") as conn:
        assert conn.query("SELECT count() AS n FROM lc") == [{"n": 2}]


def test_settings_env_var_reaches_the_server(monkeypatch, tmp_path, cluster):
    monkeypatch.setenv(
        "CLICKHOUSE_SETTINGS", "allow_suspicious_low_cardinality_types=1,max_threads=2"
    )
    migrations_dir = _migrations(tmp_path, LOW_CARDINALITY + RECORD_MAX_THREADS)

    assert _run_migrate(monkeypatch, "connect-host", migrations_dir) == 0

    with cluster.connection("pytest") as conn:
        assert conn.query("SELECT max_threads FROM seen") == [{"max_threads": 2}]


@pytest.mark.parametrize("target", TARGETS)
def test_setting_reaches_every_statement_of_every_subcommand(
    monkeypatch, tmp_path, cluster, target
):
    """Migration, bookkeeping, lock, status, dump and unlock statements alike."""
    marker = f"chm79-{uuid.uuid4()}"
    migrations_dir = _migrations(
        tmp_path, "CREATE TABLE t (i UInt8) ENGINE = MergeTree ORDER BY i;\n"
    )
    setting = ["--setting", f"log_comment={marker}"]

    assert _run_migrate(monkeypatch, target, migrations_dir, "--lock", *setting) == 0
    common = [*TARGETS[target], "--db-name", "pytest", *setting]
    assert (
        _run(monkeypatch, "status", *common, "--migrations-dir", str(migrations_dir))
        == 0
    )
    assert _run(monkeypatch, "dump", *common) == 0
    assert _run(monkeypatch, "unlock", *common) == 0

    with cluster.connection("") as conn:
        conn.command("SYSTEM FLUSH LOGS")
        tagged = [
            row["query"]
            for row in conn.query(
                "SELECT query FROM system.query_log "
                f"WHERE log_comment = '{marker}' AND type = 'QueryFinish'"
            )
        ]

    def ran(fragment):
        return any(fragment in query for query in tagged)

    assert ran("CREATE TABLE t")  # the migration
    assert ran("INSERT INTO") and ran("schema_versions")  # bookkeeping
    assert ran("schema_versions_lock")  # the lock
    assert ran("SHOW CREATE TABLE")  # dump


@pytest.mark.parametrize("driver", [DRIVER, CONNECT])
def test_unknown_setting_fails_on_both_drivers(driver):
    cluster = ClickhouseCluster(driver=driver, settings={"max_threadz": "3"})

    with pytest.raises(Exception, match="max_threadz"):
        with cluster.connection("") as conn:
            conn.query("SELECT 1 AS n")


def test_query_timeout_clickhouse_connect(tmp_path):
    cluster = ClickhouseCluster(driver=CONNECT, query_timeout=1)
    started = time.monotonic()

    with pytest.raises(OperationalError, match="timed out"):
        cluster.migrate("pytest", _migrations(tmp_path, "SELECT sleep(3);\n"))

    assert time.monotonic() - started < 2.5


def test_query_timeout_clickhouse_driver(tmp_path):
    # The native protocol counts the timeout between two packets and the server
    # sends progress packets every interactive_delay, so silence them first.
    cluster = ClickhouseCluster(
        query_timeout=1, settings={"interactive_delay": "10000000"}
    )
    started = time.monotonic()

    with pytest.raises(socket.timeout):
        cluster.migrate("pytest", _migrations(tmp_path, "SELECT sleep(3);\n"))

    assert time.monotonic() - started < 2.5


def test_query_timeout_leaves_room_for_slow_statements(tmp_path):
    connect_cluster = ClickhouseCluster(driver=CONNECT, query_timeout=5)

    applied = connect_cluster.migrate(
        "pytest", _migrations(tmp_path, "SELECT sleep(2);\n")
    )

    assert [m.version for m in applied] == [1]


@pytest.mark.parametrize("driver", [DRIVER, CONNECT])
def test_connect_timeout_is_honoured(driver):
    # A non-routable address: the connection attempt hangs until the timeout.
    cluster = ClickhouseCluster(
        db_host="10.255.255.1", driver=driver, connect_timeout=0.5
    )
    started = time.monotonic()

    with pytest.raises(Exception):
        with cluster.connection("") as conn:
            conn.query("SELECT 1 AS n")

    assert time.monotonic() - started < 5


def test_key_without_cert_fails_before_connecting(monkeypatch, tmp_path):
    migrations_dir = _migrations(tmp_path, "SELECT 1;\n")

    assert (
        _run_migrate(monkeypatch, "driver-host", migrations_dir, "--key", "k.pem") == 1
    )
    with pytest.raises(MigrationException):
        ClickhouseCluster(key="k.pem")
