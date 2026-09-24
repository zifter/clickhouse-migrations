import sys
from pathlib import Path
from time import sleep

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.migrator import STATUS_APPLIED, STATUS_PENDING

TESTS_DIR = Path(__file__).parents[1]
MIGRATIONS = TESTS_DIR / "migrations"


@pytest.fixture
def connect_cluster() -> ClickhouseCluster:
    return ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        driver="clickhouse-connect",
    )


def test_connect_status_pending_before_migrate(connect_cluster: ClickhouseCluster):
    rows = connect_cluster.status("pytest", MIGRATIONS)

    assert len(rows) == 1
    assert all(r.state == STATUS_PENDING for r in rows)


def test_connect_migrate_and_status(connect_cluster: ClickhouseCluster):
    applied = connect_cluster.migrate("pytest", MIGRATIONS)
    assert len(applied) == 1

    rows = connect_cluster.status("pytest", MIGRATIONS)
    assert len(rows) == 1
    assert all(r.state == STATUS_APPLIED for r in rows)
    assert all(r.applied_at is not None for r in rows)


def test_connect_fake(connect_cluster: ClickhouseCluster):
    applied = connect_cluster.migrate("pytest", MIGRATIONS, fake=True)

    assert len(applied) == 1
    rows = connect_cluster.status("pytest", MIGRATIONS)
    assert all(r.state == STATUS_APPLIED for r in rows)


def test_connect_migrate_on_cluster(connect_cluster: ClickhouseCluster, _schema):
    # Exercises ON CLUSTER / ReplicatedMergeTree DDL over the HTTP driver.
    connect_cluster.migrate("pytest", MIGRATIONS, cluster_name="company_cluster")

    # Replicated inserts propagate asynchronously; give them a moment.
    sleep(1)

    rows = connect_cluster.status("pytest", MIGRATIONS)
    assert all(r.state == STATUS_APPLIED for r in rows)


# clickhouse:// maps to http:// under clickhouse-connect and keeps the explicit
# port, so the URLs below target the dev container's HTTP port 8123.
@pytest.mark.parametrize(
    "url",
    [
        "clickhouse://default:@localhost:8123/pytest",
        "http://default:@localhost:8123/pytest",
        # Client parameters in the query string (clickhouse-connect < 0.7.9
        # passed them to the client as lists and failed).
        "http://default:@localhost:8123/pytest"
        "?connect_timeout=5&send_receive_timeout=30&compress=false",
    ],
)
def test_connect_db_url_migrate_and_status(url):
    cluster = ClickhouseCluster(db_url=url, driver="clickhouse-connect")

    applied = cluster.migrate(None, MIGRATIONS)
    assert len(applied) == 1

    rows = cluster.status(None, MIGRATIONS)
    assert all(r.state == STATUS_APPLIED for r in rows)
    assert {"sample", "schema_versions"} <= set(cluster.show_tables(None))


def test_connect_db_url_via_cli(monkeypatch, capsys):
    args = [
        "--db-url",
        "clickhouse://default:@localhost:8123/pytest",
        "--driver",
        "clickhouse-connect",
        "--migrations-dir",
        str(MIGRATIONS),
    ]
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", "migrate"] + args)
    assert main() == 0

    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", "status"] + args)
    assert main() == 0
    assert STATUS_APPLIED in capsys.readouterr().out

    native = ClickhouseCluster(db_host="localhost", db_name="pytest")
    assert "sample" in native.show_tables("pytest")
