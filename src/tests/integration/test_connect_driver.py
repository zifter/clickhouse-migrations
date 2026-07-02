from pathlib import Path
from time import sleep

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
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
