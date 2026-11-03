import sys
from pathlib import Path

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.migrator import STATUS_APPLIED, STATUS_PENDING

TESTS_DIR = Path(__file__).parents[1]
DOWN_MIGRATIONS = TESTS_DIR / "down_migrations"
CLUSTER_NAME = "company_cluster"


def _states(cluster: ClickhouseCluster):
    return {r.version: r.state for r in cluster.status("pytest", DOWN_MIGRATIONS)}


def test_rollback_last_migration(cluster: ClickhouseCluster):
    cluster.migrate("pytest", DOWN_MIGRATIONS)
    assert "sample_b" in cluster.show_tables("pytest")

    rolled = cluster.rollback("pytest", DOWN_MIGRATIONS, steps=1)

    assert rolled == [2]
    tables = cluster.show_tables("pytest")
    assert "sample_b" not in tables
    assert "sample_a" in tables

    states = _states(cluster)
    assert states[1] == STATUS_APPLIED
    assert states[2] == STATUS_PENDING


def test_rollback_to_version_rolls_back_all(cluster: ClickhouseCluster):
    cluster.migrate("pytest", DOWN_MIGRATIONS)

    rolled = cluster.rollback("pytest", DOWN_MIGRATIONS, to_version=0)

    assert rolled == [2, 1]
    tables = cluster.show_tables("pytest")
    assert "sample_a" not in tables
    assert "sample_b" not in tables

    states = _states(cluster)
    assert states[1] == STATUS_PENDING
    assert states[2] == STATUS_PENDING


def test_rollback_dry_run_changes_nothing(cluster: ClickhouseCluster):
    cluster.migrate("pytest", DOWN_MIGRATIONS)

    rolled = cluster.rollback("pytest", DOWN_MIGRATIONS, steps=2, dryrun=True)

    assert rolled == [2, 1]
    tables = cluster.show_tables("pytest")
    assert "sample_a" in tables
    assert "sample_b" in tables

    states = _states(cluster)
    assert states[1] == STATUS_APPLIED
    assert states[2] == STATUS_APPLIED


def test_rollback_without_schema_table_is_noop(cluster: ClickhouseCluster):
    # Nothing migrated yet -> schema_versions does not exist.
    assert cluster.rollback("pytest", DOWN_MIGRATIONS, steps=1) == []


def test_main_down_subcommand(cluster: ClickhouseCluster, monkeypatch):
    cluster.migrate("pytest", DOWN_MIGRATIONS)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "clickhouse-migrations",
            "down",
            "--db-name",
            "pytest",
            "--migrations-dir",
            str(DOWN_MIGRATIONS),
            "--steps",
            "1",
        ],
    )

    assert main() == 0
    assert "sample_b" not in cluster.show_tables("pytest")


@pytest.mark.parametrize("driver", ["clickhouse-driver", "clickhouse-connect"])
def test_migrate_after_rollback_on_cluster_records_the_migration(_schema, driver):
    # Issue #122: with --cluster-name the migrations table is a
    # ReplicatedMergeTree. `down` deletes the row of version 2 and the next
    # `migrate` inserts the very same row again; ClickHouse 26.x dropped that
    # insert as a duplicate of the first one, so version 2 stayed pending.
    cluster = ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        driver=driver,
    )
    cluster.migrate("pytest", DOWN_MIGRATIONS, cluster_name=CLUSTER_NAME)
    assert cluster.rollback("pytest", DOWN_MIGRATIONS, steps=1) == [2]

    assert [
        m.version
        for m in cluster.migrate("pytest", DOWN_MIGRATIONS, cluster_name=CLUSTER_NAME)
    ] == [2]

    assert _states(cluster) == {1: STATUS_APPLIED, 2: STATUS_APPLIED}
    # ... and the next `down` rolls back 2 again, not 1.
    assert cluster.rollback("pytest", DOWN_MIGRATIONS, steps=1) == [2]
    assert _states(cluster) == {1: STATUS_APPLIED, 2: STATUS_PENDING}
