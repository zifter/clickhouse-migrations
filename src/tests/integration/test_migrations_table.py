import sys
from pathlib import Path

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migrator import STATUS_APPLIED, STATUS_PENDING

TESTS_DIR = Path(__file__).parents[1]
MIGRATIONS = TESTS_DIR / "migrations"
DOWN_MIGRATIONS = TESTS_DIR / "down_migrations"

CLICKHOUSE_SERVERS = (
    "clickhouse01",
    "clickhouse02",
    "clickhouse03",
    "clickhouse04",
)

# NB: no {shard} macro here on purpose - the dev cluster sets insert_quorum=4,
# so every node must share one replication path.
CUSTOM_ZK_PATH = "/custom/zk/tables/{database}/{table}"
CUSTOM_ENGINE = f"ReplicatedMergeTree('{CUSTOM_ZK_PATH}', '{{replica}}')"


@pytest.fixture(name="custom_table_cluster")
def custom_table_cluster() -> ClickhouseCluster:
    return ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        migrations_table="my_versions",
    )


def test_custom_table_name_is_used_end_to_end(custom_table_cluster):
    custom_table_cluster.migrate("pytest", MIGRATIONS)

    tables = custom_table_cluster.show_tables("pytest")
    assert "my_versions" in tables
    assert "schema_versions" not in tables

    rows = custom_table_cluster.status("pytest", MIGRATIONS)
    assert [r.state for r in rows] == [STATUS_APPLIED]


def test_status_is_pending_before_migrate_with_custom_table(custom_table_cluster):
    rows = custom_table_cluster.status("pytest", MIGRATIONS)

    assert [r.state for r in rows] == [STATUS_PENDING]


def test_rollback_with_custom_table_name(custom_table_cluster):
    custom_table_cluster.migrate("pytest", DOWN_MIGRATIONS)

    assert custom_table_cluster.rollback("pytest", DOWN_MIGRATIONS, steps=1) == [2]

    states = {
        r.version: r.state
        for r in custom_table_cluster.status("pytest", DOWN_MIGRATIONS)
    }
    assert states == {1: STATUS_APPLIED, 2: STATUS_PENDING}


def test_migrations_table_in_another_database(cluster: ClickhouseCluster):
    with cluster.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest_meta SYNC")
        conn.execute("CREATE DATABASE pytest_meta")

    meta_cluster = ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        migrations_table="pytest_meta.my_versions",
    )
    meta_cluster.migrate("pytest", MIGRATIONS)

    assert "my_versions" not in meta_cluster.show_tables("pytest")
    assert "my_versions" in meta_cluster.show_tables("pytest_meta")

    rows = meta_cluster.status("pytest", MIGRATIONS)
    assert [r.state for r in rows] == [STATUS_APPLIED]

    with cluster.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest_meta SYNC")


def test_missing_migrations_table_database_is_explained(cluster: ClickhouseCluster):
    with cluster.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest_missing SYNC")

    missing_cluster = ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        migrations_table="pytest_missing.my_versions",
    )

    with pytest.raises(MigrationException, match="is not created automatically"):
        missing_cluster.migrate("pytest", MIGRATIONS)


def test_custom_replicated_path_on_cluster(_clean_slate, cluster: ClickhouseCluster):
    with cluster.connection("") as conn:
        conn.execute("CREATE DATABASE pytest ON CLUSTER company_cluster")

    replicated_cluster = ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        migrations_table_engine=CUSTOM_ENGINE,
    )
    replicated_cluster.migrate(
        "pytest",
        MIGRATIONS,
        cluster_name="company_cluster",
        create_db_if_no_exists=False,
    )

    with cluster.connection("pytest") as conn:
        for server in CLICKHOUSE_SERVERS:
            engine_full = conn.execute(  # pylint: disable=no-member
                f"select engine_full from remote('{server}', 'system.tables') "
                "where database = 'pytest' and name = 'schema_versions'"
            )[0][0]
            assert engine_full.startswith("ReplicatedMergeTree('/custom/zk/")
            assert "/clickhouse/tables/" not in engine_full

    rows = replicated_cluster.status("pytest", MIGRATIONS)
    assert [r.state for r in rows] == [STATUS_APPLIED]


def test_main_uses_migrations_table_flag(cluster: ClickhouseCluster, monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "clickhouse-migrations",
            "--db-name",
            "pytest",
            "--migrations-dir",
            str(MIGRATIONS),
            "--migrations-table",
            "cli_versions",
        ],
    )

    assert main() == 0
    assert "cli_versions" in cluster.show_tables("pytest")
