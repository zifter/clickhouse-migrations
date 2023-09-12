import logging
from pathlib import Path

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.types import MigrationStorage

TESTS_DIR = Path(__file__).parent
MIGRATIONS = MigrationStorage(TESTS_DIR / "migrations").migrations()


@pytest.fixture
def cluster():
    return ClickhouseCluster("localhost", "default", "")


@pytest.fixture(name="_clean_slate")
def clean_slate(cluster):
    with cluster.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest ON CLUSTER company_cluster")


@pytest.fixture(name="_schema")
def schema(cluster, _clean_slate):
    conn = cluster.connection("")
    conn.execute("CREATE DATABASE pytest ON CLUSTER company_cluster")
    conn = cluster.connection("pytest")
    cluster.init_schema("pytest")

    return conn


def test_dryrun(_schema, cluster, caplog):
    with caplog.at_level(logging.INFO):
        cluster.migrate("pytest", TESTS_DIR / "migrations", dryrun=True)

        with cluster.connection("pytest") as conn:
            assert (
                conn.execute("SELECT count(*) FROM pytest.schema_versions")[0][0] == 0
            )

            assert (
                conn.execute(
                    "SELECT count(*) FROM system.tables WHERE database = 'pytest' AND name = 'sample'"
                )[0][0]
                == 0
            )

    assert f"Dry run mode, would have executed: {MIGRATIONS[0].script}" in caplog.text


def test_not_dryrun(_schema, cluster, caplog):
    with caplog.at_level(logging.INFO):
        cluster.migrate("pytest", TESTS_DIR / "migrations", dryrun=False)

        with cluster.connection("pytest") as conn:
            assert (
                conn.execute("SELECT count(*) FROM pytest.schema_versions")[0][0] == 1
            )
            assert (
                conn.execute(
                    "SELECT count(*) FROM system.tables WHERE database = 'pytest' AND name = 'sample'"
                )[0][0]
                == 1
            )

    assert (
        f"Dry run mode, would have executed: {MIGRATIONS[0].script}" not in caplog.text
    )
