from pathlib import Path

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.migrator import Migrator

TESTS_DIR = Path(__file__).parent

CLICKHOUSE_SERVERS = (
    "clickhouse01",
    "clickhouse02",
    "clickhouse03",
    "clickhouse04",
)


@pytest.fixture
def cluster():
    return ClickhouseCluster(db_host="localhost", db_user="default", db_password="")


@pytest.fixture(name="_clean_slate")
def clean_slate(cluster):
    with cluster.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest ON CLUSTER company_cluster")


@pytest.fixture(name="_schema")
def schema(cluster, _clean_slate):
    conn = cluster.connection("")
    conn.execute("CREATE DATABASE pytest ON CLUSTER company_cluster")
    conn = cluster.connection("pytest")
    migrator = Migrator(conn)
    migrator.init_schema("company_cluster")

    return conn


def test_replicated_schema(_schema):
    with _schema:
        for server in CLICKHOUSE_SERVERS:
            table_engine = _schema.execute(
                f"select engine_full from remote('{server}', 'system.tables') where database = 'pytest' and name = 'schema_versions'"  # pylint: disable=C0301 # noqa: E501
            )[0][0]
            assert (
                table_engine
                == "ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY tuple(created_at) SETTINGS index_granularity = 8192"  # pylint: disable=C0301 # noqa: E501
            )


def test_distributed_database_create(cluster, _clean_slate):
    cluster.create_db("pytest", "company_cluster")
    with cluster.connection("") as conn:
        for server in CLICKHOUSE_SERVERS:
            assert (
                conn.execute(
                    f"select count(*) from remote('{server}', 'system.databases') where name = 'pytest'"
                )[0][0]
                == 1
            )


def test_migration_with_replicated_schema(_schema):
    cluster = ClickhouseCluster(db_host="localhost", db_user="default", db_password="")
    result_set = []

    cluster.migrate("pytest", TESTS_DIR / "migrations", "company_cluster")

    with cluster.connection("pytest") as conn:
        for server in CLICKHOUSE_SERVERS:
            assert (
                conn.execute(
                    f"select count(*) from remote('{server}', 'pytest.schema_versions')"
                )[0][0]
                == 1
            )
            result_set.append(
                conn.execute(
                    f"select * from remote('{server}', 'pytest.schema_versions')"
                )[0]
            )
        assert len(result_set) == len(CLICKHOUSE_SERVERS) and len(set(result_set)) == 1
