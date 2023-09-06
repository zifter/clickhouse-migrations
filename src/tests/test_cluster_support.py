import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.migrator import Migrator


@pytest.fixture
def cluster():
    return ClickhouseCluster("localhost", "default", "")


@pytest.fixture
def schema(cluster):
    conn = cluster.connection("pytest")
    conn.execute("DROP DATABASE IF EXISTS pytest ON CLUSTER company_cluster")
    conn.execute("CREATE DATABASE pytest ON CLUSTER company_cluster")
    migrator = Migrator(conn)
    migrator.init_schema("company_cluster")

    return conn


def test_replicated_schema(schema):
    clickhouse_servers = (
        "clickhouse01",
        "clickhouse02",
        "clickhouse03",
        "clickhouse04",
    )
    for server in clickhouse_servers:
        table_engine = schema.execute(
            f"select engine_full from remote('{server}', 'system.tables') where database = 'pytest' and name = 'schema_versions'"
        )[0][0]
        assert (
            table_engine
            == "ReplicatedMergeTree('/clickhouse/tables/{database}/schema_versions', '{replica}') ORDER BY tuple(created_at) SETTINGS index_granularity = 8192"
        )
