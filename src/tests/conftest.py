import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster


@pytest.fixture
def cluster():
    return ClickhouseCluster("localhost", "default", "")


@pytest.fixture(autouse=True)
def before(cluster: ClickhouseCluster):
    with cluster.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest")
        conn.execute("CREATE DATABASE pytest")
