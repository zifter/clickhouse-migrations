import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster

from migrator import Migrator


@pytest.fixture
def cluster() -> ClickhouseCluster:
    return ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
    )

@pytest.fixture(autouse=True)
def before(cluster: ClickhouseCluster):
    with cluster.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest SYNC")
        conn.execute("CREATE DATABASE pytest")


@pytest.fixture(name="_clean_slate")
def clean_slate(cluster):
    with cluster.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest ON CLUSTER company_cluster SYNC")


@pytest.fixture(name="_schema")
def schema(cluster, _clean_slate):
    conn = cluster.connection("")
    conn.execute("CREATE DATABASE pytest ON CLUSTER company_cluster")
    conn = cluster.connection("pytest")
    migrator = Migrator(conn)
    migrator.init_schema("company_cluster")

    return conn
