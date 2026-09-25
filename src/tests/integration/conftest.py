import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.migrator import Migrator
from tests.clickhouse_version import skip_unless_server_at_least


@pytest.fixture
def cluster() -> ClickhouseCluster:
    return ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
    )


@pytest.fixture(name="server_version", scope="session")
def server_version_fixture() -> str:
    """The version of the dev cluster's server, e.g. ``'25.7.4.11'``."""
    server = ClickhouseCluster(db_host="localhost", db_user="default", db_password="")
    with server.connection("") as conn:
        return conn.query("SELECT version() AS v")[0]["v"]


@pytest.fixture(autouse=True)
def _clickhouse_min_version(request, server_version):
    """Honour ``@pytest.mark.clickhouse_min_version(minimum, reason=...)``."""
    marker = request.node.get_closest_marker("clickhouse_min_version")
    if marker is not None:
        skip_unless_server_at_least(server_version, *marker.args, **marker.kwargs)


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
