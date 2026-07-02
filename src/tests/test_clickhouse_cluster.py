import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.util import quote_identifier


def _native(cluster, db_name):
    # Reach the underlying clickhouse-driver Connection for white-box assertions.
    conn = cluster.connection(db_name)
    return conn._client.connection  # pylint: disable=protected-access


def test_db_url_extracts_database_name():
    cluster = ClickhouseCluster(db_url="clickhouse://default:@localhost:9000/mydb")
    assert cluster.default_db_name == "mydb"


def test_db_url_without_database_keeps_none():
    cluster = ClickhouseCluster(db_url="clickhouse://default:@localhost:9000")
    assert cluster.default_db_name is None


def test_db_url_connection_uses_database_as_path():
    cluster = ClickhouseCluster(db_url="clickhouse://default:@localhost:9000/mydb")
    conn = _native(cluster, "otherdb")

    assert conn.database == "otherdb"
    assert conn.secure_socket is False


def test_db_url_secure_flag_is_honored():
    cluster = ClickhouseCluster(
        db_url="clickhouse://default:@localhost:9000/mydb", secure=True
    )
    conn = _native(cluster, "mydb")

    assert conn.secure_socket is True
    assert conn.database == "mydb"


def test_db_url_secure_survives_existing_query_params():
    cluster = ClickhouseCluster(
        db_url="clickhouse://default:@localhost:9000/mydb?connect_timeout=5",
        secure=True,
    )
    conn = _native(cluster, "mydb")

    assert conn.secure_socket is True
    assert conn.database == "mydb"


def test_host_based_secure_flag_is_honored():
    cluster = ClickhouseCluster(
        db_host="localhost", db_user="default", db_password="", secure=True
    )
    conn = _native(cluster, "mydb")

    assert conn.secure_socket is True


def test_host_based_defaults_to_insecure():
    cluster = ClickhouseCluster(db_host="localhost", db_user="default", db_password="")
    conn = _native(cluster, "mydb")

    assert conn.secure_socket is False


def test_port_defaults_per_driver():
    # pylint: disable=protected-access
    driver_cluster = ClickhouseCluster(db_host="h")
    assert driver_cluster._resolved_port() == 9000

    connect_cluster = ClickhouseCluster(db_host="h", driver="clickhouse-connect")
    assert connect_cluster._resolved_port() == 8123


def test_explicit_port_overrides_default():
    cluster = ClickhouseCluster(
        db_host="h", db_port="9999", driver="clickhouse-connect"
    )
    assert cluster._resolved_port() == "9999"  # pylint: disable=protected-access


def test_db_url_with_connect_driver_raises():
    with pytest.raises(MigrationException, match="db_url is only supported"):
        ClickhouseCluster(
            db_url="clickhouse://default:@localhost:9000/db",
            driver="clickhouse-connect",
        )


def test_quote_identifier_wraps_in_double_quotes():
    assert quote_identifier("mydb") == '"mydb"'


def test_quote_identifier_escapes_embedded_quote():
    assert quote_identifier('a"b') == '"a""b"'
