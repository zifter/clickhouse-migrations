from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.util import quote_identifier


def test_db_url_extracts_database_name():
    cluster = ClickhouseCluster(db_url="clickhouse://default:@localhost:9000/mydb")
    assert cluster.default_db_name == "mydb"


def test_db_url_without_database_keeps_none():
    cluster = ClickhouseCluster(db_url="clickhouse://default:@localhost:9000")
    assert cluster.default_db_name is None


def test_db_url_connection_uses_database_as_path():
    cluster = ClickhouseCluster(db_url="clickhouse://default:@localhost:9000/mydb")
    client = cluster.connection("otherdb")

    assert client.connection.database == "otherdb"
    assert client.connection.secure_socket is False


def test_db_url_secure_flag_is_honored():
    cluster = ClickhouseCluster(
        db_url="clickhouse://default:@localhost:9000/mydb", secure=True
    )
    client = cluster.connection("mydb")

    assert client.connection.secure_socket is True
    assert client.connection.database == "mydb"


def test_db_url_secure_survives_existing_query_params():
    cluster = ClickhouseCluster(
        db_url="clickhouse://default:@localhost:9000/mydb?connect_timeout=5",
        secure=True,
    )
    client = cluster.connection("mydb")

    assert client.connection.secure_socket is True
    assert client.connection.database == "mydb"


def test_host_based_secure_flag_is_honored():
    cluster = ClickhouseCluster(
        db_host="localhost", db_user="default", db_password="", secure=True
    )
    client = cluster.connection("mydb")

    assert client.connection.secure_socket is True


def test_host_based_defaults_to_insecure():
    cluster = ClickhouseCluster(db_host="localhost", db_user="default", db_password="")
    client = cluster.connection("mydb")

    assert client.connection.secure_socket is False


def test_quote_identifier_wraps_in_double_quotes():
    assert quote_identifier("mydb") == '"mydb"'


def test_quote_identifier_escapes_embedded_quote():
    assert quote_identifier('a"b') == '"a""b"'
