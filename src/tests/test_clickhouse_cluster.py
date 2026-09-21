from unittest.mock import MagicMock

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.connection import normalize_db_url
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.util import (
    format_table_reference,
    quote_identifier,
    quote_string,
    split_table_reference,
)


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


def test_db_url_connect_maps_native_scheme_to_http(caplog):
    with caplog.at_level("INFO"):
        cluster = ClickhouseCluster(
            db_url="clickhouse://user:s3cret@ch:8123/mydb",
            driver="clickhouse-connect",
        )

    assert cluster.default_db_name == "mydb"
    assert cluster.db_url == "http://user:s3cret@ch:8123"
    assert "http://ch:8123" in caplog.text
    assert "s3cret" not in caplog.text


def test_db_url_connect_logs_default_ports(caplog):
    with caplog.at_level("INFO"):
        ClickhouseCluster(db_url="clickhouse://ch", driver="clickhouse-connect")
        ClickhouseCluster(db_url="clickhouses://ch", driver="clickhouse-connect")

    assert "http://ch:8123" in caplog.text
    assert "https://ch:8443" in caplog.text


def test_db_url_native_does_not_log_resolved_endpoint(caplog):
    with caplog.at_level("INFO"):
        ClickhouseCluster(db_url="clickhouse://default:pw@ch:9000")

    assert "clickhouse-connect" not in caplog.text


@pytest.fixture(name="fake_connect")
def fake_connect_fixture(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr(
        "clickhouse_migrations.clickhouse_cluster.import_clickhouse_connect",
        lambda: fake,
    )
    return fake


def test_db_url_connect_get_client_uses_dsn(fake_connect):
    cluster = ClickhouseCluster(
        db_url="clickhouses://u:p%40ss@ch/mydb?connect_timeout=5",
        driver="clickhouse-connect",
    )
    cluster.connection("other").__exit__(None, None, None)
    cluster.connection("").__exit__(None, None, None)
    cluster.connection().__exit__(None, None, None)

    first, second, third = [c.kwargs for c in fake_connect.get_client.call_args_list]
    assert first == {
        "dsn": "https://u:p%40ss@ch?connect_timeout=5",
        "interface": "https",
        "database": "other",
    }
    assert second["database"] is None
    assert third["database"] == "mydb"


@pytest.mark.parametrize(
    "url, driver, secure, expected",
    [
        # clickhouse-driver: native schemes untouched
        ("clickhouse://h:9000", "clickhouse-driver", False, "clickhouse://h:9000"),
        ("clickhouses://h:9440", "clickhouse-driver", False, "clickhouses://h:9440"),
        ("clickhouse://h/db?a=1", "clickhouse-driver", True, "clickhouse://h/db?a=1"),
        # clickhouse-connect: native schemes mapped
        ("clickhouse://h:8123", "clickhouse-connect", False, "http://h:8123"),
        ("clickhouses://h:8443", "clickhouse-connect", False, "https://h:8443"),
        ("CLICKHOUSE://h", "clickhouse-connect", False, "http://h"),
        # clickhouse-connect: http(s) passes through
        ("http://h:8123", "clickhouse-connect", False, "http://h:8123"),
        ("https://h:8443", "clickhouse-connect", False, "https://h:8443"),
        # user:password, database, query string survive
        (
            "clickhouse://u:p@h:8123/db?connect_timeout=5",
            "clickhouse-connect",
            False,
            "http://u:p@h:8123/db?connect_timeout=5",
        ),
        # secure upgrades http/native, never downgrades
        ("clickhouse://h:8443", "clickhouse-connect", True, "https://h:8443"),
        ("http://h", "clickhouse-connect", True, "https://h"),
        ("https://h", "clickhouse-connect", False, "https://h"),
        ("clickhouses://h", "clickhouse-connect", False, "https://h"),
        # secure in the query string is honoured and removed
        ("clickhouse://h?secure=true", "clickhouse-connect", False, "https://h"),
        (
            "clickhouse://h?secure=1&x=y",
            "clickhouse-connect",
            False,
            "https://h?x=y",
        ),
        ("clickhouse://h?secure=false", "clickhouse-connect", False, "http://h"),
        ("clickhouses://h?secure=false", "clickhouse-connect", False, "https://h"),
    ],
)
def test_normalize_db_url(url, driver, secure, expected):
    assert normalize_db_url(url, driver, secure) == expected


@pytest.mark.parametrize(
    "url, driver, hint",
    [
        ("http://u:secret@h:8123", "clickhouse-driver", "clickhouse-connect"),
        ("https://u:secret@h:8443", "clickhouse-driver", "clickhouse-connect"),
        ("ftp://u:secret@h", "clickhouse-driver", "clickhouse://, clickhouses://"),
        ("ftp://u:secret@h", "clickhouse-connect", "http://, https://"),
        ("//u:secret@h", "clickhouse-connect", "accepted schemes"),
        ("h:8123", "clickhouse-connect", "accepted schemes"),
    ],
)
def test_normalize_db_url_rejects_unsupported_scheme(url, driver, hint):
    with pytest.raises(MigrationException, match="Unsupported db_url scheme") as exc:
        normalize_db_url(url, driver)

    assert hint in str(exc.value)
    assert "secret" not in str(exc.value)


def test_db_url_native_rejects_http_scheme():
    with pytest.raises(MigrationException, match="clickhouse-connect"):
        ClickhouseCluster(db_url="http://default:@localhost:8123/db")


def test_db_url_native_clickhouses_enables_tls():
    cluster = ClickhouseCluster(db_url="clickhouses://default:@localhost:9440/db")

    assert _native(cluster, "db").secure_socket is True


def test_db_url_connect_secure_flag_upgrades_to_https():
    cluster = ClickhouseCluster(
        db_url="clickhouse://default:@localhost/db",
        driver="clickhouse-connect",
        secure=True,
    )

    assert cluster.db_url == "https://default:@localhost"


def test_db_url_connect_secure_false_does_not_downgrade_clickhouses():
    cluster = ClickhouseCluster(
        db_url="clickhouses://default:@localhost/db",
        driver="clickhouse-connect",
        secure=False,
    )

    assert cluster.db_url.startswith("https://")


def test_quote_identifier_wraps_in_double_quotes():
    assert quote_identifier("mydb") == '"mydb"'


def test_quote_identifier_escapes_embedded_quote():
    assert quote_identifier('a"b') == '"a""b"'


def test_quote_string_escapes_quotes_and_backslashes():
    assert quote_string("a'b\\c") == "'a\\'b\\\\c'"


@pytest.mark.parametrize(
    "reference,expected",
    [
        ("schema_versions", (None, "schema_versions")),
        ("meta.schema_versions", ("meta", "schema_versions")),
        ('"my.db".events', ("my.db", "events")),
        ('meta."my.table"', ("meta", "my.table")),
        ("`meta`.`events`", ("meta", "events")),
        # Split on the LAST unquoted dot.
        ("a.b.c", ("a.b", "c")),
        # A doubled quote inside a quoted identifier is an escaped quote.
        ('"a""b".t', ('a"b', "t")),
    ],
)
def test_split_table_reference(reference, expected):
    assert split_table_reference(reference) == expected


@pytest.mark.parametrize("reference", ["", "meta.", ".events", '"".events'])
def test_split_table_reference_rejects_empty_parts(reference):
    with pytest.raises(MigrationException, match="Invalid table name"):
        split_table_reference(reference)


@pytest.mark.parametrize(
    "database,table,expected",
    [
        (None, "schema_versions", '"schema_versions"'),
        ("meta", "schema_versions", '"meta"."schema_versions"'),
        ('my"db', 'my"table', '"my""db"."my""table"'),
    ],
)
def test_format_table_reference(database, table, expected):
    assert format_table_reference(database, table) == expected


def test_cluster_defaults_keep_the_legacy_table():
    cluster = ClickhouseCluster(db_host="localhost")

    assert cluster.migrations_table == "schema_versions"
    assert cluster.migrations_table_engine is None


def test_cluster_passes_table_options_to_the_migrator():
    # pylint: disable=protected-access
    cluster = ClickhouseCluster(
        db_host="localhost",
        migrations_table="meta.my_versions",
        migrations_table_engine="Memory",
    )
    migrator = cluster._migrator(None)

    assert migrator.migrations_table_database == "meta"
    assert migrator.migrations_table_name == "my_versions"
    assert migrator._migrations_table_engine == "Memory"


def test_migrate_to_version_with_explicit_migrations_is_rejected(tmp_path):
    (tmp_path / "001_a.sql").write_text("SELECT 1;", encoding="utf8")
    cluster = ClickhouseCluster(db_host="localhost")

    with pytest.raises(MigrationException, match="mutually exclusive"):
        cluster.migrate("pytest", tmp_path, explicit_migrations=["1"], to_version=1)
