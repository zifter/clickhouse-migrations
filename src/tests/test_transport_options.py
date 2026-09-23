import logging
from unittest.mock import MagicMock

import pytest

from clickhouse_migrations import command_line
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import get_context, parse_setting
from clickhouse_migrations.connection import (
    CLICKHOUSE_CONNECT,
    CLICKHOUSE_DRIVER,
    TransportOptions,
    transport_kwargs,
)
from clickhouse_migrations.exceptions import MigrationException

ALL_OPTIONS = {
    "ca_cert": "/tls/ca.pem",
    "cert": "/tls/client.pem",
    "key": "/tls/client.key",
    "verify": False,
    "connect_timeout": 3.5,
    "query_timeout": 900.0,
    "settings": {"max_threads": "3", "allow_suspicious_low_cardinality_types": "1"},
}

CONNECTING_SUBCOMMANDS = ("migrate", "status", "down", "dump", "unlock")

TRANSPORT_ARGS = [
    "--ca-cert",
    "/tls/ca.pem",
    "--cert",
    "/tls/client.pem",
    "--key",
    "/tls/client.key",
    "--no-verify",
    "--connect-timeout",
    "3.5",
    "--query-timeout",
    "900",
    "--setting",
    "max_threads=3",
    "--setting",
    "allow_suspicious_low_cardinality_types=1",
]


def _client(cluster, db_name="db"):
    # The clickhouse-driver Client behind a connection; nothing connects yet.
    return cluster.connection(db_name)._client  # pylint: disable=protected-access


@pytest.fixture(name="fake_connect")
def fake_connect_fixture(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr(
        "clickhouse_migrations.clickhouse_cluster.import_clickhouse_connect",
        lambda: fake,
    )
    return fake


def _connect_kwargs(fake_connect, cluster, db_name="db"):
    cluster.connection(db_name).__exit__(None, None, None)
    return fake_connect.get_client.call_args.kwargs


# ---- translation -----------------------------------------------------------


def test_transport_kwargs_clickhouse_driver_names():
    assert transport_kwargs(TransportOptions(**ALL_OPTIONS), CLICKHOUSE_DRIVER) == {
        "ca_certs": "/tls/ca.pem",
        "certfile": "/tls/client.pem",
        "keyfile": "/tls/client.key",
        "verify": False,
        "connect_timeout": 3.5,
        "send_receive_timeout": 900.0,
        "settings": ALL_OPTIONS["settings"],
    }


def test_transport_kwargs_clickhouse_connect_names():
    assert transport_kwargs(TransportOptions(**ALL_OPTIONS), CLICKHOUSE_CONNECT) == {
        "ca_cert": "/tls/ca.pem",
        "client_cert": "/tls/client.pem",
        "client_cert_key": "/tls/client.key",
        "verify": False,
        "connect_timeout": 3.5,
        "send_receive_timeout": 900.0,
        "settings": ALL_OPTIONS["settings"],
    }


@pytest.mark.parametrize("driver", [CLICKHOUSE_DRIVER, CLICKHOUSE_CONNECT])
def test_transport_kwargs_unset_options_keep_driver_defaults(driver):
    assert not transport_kwargs(TransportOptions(), driver)
    assert not transport_kwargs(TransportOptions(settings={}), driver)


def test_transport_kwargs_copies_settings():
    settings = {"max_threads": "3"}
    kwargs = transport_kwargs(TransportOptions(settings=settings), CLICKHOUSE_DRIVER)
    kwargs["settings"]["max_threads"] = "9"

    assert settings == {"max_threads": "3"}


def test_key_needs_cert():
    with pytest.raises(MigrationException, match="--cert"):
        ClickhouseCluster(key="/tls/client.key")


@pytest.mark.parametrize("name", ["connect_timeout", "query_timeout"])
@pytest.mark.parametrize("value", [0, -1])
def test_timeouts_must_be_positive(name, value):
    with pytest.raises(MigrationException, match=name):
        ClickhouseCluster(**{name: value})


# ---- clickhouse-driver -----------------------------------------------------


def test_driver_host_path_gets_every_option():
    client = _client(ClickhouseCluster(secure=True, **ALL_OPTIONS))
    conn = client.connection

    assert conn.ssl_options["ca_certs"] == "/tls/ca.pem"
    assert conn.ssl_options["certfile"] == "/tls/client.pem"
    assert conn.ssl_options["keyfile"] == "/tls/client.key"
    assert conn.verify_cert is False
    assert conn.connect_timeout == 3.5
    assert conn.send_receive_timeout == 900.0
    assert conn.settings_is_important is True
    assert client.settings == ALL_OPTIONS["settings"]


def test_driver_db_url_path_gets_every_option():
    cluster = ClickhouseCluster(
        db_url="clickhouses://u:p@ch:9440/mydb?max_threads=2&max_memory_usage=5",
        **ALL_OPTIONS,
    )
    client = _client(cluster, "mydb")
    conn = client.connection

    assert list(conn.hosts) == [("ch", 9440)]
    assert (conn.user, conn.database) == ("u", "mydb")
    assert conn.secure_socket is True
    assert conn.ssl_options["ca_certs"] == "/tls/ca.pem"
    assert conn.ssl_options["certfile"] == "/tls/client.pem"
    assert conn.ssl_options["keyfile"] == "/tls/client.key"
    assert conn.verify_cert is False
    assert conn.connect_timeout == 3.5
    assert conn.send_receive_timeout == 900.0
    # URL query settings are kept; an explicit setting of the same name wins.
    assert client.settings == {
        "max_threads": "3",
        "max_memory_usage": "5",
        "allow_suspicious_low_cardinality_types": "1",
    }


def test_driver_explicit_options_win_over_url_query():
    cluster = ClickhouseCluster(
        db_url="clickhouse://ch/db?connect_timeout=5&send_receive_timeout=6&verify=false",
        connect_timeout=7,
        query_timeout=8,
        verify=True,
    )
    conn = _client(cluster).connection

    assert (conn.connect_timeout, conn.send_receive_timeout) == (7, 8)
    assert conn.verify_cert is True


def test_driver_url_query_options_still_work_alone():
    cluster = ClickhouseCluster(db_url="clickhouse://ch/db?connect_timeout=5")
    client = _client(cluster)

    assert client.connection.connect_timeout == 5
    assert client.connection.settings_is_important is False
    assert not client.settings


def test_driver_explicit_options_win_over_kwargs():
    cluster = ClickhouseCluster(
        send_receive_timeout=10, sync_request_timeout=4, query_timeout=5
    )
    conn = _client(cluster).connection

    assert (conn.send_receive_timeout, conn.sync_request_timeout) == (5, 4)


def test_driver_kwargs_apply_with_db_url_too():
    cluster = ClickhouseCluster(db_url="clickhouse://ch/db", send_receive_timeout=11)

    assert _client(cluster).connection.send_receive_timeout == 11


def test_driver_settings_is_important_can_be_turned_off():
    cluster = ClickhouseCluster(settings={"a": "1"}, settings_is_important=False)

    assert _client(cluster).connection.settings_is_important is False


def test_driver_without_options_keeps_defaults():
    client = _client(ClickhouseCluster())

    assert not client.settings
    assert client.connection.verify_cert is True
    assert client.connection.settings_is_important is False
    assert client.connection.send_receive_timeout == 300


# ---- clickhouse-connect ----------------------------------------------------


def test_connect_host_path_gets_every_option(fake_connect):
    cluster = ClickhouseCluster(driver=CLICKHOUSE_CONNECT, secure=True, **ALL_OPTIONS)

    assert _connect_kwargs(fake_connect, cluster) == {
        "host": "localhost",
        "port": 8123,
        "username": "default",
        "password": "",
        "database": "db",
        "secure": True,
        "ca_cert": "/tls/ca.pem",
        "client_cert": "/tls/client.pem",
        "client_cert_key": "/tls/client.key",
        "verify": False,
        "connect_timeout": 3.5,
        "send_receive_timeout": 900.0,
        "settings": ALL_OPTIONS["settings"],
    }


def test_connect_db_url_path_gets_every_option(fake_connect):
    cluster = ClickhouseCluster(
        db_url="clickhouses://u:p@ch/mydb?connect_timeout=5&compress=false",
        driver=CLICKHOUSE_CONNECT,
        **ALL_OPTIONS,
    )

    kwargs = _connect_kwargs(fake_connect, cluster)
    # The explicit connect_timeout is removed from the DSN, where it would win.
    assert kwargs.pop("dsn") == "https://u:p@ch?compress=false"
    assert kwargs == {
        "interface": "https",
        "database": "db",
        "ca_cert": "/tls/ca.pem",
        "client_cert": "/tls/client.pem",
        "client_cert_key": "/tls/client.key",
        "verify": False,
        "connect_timeout": 3.5,
        "send_receive_timeout": 900.0,
        "settings": ALL_OPTIONS["settings"],
    }


def test_connect_db_url_without_options_keeps_the_dsn(fake_connect):
    cluster = ClickhouseCluster(
        db_url="https://ch/mydb?connect_timeout=5", driver=CLICKHOUSE_CONNECT
    )

    assert _connect_kwargs(fake_connect, cluster, "mydb") == {
        "dsn": "https://ch?connect_timeout=5",
        "interface": "https",
        "database": "mydb",
    }


# ---- warnings --------------------------------------------------------------


@pytest.mark.parametrize("driver", [CLICKHOUSE_DRIVER, CLICKHOUSE_CONNECT])
def test_no_verify_logs_a_warning(caplog, driver):
    with caplog.at_level(logging.WARNING):
        ClickhouseCluster(driver=driver, secure=True, verify=False)

    assert "verification is disabled" in caplog.text


@pytest.mark.parametrize("verify", [None, True])
def test_verify_on_logs_nothing(caplog, verify):
    with caplog.at_level(logging.WARNING):
        ClickhouseCluster(secure=True, verify=verify, ca_cert="/tls/ca.pem")

    assert not caplog.text


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"db_url": "clickhouse://ch/db"},
        {"db_url": "clickhouse://ch/db?secure=false"},
        {"db_url": "http://ch/db", "driver": CLICKHOUSE_CONNECT},
    ],
)
def test_tls_files_without_tls_log_a_warning(caplog, kwargs):
    with caplog.at_level(logging.WARNING):
        ClickhouseCluster(cert="/tls/client.pem", **kwargs)

    assert "no effect on a plain connection" in caplog.text


@pytest.mark.parametrize(
    "kwargs",
    [
        {"secure": True},
        {"db_url": "clickhouse://ch/db", "secure": True},
        {"db_url": "clickhouse://ch/db?secure=true"},
        {"db_url": "clickhouses://ch/db"},
        {"db_url": "https://ch/db", "driver": CLICKHOUSE_CONNECT},
        {"db_url": "clickhouse://ch/db", "driver": CLICKHOUSE_CONNECT, "secure": True},
    ],
)
def test_tls_files_with_tls_log_nothing(caplog, kwargs):
    with caplog.at_level(logging.WARNING):
        ClickhouseCluster(ca_cert="/tls/ca.pem", **kwargs)

    assert not caplog.text


# ---- command line ----------------------------------------------------------


@pytest.mark.parametrize("command", CONNECTING_SUBCOMMANDS)
def test_transport_defaults(command):
    ctx = get_context([command])

    assert (ctx.ca_cert, ctx.cert, ctx.key) == (None, None, None)
    assert ctx.verify is True
    assert (ctx.connect_timeout, ctx.query_timeout) == (None, None)
    assert ctx.settings == {}


@pytest.mark.parametrize("command", CONNECTING_SUBCOMMANDS)
def test_transport_flags_on_every_connecting_subcommand(command):
    ctx = get_context([command, *TRANSPORT_ARGS])

    assert (ctx.ca_cert, ctx.cert, ctx.key) == (
        "/tls/ca.pem",
        "/tls/client.pem",
        "/tls/client.key",
    )
    assert ctx.verify is False
    assert (ctx.connect_timeout, ctx.query_timeout) == (3.5, 900.0)
    assert ctx.settings == ALL_OPTIONS["settings"]


def test_new_takes_no_transport_flags(capsys):
    with pytest.raises(SystemExit):
        get_context(["new", "x", "--setting", "a=1"])
    capsys.readouterr()


def test_transport_flags_from_env(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_CA_CERT", "/env/ca.pem")
    monkeypatch.setenv("CLICKHOUSE_CERT", "/env/client.pem")
    monkeypatch.setenv("CLICKHOUSE_KEY", "/env/client.key")
    monkeypatch.setenv("CLICKHOUSE_VERIFY", "0")
    monkeypatch.setenv("CLICKHOUSE_CONNECT_TIMEOUT", "2")
    monkeypatch.setenv("CLICKHOUSE_QUERY_TIMEOUT", "0.5")
    monkeypatch.setenv("CLICKHOUSE_SETTINGS", " max_threads=2 , a=x=y,, b= ")

    ctx = get_context(["status"])

    assert (ctx.ca_cert, ctx.cert, ctx.key) == (
        "/env/ca.pem",
        "/env/client.pem",
        "/env/client.key",
    )
    assert ctx.verify is False
    assert (ctx.connect_timeout, ctx.query_timeout) == (2.0, 0.5)
    assert ctx.settings == {"max_threads": "2", "a": "x=y", "b": ""}


def test_flags_win_over_env(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_VERIFY", "0")
    monkeypatch.setenv("CLICKHOUSE_QUERY_TIMEOUT", "5")
    monkeypatch.setenv("CLICKHOUSE_SETTINGS", "max_threads=2,max_memory_usage=1")

    ctx = get_context(
        ["--verify", "--query-timeout", "6", "--setting", "max_threads=4"]
    )

    assert ctx.verify is True
    assert ctx.query_timeout == 6.0
    assert ctx.settings == {"max_threads": "4", "max_memory_usage": "1"}


def test_repeated_setting_last_wins():
    ctx = get_context(["--setting", "a=1", "--setting", "a=2"])

    assert ctx.settings == {"a": "2"}


@pytest.mark.parametrize("item", ["a", "=1", "1a=2", "a b=1", "a-b=1", ""])
def test_malformed_setting_is_rejected(capsys, item):
    with pytest.raises(SystemExit) as exc:
        get_context(["--setting", item])

    assert exc.value.code == 2
    assert "expected a ClickHouse setting as name=value" in capsys.readouterr().err


def test_malformed_settings_env_is_rejected(monkeypatch, capsys):
    monkeypatch.setenv("CLICKHOUSE_SETTINGS", "a=1,oops")

    with pytest.raises(SystemExit) as exc:
        get_context(["dump"])

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "CLICKHOUSE_SETTINGS" in err
    assert "'oops'" in err
    assert "clickhouse-migrations dump" in err


def test_parse_setting_keeps_the_value_text():
    assert parse_setting("max_threads = 3") == ("max_threads", "3")
    assert parse_setting("s='a,b'") == ("s", "'a,b'")


@pytest.mark.parametrize("value", ["0", "-1", "abc", "nan", "inf"])
@pytest.mark.parametrize("flag", ["--connect-timeout", "--query-timeout"])
def test_bad_timeouts_are_rejected(capsys, flag, value):
    with pytest.raises(SystemExit):
        get_context([flag, value])

    assert "expected a positive number" in capsys.readouterr().err


def test_bad_timeout_env_is_rejected(monkeypatch, capsys):
    monkeypatch.setenv("CLICKHOUSE_CONNECT_TIMEOUT", "soon")

    with pytest.raises(SystemExit):
        get_context([])

    assert "expected a positive number" in capsys.readouterr().err


def test_create_cluster_passes_transport_options():
    cluster = command_line.create_cluster(get_context(["--secure", *TRANSPORT_ARGS]))

    assert cluster.transport == TransportOptions(**ALL_OPTIONS)


def test_create_cluster_without_transport_options():
    cluster = command_line.create_cluster(get_context([]))

    assert cluster.transport == TransportOptions(verify=True)


def test_main_no_verify_warns(monkeypatch, caplog, capsys):
    calls = []

    class _Cluster(ClickhouseCluster):
        def status(self, db_name, migration_path, explicit_migrations=None):
            calls.append(self.transport.verify)
            return []

    monkeypatch.setattr(command_line, "ClickhouseCluster", _Cluster)
    monkeypatch.setattr(
        "sys.argv", ["clickhouse-migrations", "status", "--secure", "--no-verify"]
    )

    with caplog.at_level(logging.WARNING):
        assert command_line.main() == 0

    assert calls == [False]
    assert "verification is disabled" in caplog.text
    capsys.readouterr()


def test_main_key_without_cert_is_a_clean_error(monkeypatch, caplog):
    monkeypatch.setattr(
        "sys.argv", ["clickhouse-migrations", "unlock", "--key", "/tls/client.key"]
    )

    assert command_line.main() == 1
    assert "--cert" in caplog.text
