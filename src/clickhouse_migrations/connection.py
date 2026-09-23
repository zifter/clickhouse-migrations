import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, NamedTuple, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from clickhouse_migrations.exceptions import MigrationException

CLICKHOUSE_DRIVER = "clickhouse-driver"
CLICKHOUSE_CONNECT = "clickhouse-connect"
DRIVERS = (CLICKHOUSE_DRIVER, CLICKHOUSE_CONNECT)

# Default native (clickhouse-driver) / HTTP (clickhouse-connect) ports, used
# when the caller does not set a port explicitly.
DEFAULT_PORT = {CLICKHOUSE_DRIVER: 9000, CLICKHOUSE_CONNECT: 8123}

# URL schemes accepted by ``db_url`` for each driver. clickhouse-driver speaks
# the native protocol (clickhouse://, clickhouses:// for TLS); clickhouse-connect
# speaks HTTP(S) (http://, https://) and also accepts the native-style schemes,
# which are mapped to their HTTP(S) equivalents.
NATIVE_SCHEMES = ("clickhouse", "clickhouses")
HTTP_SCHEMES = ("http", "https")
ACCEPTED_SCHEMES = {
    CLICKHOUSE_DRIVER: NATIVE_SCHEMES,
    CLICKHOUSE_CONNECT: NATIVE_SCHEMES + HTTP_SCHEMES,
}
_TRUTHY = ("1", "true", "yes", "on")


def normalize_db_url(url: str, driver: str, secure: bool = False) -> str:
    """Validate the scheme of a connection URL and adapt it to the driver.

    clickhouse-driver: ``clickhouse://`` and ``clickhouses://`` are used as is;
    ``http(s)://`` is rejected.

    clickhouse-connect: ``clickhouse://`` becomes ``http://`` and
    ``clickhouses://`` becomes ``https://``; ``http(s)://`` passes through.
    ``secure`` (the argument or a ``secure=true`` query parameter) upgrades an
    ``http`` URL to ``https``; it never downgrades an ``https`` one. The URL
    itself is never echoed in errors because it may contain a password.
    """
    scheme = urlparse(url).scheme.lower()
    accepted = ACCEPTED_SCHEMES[driver]
    if scheme not in accepted:
        hint = ""
        if scheme in HTTP_SCHEMES:
            hint = " (http/https URLs need --driver clickhouse-connect)"
        raise MigrationException(
            f"Unsupported db_url scheme {scheme!r} for {driver}{hint}; "
            f"accepted schemes: {', '.join(s + '://' for s in accepted)}"
        )

    if driver == CLICKHOUSE_DRIVER:
        return url

    parsed = urlparse(url)
    query = []
    for name, value in parse_qsl(parsed.query, keep_blank_values=True):
        if name == "secure":
            secure = secure or value.lower() in _TRUTHY
        else:
            query.append((name, value))

    is_tls = scheme in ("clickhouses", "https") or secure
    return urlunparse(
        parsed._replace(scheme="https" if is_tls else "http", query=urlencode(query))
    )


class TransportOptions(NamedTuple):
    """TLS, timeout and ClickHouse settings options, independent of the driver.

    ``None`` (and an empty ``settings``) leaves the driver's own default in
    place. ``transport_kwargs`` translates them into one driver's parameters.
    """

    ca_cert: Optional[str] = None
    cert: Optional[str] = None
    key: Optional[str] = None
    verify: Optional[bool] = None
    connect_timeout: Optional[float] = None
    query_timeout: Optional[float] = None
    settings: Optional[Dict[str, Any]] = None

    def has_tls_files(self) -> bool:
        return any((self.ca_cert, self.cert, self.key))

    def validate(self) -> None:
        if self.key and not self.cert:
            raise MigrationException(
                "A client key (--key) needs its client certificate (--cert)."
            )
        for name in ("connect_timeout", "query_timeout"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise MigrationException(
                    f"{name} must be a positive number of seconds, got {value!r}."
                )


# The driver parameter each TransportOptions field is passed as. Both drivers
# support every option; only the names differ. query_timeout is the socket
# read timeout of both drivers: over HTTP it bounds the wait for the response,
# over the native protocol the wait between two packets from the server.
TRANSPORT_PARAMETERS = {
    CLICKHOUSE_DRIVER: {
        "ca_cert": "ca_certs",
        "cert": "certfile",
        "key": "keyfile",
        "verify": "verify",
        "connect_timeout": "connect_timeout",
        "query_timeout": "send_receive_timeout",
        "settings": "settings",
    },
    CLICKHOUSE_CONNECT: {
        "ca_cert": "ca_cert",
        "cert": "client_cert",
        "key": "client_cert_key",
        "verify": "verify",
        "connect_timeout": "connect_timeout",
        "query_timeout": "send_receive_timeout",
        "settings": "settings",
    },
}


def transport_kwargs(options: TransportOptions, driver: str) -> Dict[str, Any]:
    """The client keyword arguments of ``driver`` for the options that are set.

    Settings go to the client itself (``settings=`` of ``clickhouse_driver.Client``
    and of ``clickhouse_connect.get_client``), so every statement sent over the
    connection carries them.
    """
    parameters = TRANSPORT_PARAMETERS[driver]
    kwargs = {
        parameters[name]: value
        for name, value in options._asdict().items()
        if value is not None and name != "settings"
    }
    if options.settings:
        kwargs["settings"] = dict(options.settings)
    return kwargs


def import_clickhouse_connect():
    try:
        import clickhouse_connect  # pylint: disable=import-outside-toplevel
    except ImportError as exc:
        raise MigrationException(
            "The clickhouse-connect driver is not installed. "
            "Install it with: pip install 'clickhouse-migrations[connect]'"
        ) from exc

    return clickhouse_connect


class Connection(ABC):
    """Driver-agnostic connection used by the migrator.

    Concrete implementations wrap a specific ClickHouse driver so the rest of
    the code base does not depend on any single driver's API.
    """

    @abstractmethod
    def command(self, statement: str) -> None:
        """Execute a statement that does not return rows (DDL/DML).

        The built-in connections also accept an optional ``log_statement``:
        what their debug log shows instead of ``statement``. The migrator
        passes it (only) for a statement whose ``${NAME}`` placeholders were
        substituted, with the raw text, so values never reach our logs.
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def query(self, statement: str) -> List[Dict]:
        """Execute a query and return its rows as dicts keyed by column name."""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def insert(self, table: str, rows: List[Dict]) -> None:
        """Insert a list of row dicts into a table."""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def __enter__(self) -> "Connection":
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        raise NotImplementedError  # pragma: no cover


class ClickhouseDriverConnection(Connection):
    """Connection backed by clickhouse-driver (native TCP protocol)."""

    def __init__(self, client):
        self._client = client

    def command(self, statement: str, log_statement: Optional[str] = None) -> None:
        logging.debug(statement if log_statement is None else log_statement)
        self._client.execute(statement)

    def query(self, statement: str) -> List[Dict]:
        logging.debug(statement)
        data, columns = self._client.execute(statement, with_column_types=True)
        names = [c[0] for c in columns]
        return [dict(zip(names, row)) for row in data]

    def insert(self, table: str, rows: List[Dict]) -> None:
        columns = list(rows[0].keys())
        column_list = ", ".join(columns)
        self._client.execute(f"INSERT INTO {table} ({column_list}) VALUES", rows)

    # Passthrough kept so existing tests and callers can use the native client
    # API directly against a clickhouse-driver connection.
    def execute(self, *args, **kwargs):
        return self._client.execute(*args, **kwargs)

    def __enter__(self) -> "ClickhouseDriverConnection":
        self._client.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._client.__exit__(exc_type, exc_value, traceback)


class ClickhouseConnectConnection(Connection):
    """Connection backed by clickhouse-connect (official HTTP driver)."""

    def __init__(self, client):
        self._client = client

    def command(self, statement: str, log_statement: Optional[str] = None) -> None:
        logging.debug(statement if log_statement is None else log_statement)
        self._client.command(statement)

    def query(self, statement: str) -> List[Dict]:
        logging.debug(statement)
        result = self._client.query(statement)
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def insert(self, table: str, rows: List[Dict]) -> None:
        columns = list(rows[0].keys())
        data = [[row[column] for column in columns] for row in rows]
        self._client.insert(table, data=data, column_names=columns)

    def __enter__(self) -> "ClickhouseConnectConnection":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._client.close()
