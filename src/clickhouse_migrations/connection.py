import logging
from abc import ABC, abstractmethod
from typing import Dict, List

from clickhouse_migrations.exceptions import MigrationException

CLICKHOUSE_DRIVER = "clickhouse-driver"
CLICKHOUSE_CONNECT = "clickhouse-connect"
DRIVERS = (CLICKHOUSE_DRIVER, CLICKHOUSE_CONNECT)

# Default native (clickhouse-driver) / HTTP (clickhouse-connect) ports, used
# when the caller does not set a port explicitly.
DEFAULT_PORT = {CLICKHOUSE_DRIVER: 9000, CLICKHOUSE_CONNECT: 8123}


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
        """Execute a statement that does not return rows (DDL/DML)."""
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

    def command(self, statement: str) -> None:
        logging.debug(statement)
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

    def command(self, statement: str) -> None:
        logging.debug(statement)
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
