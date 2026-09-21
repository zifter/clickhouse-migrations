from pathlib import Path
from typing import List, Optional, Union
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from clickhouse_driver import Client

from clickhouse_migrations.connection import (
    CLICKHOUSE_CONNECT,
    CLICKHOUSE_DRIVER,
    DEFAULT_PORT,
    ClickhouseConnectConnection,
    ClickhouseDriverConnection,
    Connection,
    import_clickhouse_connect,
)
from clickhouse_migrations.defaults import (
    DB_HOST,
    DB_PASSWORD,
    DB_USER,
    MIGRATIONS_TABLE,
    MIGRATIONS_TABLE_ENGINE,
)
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration, MigrationStorage
from clickhouse_migrations.migrator import STATUS_PENDING, Migrator, StatusRow
from clickhouse_migrations.util import (
    quote_identifier,
    quote_string,
    split_table_reference,
)


class ClickhouseCluster:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        db_host: str = DB_HOST,
        db_user: str = DB_USER,
        db_password: str = DB_PASSWORD,
        db_port: Optional[str] = None,
        db_url: Optional[str] = None,
        db_name: Optional[str] = None,
        secure: bool = False,
        driver: str = CLICKHOUSE_DRIVER,
        migrations_table: str = MIGRATIONS_TABLE,
        migrations_table_engine: Optional[str] = MIGRATIONS_TABLE_ENGINE,
        **kwargs,
    ):
        self.db_url: Optional[str] = None
        self.default_db_name: Optional[str] = db_name
        self.secure: bool = secure
        self.driver: str = driver
        self.migrations_table: str = migrations_table
        self.migrations_table_engine: Optional[str] = migrations_table_engine
        self.connection_kwargs = kwargs
        self._parsed_url = None

        if db_url:
            if driver != CLICKHOUSE_DRIVER:
                raise MigrationException(
                    "db_url is only supported with the clickhouse-driver driver; "
                    "use db_host/db_port with clickhouse-connect"
                )
            parsed = urlparse(db_url)
            path_db = parsed.path.lstrip("/")
            if path_db:
                self.default_db_name = path_db

            query = dict(parse_qsl(parsed.query))
            if secure:
                query.setdefault("secure", "true")

            # Keep the base URL without a database in the path; connection()
            # re-adds the database as a proper path segment so it never lands
            # after the query string.
            self._parsed_url = parsed._replace(path="", query=urlencode(query))
            self.db_url = urlunparse(self._parsed_url)
        else:
            self.db_host = db_host
            self.db_port = db_port
            self.db_user = db_user
            self.db_password = db_password

    def _resolved_port(self):
        if self.db_port is not None:
            return self.db_port
        return DEFAULT_PORT[self.driver]

    def connection(self, db_name: Optional[str] = None) -> Connection:
        db_name = db_name if db_name is not None else self.default_db_name

        if self.driver == CLICKHOUSE_CONNECT:
            clickhouse_connect = import_clickhouse_connect()
            client = clickhouse_connect.get_client(
                host=self.db_host,
                port=int(self._resolved_port()),
                username=self.db_user,
                password=self.db_password,
                database=db_name or None,
                secure=self.secure,
            )
            return ClickhouseConnectConnection(client)

        if self._parsed_url is not None:
            parsed = self._parsed_url
            if db_name:
                parsed = parsed._replace(path="/" + db_name)
            ch_client = Client.from_url(urlunparse(parsed))
        else:
            ch_client = Client(
                self.db_host,
                port=self._resolved_port(),
                user=self.db_user,
                password=self.db_password,
                database=db_name,
                secure=self.secure,
                **self.connection_kwargs,
            )
        return ClickhouseDriverConnection(ch_client)

    def _migrator(
        self,
        conn: Connection,
        dryrun: bool = False,
        migration_log_format: str = "full",
    ) -> Migrator:
        return Migrator(
            conn,
            dryrun,
            migration_log_format=migration_log_format,
            migrations_table=self.migrations_table,
            migrations_table_engine=self.migrations_table_engine,
        )

    def _is_initialized(self, db_name: Optional[str]) -> bool:
        """Whether the bookkeeping table already exists.

        The table may live in another database than the migrated one, so the
        probe respects the database part of a "db.table" value.
        """
        table_db, table_name = split_table_reference(self.migrations_table)
        if table_db is None:
            table_db = db_name

        with self.connection("") as conn:
            return bool(
                conn.query(
                    "SELECT count() AS n FROM system.tables "
                    f"WHERE database = {quote_string(table_db)} "
                    f"AND name = {quote_string(table_name)}"
                )[0]["n"]
            )

    def create_db(
        self, db_name: Optional[str] = None, cluster_name: Optional[str] = None
    ):
        db_name = db_name if db_name is not None else self.default_db_name

        with self.connection("") as conn:
            if cluster_name is None:
                conn.command(
                    f"CREATE DATABASE IF NOT EXISTS {quote_identifier(db_name)}"
                )
            else:
                conn.command(
                    f"CREATE DATABASE IF NOT EXISTS {quote_identifier(db_name)} "
                    f"ON CLUSTER {quote_identifier(cluster_name)}"
                )

    def init_schema(
        self, db_name: Optional[str] = None, cluster_name: Optional[str] = None
    ):
        db_name = db_name if db_name is not None else self.default_db_name

        with self.connection(db_name) as conn:
            migrator = self._migrator(conn)
            migrator.init_schema(cluster_name)

    def show_tables(self, db_name):
        db_name = db_name if db_name is not None else self.default_db_name

        with self.connection(db_name) as conn:
            return [row["name"] for row in conn.query("SHOW TABLES")]

    def migrate(
        self,
        db_name: Optional[str],
        migration_path: Union[Path, str],
        cluster_name: Optional[str] = None,
        create_db_if_no_exists: bool = True,
        multi_statement: bool = True,
        dryrun: bool = False,
        explicit_migrations: Optional[List[str]] = None,
        fake: bool = False,
        migration_log_format: str = "full",
        to_version: Optional[int] = None,
    ):
        db_name = db_name if db_name is not None else self.default_db_name

        if to_version is not None and explicit_migrations:
            raise MigrationException(
                "to_version and explicit_migrations are mutually exclusive."
            )

        storage = MigrationStorage(migration_path)
        migrations = storage.migrations(explicit_migrations)

        return self.apply_migrations(
            db_name,
            migrations,
            cluster_name=cluster_name,
            create_db_if_no_exists=create_db_if_no_exists,
            multi_statement=multi_statement,
            dryrun=dryrun,
            fake=fake,
            migration_log_format=migration_log_format,
            to_version=to_version,
        )

    def status(
        self,
        db_name: Optional[str],
        migration_path: Union[Path, str],
        explicit_migrations: Optional[List[str]] = None,
    ) -> List[StatusRow]:
        db_name = db_name if db_name is not None else self.default_db_name

        storage = MigrationStorage(migration_path)
        incoming = storage.migrations(explicit_migrations)

        # Read-only: never create the database or the schema table. If the
        # schema table is missing, nothing has been applied yet.
        if not self._is_initialized(db_name):
            return [StatusRow(m.version, STATUS_PENDING, m.md5, None) for m in incoming]

        with self.connection(db_name) as conn:
            return self._migrator(conn).migration_status(incoming)

    def rollback(
        self,
        db_name: Optional[str],
        migration_path: Union[Path, str],
        steps: int = 1,
        to_version: Optional[int] = None,
        dryrun: bool = False,
        multi_statement: bool = True,
    ) -> List[int]:
        db_name = db_name if db_name is not None else self.default_db_name

        down_scripts = MigrationStorage(migration_path).down_scripts()

        # Read-only pre-check: if the schema table is missing, nothing has been
        # applied yet, so there is nothing to roll back.
        if not self._is_initialized(db_name):
            return []

        with self.connection(db_name) as conn:
            migrator = self._migrator(conn, dryrun)
            return migrator.rollback_migration(
                down_scripts,
                steps=steps,
                to_version=to_version,
                multi_statement=multi_statement,
            )

    def apply_migrations(
        self,
        db_name: str,
        migrations: List[Migration],
        dryrun: bool = False,
        cluster_name: Optional[str] = None,
        create_db_if_no_exists: bool = True,
        multi_statement: bool = True,
        fake: bool = False,
        migration_log_format: str = "full",
        to_version: Optional[int] = None,
    ) -> List[Migration]:
        if create_db_if_no_exists:
            if cluster_name is None:
                self.create_db(db_name)
            else:
                self.create_db(db_name, cluster_name)

        with self.connection(db_name) as conn:
            migrator = self._migrator(
                conn, dryrun, migration_log_format=migration_log_format
            )
            migrator.init_schema(cluster_name)
            return migrator.apply_migration(
                migrations, multi_statement, fake=fake, to_version=to_version
            )
