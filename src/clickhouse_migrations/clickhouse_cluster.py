import logging
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional, Set, Union
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
    normalize_db_url,
)
from clickhouse_migrations.defaults import (
    DB_HOST,
    DB_PASSWORD,
    DB_USER,
    LOCK,
    LOCK_TIMEOUT,
    LOCK_TTL,
    MIGRATIONS_TABLE,
    MIGRATIONS_TABLE_ENGINE,
)
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.lock import KeeperMapLock, LockHolder
from clickhouse_migrations.migration import Migration, MigrationStorage
from clickhouse_migrations.migrator import STATUS_PENDING, Migrator, StatusRow
from clickhouse_migrations.schema_dump import (
    INNER_TABLE_PREFIXES,
    LOCK_TABLE,
    normalize_statement,
    render_dump,
    server_dependencies,
    sort_by_dependency,
)
from clickhouse_migrations.util import (
    format_table_reference,
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
            parsed = urlparse(normalize_db_url(db_url, driver, secure))
            path_db = parsed.path.lstrip("/")
            if path_db:
                self.default_db_name = path_db

            query = dict(parse_qsl(parsed.query))
            if secure and driver == CLICKHOUSE_DRIVER:
                query.setdefault("secure", "true")

            # Keep the base URL without a database in the path; connection()
            # re-adds the database as a proper path segment so it never lands
            # after the query string.
            self._parsed_url = parsed._replace(path="", query=urlencode(query))
            self.db_url = urlunparse(self._parsed_url)
            if driver == CLICKHOUSE_CONNECT:
                # Never log the URL itself: it may carry the password.
                logging.info(
                    "clickhouse-connect will use %s://%s:%s (HTTP interface)",
                    parsed.scheme,
                    parsed.hostname,
                    parsed.port or (8443 if parsed.scheme == "https" else 8123),
                )
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
            if self._parsed_url is not None:
                # The scheme is passed as ``interface`` because get_client
                # ignores the DSN scheme; user, password, host, port and
                # query settings come from the DSN itself.
                client = clickhouse_connect.get_client(
                    dsn=self.db_url,
                    interface=self._parsed_url.scheme,
                    database=db_name or None,
                )
                return ClickhouseConnectConnection(client)
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

    def _excluded_from_dump(self, db_name: str) -> Set[str]:
        """Bookkeeping tables of this tool that a schema dump must not contain."""
        table_db, table_name = split_table_reference(self.migrations_table)
        if table_db not in (None, db_name):
            return {LOCK_TABLE}
        return {table_name, LOCK_TABLE, f"{table_name}_lock"}

    def _dump_candidates(
        self, known: Set[str], db_name: str, include_migrations_table: bool
    ) -> Set[str]:
        excluded = (
            set() if include_migrations_table else self._excluded_from_dump(db_name)
        )
        return {
            name
            for name in known
            if not name.startswith(INNER_TABLE_PREFIXES) and name not in excluded
        }

    def dump(
        self,
        db_name: Optional[str] = None,
        tables: Optional[List[str]] = None,
        keep_replicated_paths: bool = False,
        include_migrations_table: bool = False,
    ) -> str:
        """Return the definition of the database objects as portable SQL.

        Tables, views, materialized views and dictionaries, ordered so that
        every object comes after the ones it reads from, one ``;`` terminated
        statement each. Strictly read-only: nothing is created. ``tables``
        limits the dump to those objects (a warning is logged for a listed
        object that depends on an unlisted one). See ``schema_dump`` for the
        normalisation rules.
        """
        db_name = db_name if db_name is not None else self.default_db_name
        if not db_name:
            raise MigrationException("A database name is required to dump the schema.")

        with self.connection("") as conn:
            if not conn.query(
                f"SELECT 1 AS n FROM system.databases WHERE name = {quote_string(db_name)}"
            ):
                raise MigrationException(f"Database {db_name!r} does not exist.")

            rows = conn.query(
                "SELECT * FROM system.tables "
                f"WHERE database = {quote_string(db_name)} AND NOT is_temporary "
                "ORDER BY name"
            )
            known = {row["name"] for row in rows}
            names = self._dump_candidates(known, db_name, include_migrations_table)
            dependencies = server_dependencies(rows, db_name, names)
            selected = self._select_dump_objects(names, tables, db_name)
            statements = {}
            for name in selected:
                normalized = normalize_statement(
                    self._show_create(conn, db_name, name),
                    db_name,
                    known,
                    keep_replicated_paths,
                )
                statements[name] = normalized.text
                dependencies[name] |= normalized.references

        self._warn_about_unlisted(selected, dependencies, names)
        order = sort_by_dependency(
            {name: dependencies[name] - {name} for name in selected}
        )
        return render_dump([statements[name] for name in order])

    @staticmethod
    def _show_create(conn: Connection, db_name: str, name: str) -> str:
        # Tables, views and dictionaries alike; the single column is named
        # "statement", but take the first one to not depend on it.
        rows = conn.query(f"SHOW CREATE TABLE {format_table_reference(db_name, name)}")
        return str(next(iter(rows[0].values())))

    @staticmethod
    def _warn_about_unlisted(selected, dependencies, names) -> None:
        for name in selected:
            missing = sorted((dependencies[name] & names) - set(selected))
            if missing:
                logging.warning(
                    "%s depends on %s, which is not part of the dump",
                    name,
                    ", ".join(missing),
                )

    @staticmethod
    def _select_dump_objects(
        names: Set[str], tables: Optional[List[str]], db_name: str
    ) -> List[str]:
        if not tables:
            return sorted(names)

        unknown = sorted(set(tables) - names)
        if unknown:
            raise MigrationException(
                f"Not found in database {db_name!r} (or excluded from the dump): "
                + ", ".join(unknown)
            )
        return sorted(set(tables))

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
        lock: bool = LOCK,
        lock_timeout: int = LOCK_TIMEOUT,
        lock_ttl: int = LOCK_TTL,
    ):
        db_name = db_name if db_name is not None else self.default_db_name

        if to_version is not None and explicit_migrations:
            raise MigrationException(
                "to_version and explicit_migrations are mutually exclusive."
            )

        migrations = MigrationStorage(migration_path).migrations(explicit_migrations)

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
            lock=lock,
            lock_timeout=lock_timeout,
            lock_ttl=lock_ttl,
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
        down_versions = set(storage.down_scripts())

        # Read-only: never create the database or the schema table. If the
        # schema table is missing, nothing has been applied yet.
        if not self._is_initialized(db_name):
            return [
                StatusRow(
                    m.version,
                    STATUS_PENDING,
                    m.md5,
                    None,
                    m.version in down_versions,
                )
                for m in incoming
            ]

        with self.connection(db_name) as conn:
            return self._migrator(conn).migration_status(incoming, down_versions)

    def rollback(
        self,
        db_name: Optional[str],
        migration_path: Union[Path, str],
        steps: int = 1,
        to_version: Optional[int] = None,
        dryrun: bool = False,
        multi_statement: bool = True,
        lock: bool = LOCK,
        lock_timeout: int = LOCK_TIMEOUT,
        lock_ttl: int = LOCK_TTL,
    ) -> List[int]:
        db_name = db_name if db_name is not None else self.default_db_name

        down_scripts = MigrationStorage(migration_path).down_scripts()

        # Read-only pre-check: if the schema table is missing, nothing has been
        # applied yet, so there is nothing to roll back.
        if not self._is_initialized(db_name):
            return []

        # A dry run changes nothing, so it never blocks a real run.
        with self.migration_lock(
            db_name,
            lock=lock,
            lock_timeout=lock_timeout,
            lock_ttl=lock_ttl,
            enabled=not dryrun,
        ):
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
        lock: bool = LOCK,
        lock_timeout: int = LOCK_TIMEOUT,
        lock_ttl: int = LOCK_TTL,
    ) -> List[Migration]:
        if create_db_if_no_exists:
            if cluster_name is None:
                self.create_db(db_name)
            else:
                self.create_db(db_name, cluster_name)

        # A dry run executes nothing, so it never blocks a real run.
        with self.migration_lock(
            db_name,
            lock=lock,
            lock_timeout=lock_timeout,
            lock_ttl=lock_ttl,
            enabled=not dryrun,
        ):
            with self.connection(db_name) as conn:
                migrator = self._migrator(
                    conn, dryrun, migration_log_format=migration_log_format
                )
                migrator.init_schema(cluster_name)
                return migrator.apply_migration(
                    migrations, multi_statement, fake=fake, to_version=to_version
                )

    def _lock(
        self,
        conn: Connection,
        db_name: Optional[str],
        lock_timeout: int = LOCK_TIMEOUT,
        lock_ttl: int = LOCK_TTL,
    ) -> KeeperMapLock:
        return KeeperMapLock(
            conn,
            db_name,
            migrations_table=self.migrations_table,
            timeout=lock_timeout,
            ttl=lock_ttl,
        )

    @contextmanager
    def migration_lock(
        self,
        db_name: Optional[str] = None,
        lock: bool = LOCK,
        lock_timeout: int = LOCK_TIMEOUT,
        lock_ttl: int = LOCK_TTL,
        enabled: bool = True,
    ):
        """Hold the migration lock of ``db_name`` for the duration of the block.

        Locking is opt-in: without ``lock`` nothing lock-related touches the
        database at all, and the run behaves exactly as it did before the lock
        existed. With it, the lock uses its own connection so releasing it does
        not depend on the state of the connection that ran the migrations.
        """
        db_name = db_name if db_name is not None else self.default_db_name

        if not enabled or not lock:
            yield None
            return

        with self.connection(db_name) as lock_conn:
            migration_lock = self._lock(lock_conn, db_name, lock_timeout, lock_ttl)
            migration_lock.acquire()
            try:
                yield migration_lock
            finally:
                # Also covers a failing migration and KeyboardInterrupt: a lock
                # we never took is never released, and only our own row goes.
                migration_lock.release()

    def force_unlock(self, db_name: Optional[str] = None) -> Optional[LockHolder]:
        """Force-release the migration lock of a database ("unlock")."""
        db_name = db_name if db_name is not None else self.default_db_name

        with self.connection(db_name) as conn:
            return self._lock(conn, db_name).force_release()
