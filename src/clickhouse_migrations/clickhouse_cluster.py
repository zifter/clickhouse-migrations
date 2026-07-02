from pathlib import Path
from typing import List, Optional, Union
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from clickhouse_driver import Client

from clickhouse_migrations.defaults import DB_HOST, DB_PASSWORD, DB_PORT, DB_USER
from clickhouse_migrations.migration import Migration, MigrationStorage
from clickhouse_migrations.migrator import STATUS_PENDING, Migrator, StatusRow
from clickhouse_migrations.util import quote_identifier


class ClickhouseCluster:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        db_host: str = DB_HOST,
        db_user: str = DB_USER,
        db_password: str = DB_PASSWORD,
        db_port: str = DB_PORT,
        db_url: Optional[str] = None,
        db_name: Optional[str] = None,
        secure: bool = False,
        **kwargs,
    ):
        self.db_url: Optional[str] = None
        self.default_db_name: Optional[str] = db_name
        self.secure: bool = secure
        self.connection_kwargs = kwargs
        self._parsed_url = None

        if db_url:
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

    def connection(self, db_name: Optional[str] = None) -> Client:
        db_name = db_name if db_name is not None else self.default_db_name

        if self._parsed_url is not None:
            parsed = self._parsed_url
            if db_name:
                parsed = parsed._replace(path="/" + db_name)
            ch_client = Client.from_url(urlunparse(parsed))
        else:
            ch_client = Client(
                self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=db_name,
                secure=self.secure,
                **self.connection_kwargs,
            )
        return ch_client

    def create_db(
        self, db_name: Optional[str] = None, cluster_name: Optional[str] = None
    ):
        db_name = db_name if db_name is not None else self.default_db_name

        with self.connection("") as conn:
            if cluster_name is None:
                conn.execute(
                    f"CREATE DATABASE IF NOT EXISTS {quote_identifier(db_name)}"
                )
            else:
                conn.execute(
                    f"CREATE DATABASE IF NOT EXISTS {quote_identifier(db_name)} "
                    f"ON CLUSTER {quote_identifier(cluster_name)}"
                )

    def init_schema(
        self, db_name: Optional[str] = None, cluster_name: Optional[str] = None
    ):
        db_name = db_name if db_name is not None else self.default_db_name

        with self.connection(db_name) as conn:
            migrator = Migrator(conn)
            migrator.init_schema(cluster_name)

    def show_tables(self, db_name):
        db_name = db_name if db_name is not None else self.default_db_name

        with self.connection(db_name) as conn:
            result = conn.execute("show tables")
            return [t[0] for t in result]

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
    ):
        db_name = db_name if db_name is not None else self.default_db_name

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
        with self.connection("") as conn:
            initialized = conn.execute(
                "SELECT count() FROM system.tables "
                "WHERE database = %(db)s AND name = 'schema_versions'",
                {"db": db_name},
            )[0][0]

        if not initialized:
            return [StatusRow(m.version, STATUS_PENDING, m.md5, None) for m in incoming]

        with self.connection(db_name) as conn:
            return Migrator(conn).migration_status(incoming)

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
    ) -> List[Migration]:
        if create_db_if_no_exists:
            if cluster_name is None:
                self.create_db(db_name)
            else:
                self.create_db(db_name, cluster_name)

        with self.connection(db_name) as conn:
            migrator = Migrator(conn, dryrun, migration_log_format=migration_log_format)
            migrator.init_schema(cluster_name)
            return migrator.apply_migration(migrations, multi_statement, fake=fake)
