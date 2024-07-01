from pathlib import Path
from typing import List, Optional

from clickhouse_driver import Client

from clickhouse_migrations.defaults import DB_HOST, DB_PASSWORD, DB_PORT, DB_USER
from clickhouse_migrations.migration import Migration, MigrationStorage
from clickhouse_migrations.migrator import Migrator


class ClickhouseCluster:
    def __init__(
        self,
        db_host: str = DB_HOST,
        db_user: str = DB_USER,
        db_password: str = DB_PASSWORD,
        db_port: str = DB_PORT,
        db_url: Optional[str] = None,
        db_name: Optional[str] = None,
        **kwargs,
    ):
        self.db_url: Optional[str] = db_url
        self.default_db_name: Optional[str] = db_name

        if db_url:
            parts = self.db_url.split("/")
            if len(parts) == 4:
                self.default_db_name = parts[-1]
                parts = parts[0:-1]

            self.db_url = "/".join(parts)
        else:
            self.db_host = db_host
            self.db_port = db_port
            self.db_user = db_user
            self.db_password = db_password
            self.connection_kwargs = kwargs

    def connection(self, db_name: Optional[str] = None) -> Client:
        db_name = db_name if db_name is not None else self.default_db_name

        if self.db_url:
            db_url = self.db_url
            if db_name:
                db_url = db_url + "/" + db_name
            ch_client = Client.from_url(db_url)
        else:
            ch_client = Client(
                self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=db_name,
                **self.connection_kwargs,
            )
        return ch_client

    def create_db(
        self, db_name: Optional[str] = None, cluster_name: Optional[str] = None
    ):
        db_name = db_name if db_name is not None else self.default_db_name

        with self.connection("") as conn:
            if cluster_name is None:
                conn.execute(f'CREATE DATABASE IF NOT EXISTS "{db_name}"')
            else:
                conn.execute(
                    f'CREATE DATABASE IF NOT EXISTS "{db_name}" ON CLUSTER "{cluster_name}"'
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
        migration_path: Path,
        cluster_name: Optional[str] = None,
        create_db_if_no_exists: bool = True,
        multi_statement: bool = True,
        dryrun: bool = False,
    ):
        db_name = db_name if db_name is not None else self.default_db_name

        storage = MigrationStorage(migration_path)
        migrations = storage.migrations()

        return self.apply_migrations(
            db_name,
            migrations,
            cluster_name=cluster_name,
            create_db_if_no_exists=create_db_if_no_exists,
            multi_statement=multi_statement,
            dryrun=dryrun,
        )

    def apply_migrations(
        self,
        db_name: str,
        migrations: List[Migration],
        dryrun: bool = False,
        cluster_name: Optional[str] = None,
        create_db_if_no_exists: bool = True,
        multi_statement: bool = True,
    ) -> List[Migration]:
        if create_db_if_no_exists:
            if cluster_name is None:
                self.create_db(db_name)
            else:
                self.create_db(db_name, cluster_name)

        with self.connection(db_name) as conn:
            migrator = Migrator(conn, dryrun)
            migrator.init_schema(cluster_name)
            return migrator.apply_migration(migrations, multi_statement)
