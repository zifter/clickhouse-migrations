from pathlib import Path
from typing import List, Optional

from clickhouse_driver import Client

from clickhouse_migrations.defaults import DB_HOST, DB_PASSWORD, DB_PORT, DB_USER
from clickhouse_migrations.migrator import Migrator
from clickhouse_migrations.types import Migration, MigrationStorage


class ClickhouseCluster:
    def __init__(
        self,
        db_host: str = DB_HOST,
        db_user: str = DB_USER,
        db_password: str = DB_PASSWORD,
        db_port: str = DB_PORT,
        **kwargs,
    ):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.connection_kwargs = kwargs

    def connection(self, db_name: str) -> Client:
        return Client(
            self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database=db_name,
            **self.connection_kwargs,
        )

    def create_db(self, db_name, cluster_name=None):
        with self.connection("") as conn:
            if cluster_name is None:
                conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            else:
                conn.execute(
                    f"CREATE DATABASE IF NOT EXISTS {db_name} ON CLUSTER {cluster_name}"
                )

    def init_schema(self, db_name, cluster_name=None):
        with self.connection(db_name) as conn:
            migrator = Migrator(conn)
            migrator.init_schema(cluster_name)

    def show_tables(self, db_name):
        with self.connection(db_name) as conn:
            result = conn.execute("show tables")
            return [t[0] for t in result]

    def migrate(
        self,
        db_name: str,
        migration_path: Path,
        cluster_name: Optional[str] = None,
        create_db_if_no_exists: bool = True,
        multi_statement: bool = True,
    ):
        storage = MigrationStorage(migration_path)
        migrations = storage.migrations()

        return self.apply_migrations(
            db_name,
            migrations,
            cluster_name=cluster_name,
            create_db_if_no_exists=create_db_if_no_exists,
            multi_statement=multi_statement,
        )

    def apply_migrations(
        self,
        db_name: str,
        migrations: List[Migration],
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
            migrator = Migrator(conn)
            migrator.init_schema(cluster_name)
            return migrator.apply_migration(migrations, multi_statement)
