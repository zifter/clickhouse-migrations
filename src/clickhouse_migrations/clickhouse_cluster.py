import logging
from pathlib import Path
from typing import List

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

    def create_db(self, db_name):
        with self.connection("") as conn:
            conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    def init_schema(self, db_name):
        with self.connection(db_name) as conn:
            migrator = Migrator(conn)
            migrator.init_schema()

    def show_tables(self, db_name):
        with self.connection(db_name) as conn:
            result = conn.execute("show tables")
            return [t[0] for t in result]

    def migrate(
        self,
        db_name: str,
        migration_path: Path,
        create_db_if_no_exists: bool = True,
        multi_statement: bool = True,
    ):
        storage = MigrationStorage(migration_path)
        migrations = storage.migrations()

        return self.apply_migrations(
            db_name,
            migrations,
            create_db_if_no_exists=create_db_if_no_exists,
            multi_statement=multi_statement,
        )

    def apply_migrations(
        self,
        db_name: str,
        migrations: List[Migration],
        create_db_if_no_exists: bool = True,
        multi_statement: bool = True,
    ) -> List[Migration]:

        if create_db_if_no_exists:
            self.create_db(db_name)

        logging.info("Total migrations to apply: %d", len(migrations))

        with self.connection(db_name) as conn:
            migrator = Migrator(conn)
            migrator.init_schema()
            return migrator.apply_migration(migrations, multi_statement)
