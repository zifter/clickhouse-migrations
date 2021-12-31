import logging
from pathlib import Path

import pandas as pd
from clickhouse_driver import Client

from .defaults import DB_HOST, DB_PASSWORD, DB_USER
from .migrate import apply_migration, migrations_to_apply
from .types import MigrationStorage


class Migrator:
    def __init__(
        self,
        db_host: str = DB_HOST,
        db_user: str = DB_USER,
        db_password: str = DB_PASSWORD,
    ):
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password

    def connection(self, db_name: str) -> Client:
        return Client(
            self.db_host, user=self.db_user, password=self.db_password, database=db_name
        )

    def create_db(self, db_name):
        with self.connection("") as conn:
            conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    @classmethod
    def init_schema(cls, conn):
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_versions ("
            "version UInt32, "
            "md5 String, "
            "script String, "
            "created_at DateTime DEFAULT now()"
            ") ENGINE = MergeTree ORDER BY tuple(created_at)"
        )

    def migrate(
        self,
        db_name: str,
        migration_path: Path,
        create_db_if_no_exists: bool = True,
    ):
        if create_db_if_no_exists:
            self.create_db(db_name)

        storage = MigrationStorage(migration_path)
        migrations = storage.migrations()
        logging.info("Total migrations: %d", len(migrations))

        with self.connection(db_name) as conn:
            self.init_schema(conn)

            apply_migration(conn, migrations_to_apply(conn, pd.DataFrame(migrations)))
