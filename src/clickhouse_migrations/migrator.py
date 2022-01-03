import logging
from typing import List

import pandas
from clickhouse_driver import Client

from clickhouse_migrations.types import Migration


class Migrator:
    def __init__(self, conn: Client):
        self._conn: Client = conn

    def init_schema(self):
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_versions ("
            "version UInt32, "
            "md5 String, "
            "script String, "
            "created_at DateTime DEFAULT now()"
            ") ENGINE = MergeTree ORDER BY tuple(created_at)"
        )

    def execute_and_inflate(self, query) -> pandas.DataFrame:
        result = self._conn.execute(query, with_column_types=True)
        column_names = [c[0] for c in result[len(result) - 1]]
        return pandas.DataFrame([dict(zip(column_names, d)) for d in result[0]])

    def migrations_to_apply(self, migrations: List[Migration]) -> List[Migration]:
        applied_migrations = self.execute_and_inflate(
            "SELECT version AS version, script AS c_script, md5 as c_md5 from schema_versions",
        )

        if applied_migrations.empty:
            return migrations

        incoming = pandas.DataFrame(migrations)
        if len(incoming) == 0 or len(incoming) < len(applied_migrations):
            raise AssertionError(
                "Migrations have gone missing, "
                "your code base should not truncate migrations, "
                "use migrations to correct older migrations"
            )

        applied_migrations = applied_migrations.astype({"version": "int32"})
        incoming = incoming.astype({"version": "int32"})
        exec_stat = pandas.merge(
            applied_migrations, incoming, on="version", how="outer"
        )
        committed_and_absconded = exec_stat[
            exec_stat.c_md5.notnull() & exec_stat.md5.isnull()
        ]
        if len(committed_and_absconded) > 0:
            raise AssertionError(
                "Migrations have gone missing, "
                "your code base should not truncate migrations, "
                "use migrations to correct older migrations"
            )

        index = (
            exec_stat.c_md5.notnull()
            & exec_stat.md5.notnull()
            & ~(exec_stat.md5 == exec_stat.c_md5)
        )
        terms_violated = exec_stat[index]
        if len(terms_violated) > 0:
            raise AssertionError(
                "Do not edit migrations once run, "
                "use migrations to correct older migrations"
            )
        versions_to_apply = exec_stat[exec_stat.c_md5.isnull()][["version"]]
        return [m for m in migrations if m.version in versions_to_apply.values]

    def apply_migration(self, migrations: List[Migration]) -> List[Migration]:
        new_migrations = self.migrations_to_apply(migrations)
        if not new_migrations:
            return []

        for migration in new_migrations:
            logging.info("Execute migration %s", migration)
            self._conn.execute(migration.script)

            logging.info("Migration applied")

            self._conn.execute(
                "INSERT INTO schema_versions(version, script, md5) VALUES",
                [
                    {
                        "version": migration.version,
                        "script": migration.script,
                        "md5": migration.md5,
                    }
                ],
            )

        return new_migrations
