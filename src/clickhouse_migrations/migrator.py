import logging
from typing import Dict, List, Tuple

from clickhouse_driver import Client

from clickhouse_migrations.exceptions import MigrationException
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

    def query_applied_migrations(self) -> List[Migration]:
        query = """SELECT
            version,
            script,
            md5
        FROM schema_versions"""

        result = self._conn.execute(query, with_column_types=True)
        column_names = [c[0] for c in result[len(result) - 1]]

        migrations_as_dict = [dict(zip(column_names, d)) for d in result[0]]

        return [Migration(**d) for d in migrations_as_dict]

    def migrations_to_apply(self, incoming: List[Migration]) -> List[Migration]:
        applied = self.query_applied_migrations()

        if not applied:
            return incoming

        if len(incoming) == 0 or len(incoming) < len(applied):
            raise MigrationException(
                "Migrations have gone missing, "
                "your code base should not truncate migrations, "
                "use migrations to correct older migrations"
            )

        # create outer join
        joined_migrations: Dict[Tuple[Migration, Migration]] = {
            m.version: [m, None] for m in incoming
        }
        for m in applied:
            if m.version in joined_migrations:
                joined_migrations[m.version][1] = m
            else:
                joined_migrations[m.version] = [None, m]

        # md5 of applied function must be equal
        for version, p in joined_migrations.items():
            left, right = p
            if left and right and left.md5 != right.md5:
                raise MigrationException(
                    "Migrations md5 is not equal, " f"Migration version is {version}."
                )

        # all migrations should be known
        for version, p in joined_migrations.items():
            left, right = p
            if not left and right:
                raise MigrationException(
                    "There is applied migrations, which is not known by current migrations list. "
                    f"Migration version is {version}."
                )

        to_apply = [
            left for left, right in joined_migrations.values() if left and not right
        ]
        return sorted(to_apply, key=lambda x: x.version)

    def apply_migration(
        self, migrations: List[Migration], multi_statement: bool
    ) -> List[Migration]:
        new_migrations = self.migrations_to_apply(migrations)
        if not new_migrations:
            return []

        for migration in new_migrations:
            logging.info("Execute migration %s", migration)

            statements = self.script_to_statements(migration.script, multi_statement)
            for statement in statements:
                statement = statement.strip()
                self._conn.execute(statement)

            logging.info("Migration applied. Update schema version table.")

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

    @classmethod
    def script_to_statements(cls, script: str, multi_statement: bool) -> List[str]:
        script = script.strip()

        if multi_statement:
            return [m.strip() for m in script.split(";") if m]

        return [script]
