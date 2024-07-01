import logging
from typing import Dict, List, Optional, Tuple

from clickhouse_driver import Client

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration


class Migrator:
    def __init__(self, conn: Client, dryrun: bool = False):
        self._conn: Client = conn
        self._dryrun = dryrun

    def init_schema(self, cluster_name: Optional[str] = None):
        cluster_schema = f"""CREATE TABLE IF NOT EXISTS schema_versions ON CLUSTER "{cluster_name}" (
    version UInt32,
    md5 String,
    script String,
    created_at DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{database}}/{{table}}', '{{replica}}')
ORDER BY tuple(created_at)"""

        single_schema = """CREATE TABLE IF NOT EXISTS schema_versions (
    version UInt32,
    md5 String,
    script String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY tuple(created_at)"""

        self._execute(single_schema if cluster_name is None else cluster_schema)

    def query_applied_migrations(self) -> List[Migration]:
        query = """SELECT
            version,
            script,
            md5
        FROM schema_versions"""

        result = self._execute(query, with_column_types=True)
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

        logging.info("Total migrations to apply: %d", len(new_migrations))

        if not new_migrations:
            return []

        for migration in new_migrations:
            logging.info("Execute migration %s", migration)

            statements = self.script_to_statements(migration.script, multi_statement)

            logging.info("Migration contains %s statements to apply", len(statements))
            for statement in statements:
                if not self._dryrun:
                    self._execute(statement)
                else:
                    logging.info("Dry run mode, would have executed: %s", statement)

            logging.info("Migration applied, need to update schema version table.")
            if not self._dryrun:
                self._execute(
                    "INSERT INTO schema_versions(version, script, md5) VALUES",
                    [
                        {
                            "version": migration.version,
                            "script": migration.script,
                            "md5": migration.md5,
                        }
                    ],
                )

            logging.info("Migration is fully applied.")

        return new_migrations

    def _execute(self, statement, *args, **kwargs):
        logging.debug(statement)
        return self._conn.execute(statement, *args, **kwargs)

    @classmethod
    def script_to_statements(cls, script: str, multi_statement: bool) -> List[str]:
        statements = []
        if multi_statement:
            for statement in script.split(";"):
                statement = statement.strip()
                if statement:
                    statements.append(statement + ";")
        else:
            statements.append(script.strip())

        return statements
