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
        self.optimize_schema_table()

        query = """SELECT DISTINCT
            version,
            script,
            md5
        FROM schema_versions
        ORDER BY version"""

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
        self,
        migrations: List[Migration],
        multi_statement: bool,
        fake: bool = False,
    ) -> List[Migration]:
        migrations_to_process = (
            migrations if fake else self.migrations_to_apply(migrations)
        )

        logging.info("Total migrations to apply: %d", len(migrations_to_process))

        if not migrations_to_process:
            return []

        for migration in migrations_to_process:
            logging.info("Execute migration %s", migration)

            statements = self.script_to_statements(migration.script, multi_statement)

            logging.info("Migration contains %s statements to apply", len(statements))
            for statement in statements:
                if fake:
                    logging.warning(
                        "Fake mode, statement will be skipped: %s", statement
                    )
                elif self._dryrun:
                    logging.info("Dry run mode, would have executed: %s", statement)
                else:
                    self._execute(statement)

            logging.info("Migration applied, need to update schema version table.")
            if fake:
                logging.debug("update schema versions because fake option is enabled")
                self._execute(
                    "ALTER TABLE schema_versions DELETE WHERE version = %(version)s;",
                    {
                        "version": migration.version,
                    },
                )
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
            elif self._dryrun:
                logging.debug(
                    "Skip updating schema versions because dry run is enabled"
                )
            else:
                logging.debug("Insert new schemas")
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

        return migrations_to_process

    def optimize_schema_table(self):
        self._execute("OPTIMIZE TABLE schema_versions FINAL;")

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
