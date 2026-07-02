import logging
import re
from collections import namedtuple
from typing import Dict, List, Optional, Tuple

from clickhouse_migrations.connection import Connection
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration
from clickhouse_migrations.util import quote_identifier

MIGRATION_LOG_FORMAT_FULL = "full"
MIGRATION_LOG_FORMAT_COMPACT = "compact"
MIGRATION_LOG_FORMATS = (MIGRATION_LOG_FORMAT_FULL, MIGRATION_LOG_FORMAT_COMPACT)

STATUS_APPLIED = "applied"
STATUS_PENDING = "pending"
STATUS_MD5_MISMATCH = "md5-mismatch"
STATUS_UNKNOWN = "unknown"

# One row of a migration status report. state is one of the STATUS_* values;
# applied_at is None for migrations that have not been applied yet.
StatusRow = namedtuple("StatusRow", ["version", "state", "md5", "applied_at"])

# Tokenizer used to split a script into statements without treating a ";" that
# lives inside a string literal, quoted identifier or comment as a delimiter.
_STATEMENT_TOKEN_RE = re.compile(
    r"""
      (?P<line_comment>--[^\n]*)
    | (?P<block_comment>/\*.*?\*/)
    | (?P<single>'(?:\\.|''|[^'])*')
    | (?P<double>"(?:\\.|""|[^"])*")
    | (?P<backtick>`(?:``|[^`])*`)
    | (?P<semicolon>;)
    | (?P<other>[^-/'"`;]+|.)
    """,
    re.VERBOSE | re.DOTALL,
)


class Migrator:
    def __init__(
        self,
        conn: Connection,
        dryrun: bool = False,
        migration_log_format: str = MIGRATION_LOG_FORMAT_FULL,
    ):
        if migration_log_format not in MIGRATION_LOG_FORMATS:
            raise ValueError(
                f"Unknown migration log format: {migration_log_format}. "
                f"Expected one of: {', '.join(MIGRATION_LOG_FORMATS)}"
            )

        self._conn: Connection = conn
        self._dryrun = dryrun
        self._migration_log_format = migration_log_format

    def init_schema(self, cluster_name: Optional[str] = None):
        if cluster_name is None:
            schema = """CREATE TABLE IF NOT EXISTS schema_versions (
    version UInt32,
    md5 String,
    script String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY tuple(created_at)"""
        else:
            schema = f"""CREATE TABLE IF NOT EXISTS schema_versions ON CLUSTER {quote_identifier(cluster_name)} (
    version UInt32,
    md5 String,
    script String,
    created_at DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{database}}/{{table}}', '{{replica}}')
ORDER BY tuple(created_at)"""

        self._conn.command(schema)

    def query_applied_migrations(self) -> List[Migration]:
        self.optimize_schema_table()

        query = """SELECT DISTINCT
            version,
            script,
            md5
        FROM schema_versions
        ORDER BY version"""

        return [Migration(**row) for row in self._conn.query(query)]

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

    def migration_status(self, incoming: List[Migration]) -> List[StatusRow]:
        return self._build_status(incoming, self._query_applied_meta())

    def _query_applied_meta(self) -> Dict[int, Tuple[str, object]]:
        rows = self._conn.query(
            "SELECT version, argMax(md5, created_at) AS md5, "
            "max(created_at) AS applied_at "
            "FROM schema_versions GROUP BY version ORDER BY version"
        )
        return {row["version"]: (row["md5"], row["applied_at"]) for row in rows}

    @staticmethod
    def _build_status(
        incoming: List[Migration], applied: Dict[int, Tuple[str, object]]
    ) -> List[StatusRow]:
        incoming_by_version = {m.version: m for m in incoming}

        rows: List[StatusRow] = []
        for version in sorted(set(incoming_by_version) | set(applied)):
            local = incoming_by_version.get(version)
            applied_meta = applied.get(version)

            if local and applied_meta:
                applied_md5, applied_at = applied_meta
                state = (
                    STATUS_APPLIED if local.md5 == applied_md5 else STATUS_MD5_MISMATCH
                )
                rows.append(StatusRow(version, state, applied_md5, applied_at))
            elif local:
                rows.append(StatusRow(version, STATUS_PENDING, local.md5, None))
            else:
                applied_md5, applied_at = applied_meta
                rows.append(StatusRow(version, STATUS_UNKNOWN, applied_md5, applied_at))

        return rows

    def format_migration_log(self, migration: Migration) -> str:
        if self._migration_log_format == MIGRATION_LOG_FORMAT_COMPACT:
            return f"version={migration.version}, md5={migration.md5}"

        return str(migration)

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
            logging.info("Execute migration %s", self.format_migration_log(migration))

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
                    self._conn.command(statement)

            logging.info("Migration applied, need to update schema version table.")
            if fake:
                logging.debug("update schema versions because fake option is enabled")
                self._conn.command(
                    "ALTER TABLE schema_versions "
                    f"DELETE WHERE version = {int(migration.version)}"
                )
                self._insert_schema_version(migration)
            elif self._dryrun:
                logging.debug(
                    "Skip updating schema versions because dry run is enabled"
                )
            else:
                logging.debug("Insert new schemas")
                self._insert_schema_version(migration)

            logging.info("Migration is fully applied.")

        return migrations_to_process

    def _insert_schema_version(self, migration: Migration) -> None:
        self._conn.insert(
            "schema_versions",
            [
                {
                    "version": migration.version,
                    "script": migration.script,
                    "md5": migration.md5,
                }
            ],
        )

    def optimize_schema_table(self):
        self._conn.command("OPTIMIZE TABLE schema_versions FINAL")

    @classmethod
    def script_to_statements(cls, script: str, multi_statement: bool) -> List[str]:
        if not multi_statement:
            return [script.strip()]

        statements: List[str] = []
        current: List[str] = []
        for match in _STATEMENT_TOKEN_RE.finditer(script):
            if match.lastgroup == "semicolon":
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement + ";")
                current = []
            else:
                current.append(match.group())

        statement = "".join(current).strip()
        if statement:
            statements.append(statement + ";")

        return statements
