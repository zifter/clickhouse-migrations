import logging
import re
from collections import namedtuple
from typing import Dict, List, Mapping, Optional, Set, Tuple

from clickhouse_migrations.connection import Connection
from clickhouse_migrations.defaults import MIGRATIONS_TABLE, MIGRATIONS_TABLE_ENGINE
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration
from clickhouse_migrations.substitution import substitute
from clickhouse_migrations.util import (
    format_table_reference,
    quote_identifier,
    quote_string,
    split_table_reference,
)

MIGRATION_LOG_FORMAT_FULL = "full"
MIGRATION_LOG_FORMAT_COMPACT = "compact"
MIGRATION_LOG_FORMATS = (MIGRATION_LOG_FORMAT_FULL, MIGRATION_LOG_FORMAT_COMPACT)

# Engines used for the bookkeeping table when the user does not pass one.
DEFAULT_TABLE_ENGINE = "MergeTree"
# NB: {database}/{table}/{replica} are ClickHouse macros, not Python
# placeholders - this literal is passed to the server as is.
DEFAULT_REPLICATED_TABLE_ENGINE = (
    "ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}')"
)

STATUS_APPLIED = "applied"
STATUS_PENDING = "pending"
STATUS_MD5_MISMATCH = "md5-mismatch"
STATUS_UNKNOWN = "unknown"
# Only reported by repair: an unknown row that was deleted by --prune.
STATUS_PRUNED = "pruned"
# States repair works on: the bookkeeping disagrees with the local files.
OUT_OF_SYNC_STATES = (STATUS_MD5_MISMATCH, STATUS_UNKNOWN)

# One row of a migration status report. state is one of the STATUS_* values;
# applied_at is None for migrations that have not been applied yet.
# has_down tells whether a local {VERSION}_{name}.down.sql file exists.
StatusRow = namedtuple(
    "StatusRow",
    ["version", "state", "md5", "applied_at", "has_down"],
    defaults=(False,),
)

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
        migrations_table: str = MIGRATIONS_TABLE,
        migrations_table_engine: Optional[str] = MIGRATIONS_TABLE_ENGINE,
    ):
        if migration_log_format not in MIGRATION_LOG_FORMATS:
            raise ValueError(
                f"Unknown migration log format: {migration_log_format}. "
                f"Expected one of: {', '.join(MIGRATION_LOG_FORMATS)}"
            )

        self._conn: Connection = conn
        self._dryrun = dryrun
        self._migration_log_format = migration_log_format
        self.migrations_table_database, self.migrations_table_name = (
            split_table_reference(migrations_table)
        )
        self._table = format_table_reference(
            self.migrations_table_database, self.migrations_table_name
        )
        self._migrations_table_engine = migrations_table_engine

    def init_schema(self, cluster_name: Optional[str] = None):
        if cluster_name is None:
            on_cluster = ""
            engine = self._migrations_table_engine or DEFAULT_TABLE_ENGINE
        else:
            on_cluster = f" ON CLUSTER {quote_identifier(cluster_name)}"
            engine = self._migrations_table_engine or DEFAULT_REPLICATED_TABLE_ENGINE

        schema = f"""CREATE TABLE IF NOT EXISTS {self._table}{on_cluster} (
    version UInt32,
    md5 String,
    script String,
    created_at DateTime DEFAULT now()
) ENGINE = {engine}
ORDER BY tuple(created_at)"""

        if self.migrations_table_database is None:
            self._conn.command(schema)
            return

        # The bookkeeping table was pointed at another database, which we never
        # create implicitly. Make the failure self-explanatory instead of
        # surfacing a bare driver error.
        try:
            self._conn.command(schema)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            raise MigrationException(
                f"Failed to create the migrations table {self._table}: {exc}. "
                f"The database {quote_identifier(self.migrations_table_database)} "
                "is not created automatically - create it first."
            ) from exc

    def query_applied_migrations(self) -> List[Migration]:
        self.optimize_schema_table()

        query = f"""SELECT DISTINCT
            version,
            script,
            md5
        FROM {self._table}
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

    def migration_status(
        self, incoming: List[Migration], down_versions: Optional[Set[int]] = None
    ) -> List[StatusRow]:
        return self._build_status(
            incoming, self._query_applied_meta(), down_versions or set()
        )

    def _query_applied_meta(self) -> Dict[int, Tuple[str, object]]:
        rows = self._conn.query(
            "SELECT version, argMax(md5, created_at) AS md5, "
            "max(created_at) AS applied_at "
            f"FROM {self._table} GROUP BY version ORDER BY version"
        )
        return {row["version"]: (row["md5"], row["applied_at"]) for row in rows}

    @staticmethod
    def _build_status(
        incoming: List[Migration],
        applied: Dict[int, Tuple[str, object]],
        down_versions: Optional[Set[int]] = None,
    ) -> List[StatusRow]:
        down_versions = down_versions or set()
        incoming_by_version = {m.version: m for m in incoming}

        rows: List[StatusRow] = []
        for version in sorted(set(incoming_by_version) | set(applied)):
            local = incoming_by_version.get(version)
            has_down = local is not None and version in down_versions
            applied_meta = applied.get(version)

            if local and applied_meta:
                applied_md5, applied_at = applied_meta
                state = (
                    STATUS_APPLIED if local.md5 == applied_md5 else STATUS_MD5_MISMATCH
                )
                rows.append(
                    StatusRow(version, state, applied_md5, applied_at, has_down)
                )
            elif local:
                rows.append(
                    StatusRow(version, STATUS_PENDING, local.md5, None, has_down)
                )
            else:
                applied_md5, applied_at = applied_meta
                rows.append(
                    StatusRow(version, STATUS_UNKNOWN, applied_md5, applied_at, False)
                )

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
        to_version: Optional[int] = None,
        variables: Optional[Mapping[str, str]] = None,
        sources: Optional[Mapping[int, str]] = None,
    ) -> List[Migration]:
        """Apply the pending ``migrations``.

        ``variables`` enables ``${NAME}`` substitution (None: the scripts run
        byte for byte). ``sources`` maps a version to its file name for error
        messages. The md5 and the stored script are always the raw text.
        """
        if to_version is not None and to_version not in {m.version for m in migrations}:
            raise MigrationException(
                f"Target version {to_version} is not among the local migrations."
            )

        # The md5 / missing / gap checks in migrations_to_apply always see the
        # full local set; the target only trims what is executed afterwards.
        migrations_to_process = (
            migrations if fake else self.migrations_to_apply(migrations)
        )

        if to_version is not None:
            if not fake:
                self._check_target_not_below_applied(to_version)
            migrations_to_process = [
                m for m in migrations_to_process if m.version <= to_version
            ]

        logging.info("Total migrations to apply: %d", len(migrations_to_process))

        if not migrations_to_process:
            return []

        # Substitute every script up front, so an unset variable in a later
        # migration fails the run before the first statement executes. A fake
        # run executes nothing, so it needs no variables.
        rendered = self._render_scripts(
            {m.version: m.script for m in migrations_to_process},
            None if fake else variables,
            sources,
            "migration",
        )

        for migration in migrations_to_process:
            logging.info("Execute migration %s", self.format_migration_log(migration))

            statements = self._statements(
                migration.script, rendered[migration.version], multi_statement
            )

            logging.info("Migration contains %s statements to apply", len(statements))
            for statement, shown in statements:
                if fake:
                    logging.warning("Fake mode, statement will be skipped: %s", shown)
                elif self._dryrun:
                    logging.info("Dry run mode, would have executed: %s", shown)
                else:
                    self._command(statement, shown)

            logging.info("Migration applied, need to update schema version table.")
            if fake:
                logging.debug("update schema versions because fake option is enabled")
                self._conn.command(
                    f"ALTER TABLE {self._table} "
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

    @staticmethod
    def _render_scripts(
        scripts: Dict[int, str],
        variables: Optional[Mapping[str, str]],
        sources: Optional[Mapping[int, str]],
        kind: str,
    ) -> Dict[int, Optional[str]]:
        """Substituted script per version; None when substitution is off."""
        if variables is None:
            return {version: None for version in scripts}

        sources = sources or {}
        return {
            version: substitute(
                script, variables, sources.get(version, f"{kind} version {version}")
            )
            for version, script in scripts.items()
        }

    def _statements(
        self, raw: str, rendered: Optional[str], multi_statement: bool
    ) -> List[Tuple[str, str]]:
        """Pairs of (statement to execute, statement safe to log).

        Substituted values may be secrets, so our own logs only ever show the
        raw text with its placeholders. When a value changed how the script
        splits into statements, the raw statements no longer line up and only
        a description is logged.
        """
        raw_statements = self.script_to_statements(raw, multi_statement)
        if rendered is None:
            return list(zip(raw_statements, raw_statements))

        statements = self.script_to_statements(rendered, multi_statement)
        if len(statements) == len(raw_statements):
            return list(zip(statements, raw_statements))

        return [
            (
                statement,
                f"<statement {index} of {len(statements)} after variable "
                "substitution, not logged>",
            )
            for index, statement in enumerate(statements, start=1)
        ]

    def _command(self, statement: str, shown: str) -> None:
        if statement == shown:
            self._conn.command(statement)
        else:
            # Keep substituted values out of the connection's debug log.
            self._conn.command(statement, log_statement=shown)

    def _check_target_not_below_applied(self, to_version: int) -> None:
        applied = [m.version for m in self.query_applied_migrations()]
        if applied and max(applied) > to_version:
            raise MigrationException(
                f"Target version {to_version} is below the highest applied "
                f"version {max(applied)}. migrate never rolls back, "
                "use the down subcommand instead."
            )

    def _rollback_targets(self, steps: int, to_version: Optional[int]) -> List[int]:
        applied_versions = [m.version for m in self.query_applied_migrations()]

        if to_version is not None:
            targets = [v for v in applied_versions if v > to_version]
        elif steps < 1:
            raise MigrationException("steps must be >= 1")
        else:
            targets = applied_versions[-steps:]

        # Roll back newest first.
        return sorted(targets, reverse=True)

    def rollback_migration(
        self,
        down_scripts: Dict[int, str],
        steps: int = 1,
        to_version: Optional[int] = None,
        multi_statement: bool = True,
        variables: Optional[Mapping[str, str]] = None,
        sources: Optional[Mapping[int, str]] = None,
    ) -> List[int]:
        targets = self._rollback_targets(steps, to_version)
        if not targets:
            logging.info("Nothing to roll back.")
            return []

        # Fail-fast: refuse to start unless every target has a down script, so we
        # never leave the database half rolled back on a missing file.
        missing = sorted(v for v in targets if v not in down_scripts)
        if missing:
            raise MigrationException(
                "No down migration for version(s): "
                + ", ".join(str(v) for v in missing)
            )

        # Same as missing files: substitute every down script before the first
        # one runs, so an unset variable never leaves a half rollback behind.
        rendered = self._render_scripts(
            {v: down_scripts[v] for v in targets}, variables, sources, "down migration"
        )

        for version in targets:
            logging.info("Rolling back migration version %s", version)
            statements = self._statements(
                down_scripts[version], rendered[version], multi_statement
            )
            for statement, shown in statements:
                if self._dryrun:
                    logging.info("Dry run mode, would have executed: %s", shown)
                else:
                    self._command(statement, shown)

            # Delete the row only after the down script ran, so a failed down
            # keeps the migration marked as applied. mutations_sync = 2 makes the
            # delete synchronous (waiting on all replicas), so a subsequent
            # status/rollback immediately sees the migration as pending — an
            # async mutation would otherwise still report it as applied.
            if self._dryrun:
                logging.info(
                    "Dry run mode, would have removed schema version %s", version
                )
            else:
                self._conn.command(
                    f"ALTER TABLE {self._table} "
                    f"DELETE WHERE version = {int(version)} "
                    "SETTINGS mutations_sync = 2"
                )

        if not self._dryrun:
            self.optimize_schema_table()

        return targets

    @staticmethod
    def baseline_migrations(
        migrations: List[Migration], to_version: int
    ) -> List[Migration]:
        """The local migrations a baseline up to ``to_version`` records."""
        if to_version not in {m.version for m in migrations}:
            raise MigrationException(
                f"Baseline version {to_version} is not among the local migrations."
            )

        return sorted(
            (m for m in migrations if m.version <= to_version),
            key=lambda m: m.version,
        )

    def ensure_history_is_empty(self) -> None:
        """Refuse to go on if the bookkeeping table records anything at all."""
        count = self._conn.query(f"SELECT count() AS n FROM {self._table}")[0]["n"]
        if count:
            raise MigrationException(
                f"Refusing to baseline: {self._table} already records {count} "
                "migration row(s). baseline is only for a database without "
                "migration history; use repair to fix changed migrations."
            )

    def baseline(self, migrations: List[Migration], to_version: int) -> List[Migration]:
        """Record every local migration up to ``to_version`` as applied.

        Nothing is executed. The bookkeeping table must exist (see
        ``init_schema``) and be empty. With ``dryrun`` nothing is written.
        """
        selected = self.baseline_migrations(migrations, to_version)
        self.ensure_history_is_empty()

        for migration in selected:
            logging.info(
                "%s migration %s as applied without executing it",
                "Dry run mode, would record" if self._dryrun else "Record",
                self.format_migration_log(migration),
            )

        if not self._dryrun:
            # One block, so the baseline lands as a whole or not at all.
            self._insert_schema_versions(selected)

        return selected

    @staticmethod
    def select_repair_rows(
        status_rows: List[StatusRow], versions: Optional[List[int]] = None
    ) -> List[StatusRow]:
        """The out-of-sync status rows, narrowed to ``versions`` if given.

        Every requested version must be out of sync (``md5-mismatch`` or
        ``unknown``); naming one that is in sync, pending or absent is an
        error, so a typo never silently repairs nothing.
        """
        rows = [r for r in status_rows if r.state in OUT_OF_SYNC_STATES]
        if not versions:
            return rows

        states = {r.version: r.state for r in status_rows}
        wanted = set(versions)
        invalid = sorted(v for v in wanted if states.get(v) not in OUT_OF_SYNC_STATES)
        if invalid:
            details = ", ".join(f"{v} ({states.get(v, 'not found')})" for v in invalid)
            raise MigrationException(
                "Nothing to repair for version(s) "
                f"{details}: only {' and '.join(OUT_OF_SYNC_STATES)} "
                "migrations can be repaired."
            )

        return [r for r in rows if r.version in wanted]

    def repair(
        self,
        incoming: List[Migration],
        versions: Optional[List[int]] = None,
        write: bool = False,
        prune: bool = False,
        down_versions: Optional[Set[int]] = None,
    ) -> List[StatusRow]:
        """Report, or with ``write`` fix, applied migrations out of sync.

        ``md5-mismatch`` rows get the md5 and script of the local file;
        ``unknown`` rows (applied, no local file) are deleted only with
        ``prune``. Without ``write`` nothing changes and the out-of-sync rows
        are returned. With it, the returned rows show the state afterwards:
        ``applied`` for a repaired migration, ``pruned`` for a deleted row and
        ``unknown`` for one left alone.
        """
        if prune and not write:
            raise MigrationException("prune only works together with write.")

        plan = self.select_repair_rows(
            self.migration_status(incoming, down_versions), versions
        )
        if not write:
            return plan

        local = {m.version: m for m in incoming}
        changed = False
        for row in plan:
            if row.state == STATUS_MD5_MISMATCH:
                logging.info(
                    "Repair migration %s: md5 %s -> %s",
                    row.version,
                    row.md5,
                    local[row.version].md5,
                )
                self._replace_schema_version(local[row.version])
                changed = True
            elif prune:
                logging.info("Prune unknown migration %s", row.version)
                self._delete_schema_version(row.version)
                changed = True
            else:
                logging.warning(
                    "Migration %s is applied but has no local file; "
                    "pass prune to delete its row.",
                    row.version,
                )

        if changed:
            self.optimize_schema_table()

        after = {r.version: r for r in self.migration_status(incoming, down_versions)}
        return [
            after.get(row.version, row._replace(state=STATUS_PRUNED)) for row in plan
        ]

    def _replace_schema_version(self, migration: Migration) -> None:
        # Insert the fresh row first and only then delete the stale ones, so a
        # failure in between never leaves the migration looking pending (which
        # would make the next migrate execute it again). mutations_sync = 2
        # waits for every replica, so status sees the result right away.
        self._insert_schema_version(migration)
        self._conn.command(
            f"ALTER TABLE {self._table} "
            f"DELETE WHERE version = {int(migration.version)} "
            f"AND md5 != {quote_string(migration.md5)} "
            "SETTINGS mutations_sync = 2"
        )

    def _delete_schema_version(self, version: int) -> None:
        self._conn.command(
            f"ALTER TABLE {self._table} "
            f"DELETE WHERE version = {int(version)} "
            "SETTINGS mutations_sync = 2"
        )

    def _insert_schema_version(self, migration: Migration) -> None:
        self._insert_schema_versions([migration])

    def _insert_schema_versions(self, migrations: List[Migration]) -> None:
        self._conn.insert(
            self._table,
            [
                {
                    "version": migration.version,
                    "script": migration.script,
                    "md5": migration.md5,
                }
                for migration in migrations
            ],
        )

    def optimize_schema_table(self):
        self._conn.command(f"OPTIMIZE TABLE {self._table} FINAL")

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
