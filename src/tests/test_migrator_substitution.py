import logging

import pytest

from clickhouse_migrations.connection import (
    ClickhouseConnectConnection,
    ClickhouseDriverConnection,
)
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration
from clickhouse_migrations.migrator import Migrator

SECRET = "pa55-s3cr3t"


class _Conn:
    """Connection stub that records statements (bar the bookkeeping OPTIMIZE),
    what was logged for them, and inserts; applied_versions are reported as
    already applied."""

    def __init__(self, applied_versions=()):
        self._applied_versions = list(applied_versions)
        self.commands = []
        self.logged_as = []
        self.inserts = []

    def command(self, statement, log_statement=None):
        if statement.startswith("OPTIMIZE"):
            return
        self.commands.append(statement)
        self.logged_as.append(log_statement)

    def insert(self, _table, rows):
        self.inserts.extend(rows)

    def query(self, _query):
        return [
            {"version": v, "script": f"script{v}", "md5": f"md5{v}"}
            for v in self._applied_versions
        ]


class _LegacyConn(_Conn):
    """A Connection written before log_statement existed."""

    def command(self, statement):  # pylint: disable=arguments-differ
        super().command(statement)


def _migration(version, script):
    return Migration(version=version, md5=f"md5{version}", script=script)


def test_substitutes_before_execution_and_stores_raw_script():
    conn = _Conn()
    raw = "CREATE TABLE t ON CLUSTER ${C} (x String);\nSELECT '${P}';"

    Migrator(conn).apply_migration(
        [_migration(1, raw)], True, variables={"C": "c1", "P": SECRET}
    )

    assert conn.commands == [
        "CREATE TABLE t ON CLUSTER c1 (x String);",
        f"SELECT '{SECRET}';",
    ]
    assert conn.inserts == [{"version": 1, "script": raw, "md5": "md51"}]


def test_substituted_statements_are_logged_raw():
    conn = _Conn()

    Migrator(conn).apply_migration(
        [_migration(1, "SELECT '${P}';")], True, variables={"P": SECRET}
    )

    assert conn.commands == [f"SELECT '{SECRET}';"]
    assert conn.logged_as == ["SELECT '${P}';"]


def test_statement_without_placeholder_is_passed_as_is():
    conn = _LegacyConn()

    Migrator(conn).apply_migration(
        [_migration(1, "SELECT 1;")], True, variables={"P": SECRET}
    )

    assert conn.commands == ["SELECT 1;"]


def test_without_variables_scripts_run_byte_for_byte():
    conn = _LegacyConn()
    raw = "SELECT '${NOT_A_VAR}', '$${X}', '${bad-name';"

    Migrator(conn).apply_migration([_migration(1, raw)], True)

    assert conn.commands == [raw]


def test_value_changing_the_split_is_not_logged():
    conn = _Conn()

    Migrator(conn).apply_migration(
        [_migration(1, "SELECT ${A};")], True, variables={"A": "1; SELECT 2"}
    )

    assert conn.commands == ["SELECT 1;", "SELECT 2;"]
    assert conn.logged_as == [
        "<statement 1 of 2 after variable substitution, not logged>",
        "<statement 2 of 2 after variable substitution, not logged>",
    ]


def test_single_statement_mode():
    conn = _Conn()

    Migrator(conn).apply_migration(
        [_migration(1, "SELECT ${A}; SELECT ${A};")], False, variables={"A": "7"}
    )

    assert conn.commands == ["SELECT 7; SELECT 7;"]


def test_unset_variable_fails_before_anything_executes():
    conn = _Conn()
    migrations = [_migration(1, "SELECT 1;"), _migration(2, "SELECT ${MISSING};")]

    with pytest.raises(MigrationException) as err:
        Migrator(conn).apply_migration(
            migrations, True, variables={}, sources={2: "002_x.sql"}
        )

    assert str(err.value).startswith("002_x.sql: variable(s) not set: MISSING")
    assert not conn.commands
    assert not conn.inserts


def test_error_without_sources_names_the_version():
    with pytest.raises(MigrationException, match="^migration version 3:3: malformed"):
        Migrator(_Conn()).apply_migration(
            [_migration(3, "\n\n${A-B}")], True, variables={}
        )


def test_applied_migrations_are_not_substituted():
    # Version 1 is applied already: its unset variable does not matter.
    conn = _Conn(applied_versions=[1])
    migrations = [_migration(1, "SELECT ${GONE};"), _migration(2, "SELECT ${A};")]

    applied = Migrator(conn).apply_migration(migrations, True, variables={"A": "2"})

    assert [m.version for m in applied] == [2]
    assert conn.commands == ["SELECT 2;"]


def test_dry_run_validates_and_logs_raw(caplog):
    conn = _Conn()

    with caplog.at_level(logging.DEBUG):
        Migrator(conn, dryrun=True).apply_migration(
            [_migration(1, "SELECT '${P}';")], True, variables={"P": SECRET}
        )

    assert "Dry run mode, would have executed: SELECT '${P}';" in caplog.text
    assert SECRET not in caplog.text
    assert not conn.commands


def test_dry_run_fails_on_unset_variable():
    with pytest.raises(MigrationException, match="not set: P"):
        Migrator(_Conn(), dryrun=True).apply_migration(
            [_migration(1, "SELECT '${P}';")], True, variables={}
        )


def test_fake_needs_no_variables(caplog):
    conn = _Conn()

    with caplog.at_level(logging.WARNING):
        applied = Migrator(conn).apply_migration(
            [_migration(1, "SELECT '${P}';")], True, fake=True, variables={}
        )

    assert [m.version for m in applied] == [1]
    assert "Fake mode, statement will be skipped: SELECT '${P}';" in caplog.text
    assert conn.inserts == [{"version": 1, "script": "SELECT '${P}';", "md5": "md51"}]


def test_full_log_format_shows_raw_script(caplog):
    with caplog.at_level(logging.INFO):
        Migrator(_Conn()).apply_migration(
            [_migration(1, "SELECT '${P}';")], True, variables={"P": SECRET}
        )

    assert "script=\"SELECT '${P}';\"" in caplog.text
    assert SECRET not in caplog.text


def test_rollback_substitutes_down_scripts():
    conn = _Conn(applied_versions=[1, 2])

    rolled = Migrator(conn).rollback_migration(
        {1: "DROP TABLE a ON CLUSTER ${C};", 2: "DROP TABLE b ON CLUSTER ${C};"},
        steps=2,
        variables={"C": "c1"},
    )

    assert rolled == [2, 1]
    drops = [c for c in conn.commands if c.startswith("DROP")]
    assert drops == ["DROP TABLE b ON CLUSTER c1;", "DROP TABLE a ON CLUSTER c1;"]
    assert "DROP TABLE b ON CLUSTER ${C};" in conn.logged_as


def test_rollback_unset_variable_fails_before_any_down_runs():
    conn = _Conn(applied_versions=[1, 2])

    with pytest.raises(MigrationException) as err:
        Migrator(conn).rollback_migration(
            {1: "DROP TABLE a ON CLUSTER ${MISSING};", 2: "DROP TABLE b;"},
            steps=2,
            variables={},
            sources={1: "001_a.down.sql"},
        )

    assert str(err.value).startswith("001_a.down.sql: variable(s) not set: MISSING")
    assert not any(c.startswith(("DROP", "ALTER")) for c in conn.commands)


def test_rollback_error_without_sources_names_the_version():
    with pytest.raises(MigrationException, match="^down migration version 1:1: "):
        Migrator(_Conn(applied_versions=[1])).rollback_migration(
            {1: "DROP ${"}, variables={}
        )


def test_rollback_dry_run_logs_raw(caplog):
    conn = _Conn(applied_versions=[1])

    with caplog.at_level(logging.DEBUG):
        Migrator(conn, dryrun=True).rollback_migration(
            {1: "DROP USER '${P}';"}, variables={"P": SECRET}
        )

    assert "Dry run mode, would have executed: DROP USER '${P}';" in caplog.text
    assert SECRET not in caplog.text
    assert not any(c.startswith(("DROP", "ALTER")) for c in conn.commands)


class _Client:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def command(self, statement):
        self.statements.append(statement)


@pytest.mark.parametrize(
    "connection_class", [ClickhouseDriverConnection, ClickhouseConnectConnection]
)
def test_connection_logs_log_statement_instead(connection_class, caplog):
    client = _Client()
    conn = connection_class(client)

    with caplog.at_level(logging.DEBUG):
        conn.command(f"SELECT '{SECRET}'", log_statement="SELECT '${P}'")
        conn.command("SELECT 1")

    assert client.statements == [f"SELECT '{SECRET}'", "SELECT 1"]
    assert "SELECT '${P}'" in caplog.text
    assert "SELECT 1" in caplog.text
    assert SECRET not in caplog.text
