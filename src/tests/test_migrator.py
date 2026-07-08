from pathlib import Path

import pytest

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration
from clickhouse_migrations.migrator import (
    STATUS_APPLIED,
    STATUS_MD5_MISMATCH,
    STATUS_PENDING,
    STATUS_UNKNOWN,
    Migrator,
)

FIXTURES_DIR = Path(__file__).parent


class _FakeConn:
    """Minimal Connection stub that records commands and returns a fixed set of
    applied migrations, so rollback branching can be tested without ClickHouse."""

    def __init__(self, applied_versions):
        self._applied_versions = applied_versions
        self.commands = []

    def command(self, statement):
        self.commands.append(statement)

    def query(self, _query):
        return [
            {"version": v, "script": f"script{v}", "md5": f"md5{v}"}
            for v in self._applied_versions
        ]


def _down_scripts(*versions):
    return {v: f"DROP TABLE t{v};" for v in versions}


def test_split_statements_with_multi_line_ok():
    script = """create table test
    (
        `app_id`     Int16
    )
        ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)
            PRIMARY KEY app_id
            ORDER BY (app_id, created_at)
            SETTINGS index_granularity = 8192;

    create table refund
    (
        `app_id`       Int16
    )
        ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)
            PRIMARY KEY app_id
            ORDER BY (app_id, created_at)
            SETTINGS index_granularity = 8192;
    """

    statemets = Migrator.script_to_statements(script, True)

    assert len(statemets) == 2
    assert statemets[0][-1] == ";"


def test_split_and_ignore_empy_ok():
    script = """create table test
    (
        `app_id`     Int16
    )
        ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)
            PRIMARY KEY app_id
            ORDER BY (app_id, created_at)
            SETTINGS index_granularity = 8192; ;;;
    """

    statemets = Migrator.script_to_statements(script, True)

    assert len(statemets) == 1
    assert statemets[0][-1] == ";"


def test_single_statement_mode_returns_whole_script():
    script = "SELECT 1; SELECT 2;"
    assert Migrator.script_to_statements(script, False) == ["SELECT 1; SELECT 2;"]


def test_trailing_statement_without_semicolon_is_kept():
    statements = Migrator.script_to_statements("SELECT 1;\nSELECT 2", True)
    assert statements == ["SELECT 1;", "SELECT 2;"]


def test_semicolon_inside_single_quoted_string_is_not_a_delimiter():
    statements = Migrator.script_to_statements(
        "INSERT INTO t VALUES ('a;b'), ('c;d');", True
    )
    assert statements == ["INSERT INTO t VALUES ('a;b'), ('c;d');"]


def test_semicolon_inside_line_comment_is_not_a_delimiter():
    statements = Migrator.script_to_statements(
        "SELECT 1;\n-- comment with ; inside\nSELECT 2;", True
    )
    assert statements == ["SELECT 1;", "-- comment with ; inside\nSELECT 2;"]


def test_semicolon_inside_block_comment_is_not_a_delimiter():
    statements = Migrator.script_to_statements("/* a; b */ SELECT 1;", True)
    assert statements == ["/* a; b */ SELECT 1;"]


def test_semicolon_inside_backtick_identifier_is_not_a_delimiter():
    statements = Migrator.script_to_statements("CREATE TABLE `a;b` (x Int8);", True)
    assert statements == ["CREATE TABLE `a;b` (x Int8);"]


def test_semicolon_inside_double_quoted_identifier_is_not_a_delimiter():
    statements = Migrator.script_to_statements('SELECT "a;b" FROM t;', True)
    assert statements == ['SELECT "a;b" FROM t;']


def test_escaped_quote_inside_string_is_handled():
    statements = Migrator.script_to_statements(
        "INSERT INTO t VALUES ('it''s ok; really'); SELECT 1;", True
    )
    assert statements == [
        "INSERT INTO t VALUES ('it''s ok; really');",
        "SELECT 1;",
    ]


# Golden regression guard: the real migration fixtures must keep splitting
# into the same number of statements as before the splitter was rewritten.
@pytest.mark.parametrize(
    "rel_path,expected_count",
    [
        ("migrations/001_create_test.sql", 1),
        ("complex_migrations/001_init.sql", 1),
        ("complex_migrations/002_test2.sql", 2),
        ("complex_migrations/003_third_test.sql", 3),
        ("complex_migrations/010_migrations_is_not_in_row.sql", 1),
        ("issue_15/001_init.sql", 2),
        ("multi_statements_migrations/001_create_test.sql", 2),
    ],
)
def test_real_fixture_split_counts(rel_path, expected_count):
    script = (FIXTURES_DIR / rel_path).read_text(encoding="utf8")
    statements = Migrator.script_to_statements(script, True)

    assert len(statements) == expected_count
    assert all(s.endswith(";") for s in statements)
    assert all(s.strip(";").strip() for s in statements)


def test_full_migration_log_format_keeps_previous_behavior():
    migration = Migration(
        version=1,
        md5="abc",
        script="CREATE TABLE test(value String)",
    )
    migrator = Migrator(None)

    assert migrator.format_migration_log(migration) == str(migration)


def test_compact_migration_log_format_does_not_include_script():
    migration = Migration(
        version=1,
        md5="abc",
        script="CREATE TABLE test(value String)",
    )
    migrator = Migrator(None, migration_log_format="compact")

    assert migrator.format_migration_log(migration) == "version=1, md5=abc"


def test_unknown_migration_log_format_raises_error():
    with pytest.raises(ValueError):
        Migrator(None, migration_log_format="unknown")


def test_build_status_classifies_every_state():
    incoming = [
        Migration(version=1, md5="a", script="s1"),  # applied
        Migration(version=2, md5="b", script="s2"),  # pending
        Migration(version=3, md5="c", script="s3"),  # md5 mismatch
    ]
    applied = {
        1: ("a", "2024-01-01 00:00:00"),
        3: ("different", "2024-01-03 00:00:00"),
        4: ("d", "2024-01-04 00:00:00"),  # applied but not known locally
    }

    rows = Migrator._build_status(incoming, applied)  # pylint: disable=protected-access
    by_version = {r.version: r for r in rows}

    assert [r.version for r in rows] == [1, 2, 3, 4]
    assert by_version[1].state == STATUS_APPLIED
    assert by_version[1].applied_at == "2024-01-01 00:00:00"
    assert by_version[2].state == STATUS_PENDING
    assert by_version[2].applied_at is None
    assert by_version[3].state == STATUS_MD5_MISMATCH
    assert by_version[4].state == STATUS_UNKNOWN


def test_build_status_empty():
    # pylint: disable=protected-access
    assert not Migrator._build_status([], {})


def test_rollback_default_step_removes_newest():
    conn = _FakeConn([1, 2, 3])
    migrator = Migrator(conn)

    rolled = migrator.rollback_migration(_down_scripts(1, 2, 3))

    assert rolled == [3]
    assert "DROP TABLE t3;" in conn.commands
    assert any("DELETE WHERE version = 3" in c for c in conn.commands)
    assert not any("DELETE WHERE version = 2" in c for c in conn.commands)


def test_rollback_multiple_steps_are_newest_first():
    conn = _FakeConn([1, 2, 3])
    migrator = Migrator(conn)

    rolled = migrator.rollback_migration(_down_scripts(1, 2, 3), steps=2)

    assert rolled == [3, 2]
    deletes = [c for c in conn.commands if "DELETE WHERE version" in c]
    # Newest version is deleted first.
    assert "version = 3" in deletes[0]
    assert "version = 2" in deletes[1]


def test_rollback_to_version_rolls_back_everything_above():
    conn = _FakeConn([1, 2, 3])
    migrator = Migrator(conn)

    rolled = migrator.rollback_migration(_down_scripts(1, 2, 3), to_version=1)

    assert rolled == [3, 2]


def test_rollback_to_version_zero_rolls_back_all():
    conn = _FakeConn([1, 2])
    migrator = Migrator(conn)

    assert migrator.rollback_migration(_down_scripts(1, 2), to_version=0) == [2, 1]


def test_rollback_no_applied_returns_empty():
    conn = _FakeConn([])
    migrator = Migrator(conn)

    assert migrator.rollback_migration(_down_scripts(1)) == []


def test_rollback_to_version_above_head_is_noop():
    conn = _FakeConn([1, 2])
    migrator = Migrator(conn)

    assert migrator.rollback_migration(_down_scripts(1, 2), to_version=5) == []


def test_rollback_missing_down_script_fails_fast():
    conn = _FakeConn([1, 2])
    migrator = Migrator(conn)

    with pytest.raises(MigrationException, match="No down migration for version"):
        migrator.rollback_migration(_down_scripts(1), steps=2)

    # Nothing should have been rolled back.
    assert not any("DELETE WHERE version" in c for c in conn.commands)


def test_rollback_invalid_steps_raises():
    conn = _FakeConn([1, 2])
    migrator = Migrator(conn)

    with pytest.raises(MigrationException, match="steps must be >= 1"):
        migrator.rollback_migration(_down_scripts(1, 2), steps=0)


def test_rollback_dry_run_executes_nothing():
    conn = _FakeConn([1, 2])
    migrator = Migrator(conn, dryrun=True)

    rolled = migrator.rollback_migration(_down_scripts(1, 2), steps=1)

    assert rolled == [2]
    assert not any("DROP TABLE" in c for c in conn.commands)
    assert not any("DELETE WHERE version" in c for c in conn.commands)
