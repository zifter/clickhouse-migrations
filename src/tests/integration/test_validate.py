"""validate is offline, but its idea of "a statement" must match what migrate
really sends to ClickHouse - these tests check that against the server."""

import shutil
from pathlib import Path

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.connection import (
    ClickhouseConnectConnection,
    ClickhouseDriverConnection,
)
from clickhouse_migrations.migration import DOWN_SUFFIX, MigrationStorage
from clickhouse_migrations.migrator import STATUS_PENDING, split_statement_tokens
from clickhouse_migrations.validate import (
    CHECK_EMPTY_FILE,
    CHECK_EMPTY_STATEMENT,
    CHECK_UNTERMINATED,
    LEVEL_ERROR,
    validate_exit_code,
    validate_migrations,
)

FIXTURES = Path(__file__).parents[1] / "validate"
VALID = FIXTURES / "valid"


def _record_commands(monkeypatch):
    executed = []
    for connection_class in (ClickhouseDriverConnection, ClickhouseConnectConnection):
        original = connection_class.command

        def command(self, statement, _original=original):
            executed.append(statement)
            return _original(self, statement)

        monkeypatch.setattr(connection_class, "command", command)
    return executed


def _validated_statements(path: Path):
    # The statements validate checks, rebuilt from its own tokens.
    return [
        "".join(token.text for token in tokens).strip() + ";"
        for tokens in split_statement_tokens(path.read_text(encoding="utf8"))
    ]


def _migration_statements(executed):
    # Leave out the bookkeeping that migrate/down run around the migrations.
    return [
        s
        for s in executed
        if "schema_versions" not in s and not s.startswith("CREATE DATABASE")
    ]


@pytest.mark.parametrize("driver", ["clickhouse-driver", "clickhouse-connect"])
def test_valid_directory_applies_and_statements_match(monkeypatch, driver):
    report = validate_migrations(VALID, require_down=True)
    assert validate_exit_code(report, strict=True) == 0

    cluster = ClickhouseCluster(
        db_host="localhost", db_user="default", db_password="", driver=driver
    )
    executed = _record_commands(monkeypatch)

    applied = cluster.migrate("pytest", VALID)

    assert [m.version for m in applied] == [1, 2]
    up_files = sorted(MigrationStorage(VALID).filenames())
    expected = [s for path in up_files for s in _validated_statements(path)]
    assert _migration_statements(executed) == expected
    # The ';' inside strings, quoted names and comments did not split anything.
    assert len(expected) == 5
    with cluster.connection("pytest") as conn:
        rows = conn.query("SELECT note FROM events ORDER BY `id;key`")
    assert [r["note"] for r in rows] == ["a'b; ON CLUSTER c", "SET x = 1"]

    executed.clear()
    assert cluster.rollback("pytest", VALID, steps=2) == [2, 1]

    down_files = sorted(VALID.glob(f"*{DOWN_SUFFIX}"), reverse=True)
    expected = [s for path in down_files for s in _validated_statements(path)]
    assert _migration_statements(executed) == expected
    assert "events" not in cluster.show_tables("pytest")


_SERVER_REJECTS = [
    ("unterminated", "001_string.sql", CHECK_UNTERMINATED),
    ("unterminated", "002_backtick.sql", CHECK_UNTERMINATED),
    ("unterminated", "003_double_quote.sql", CHECK_UNTERMINATED),
    ("unterminated", "004_block_comment.sql", CHECK_UNTERMINATED),
    ("unterminated", "005_escaped_quote.sql", CHECK_UNTERMINATED),
    ("empty_statement", "001_trailing_comment.sql", CHECK_EMPTY_STATEMENT),
    ("empty_file", "003_scaffold_only.sql", CHECK_EMPTY_FILE),
    ("empty_file", "004_block_comment_only.sql", CHECK_EMPTY_FILE),
]


@pytest.mark.parametrize("fixture,name,check", _SERVER_REJECTS)
def test_content_errors_are_rejected_by_the_server(
    cluster: ClickhouseCluster, tmp_path, fixture, name, check
):
    storage_dir = tmp_path / "migrations"
    storage_dir.mkdir()
    shutil.copy(FIXTURES / fixture / name, storage_dir / "001_m.sql")

    findings = validate_migrations(storage_dir).findings
    assert [(f.level, f.check) for f in findings] == [(LEVEL_ERROR, check)]

    with pytest.raises(Exception):
        cluster.migrate("pytest", storage_dir)

    rows = cluster.status("pytest", storage_dir)
    assert [(r.version, r.state) for r in rows] == [(1, STATUS_PENDING)]
