import logging

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster, _short_error
from clickhouse_migrations.command_line import get_context
from clickhouse_migrations.migrator import Migrator

# ---- CLI parsing -----------------------------------------------------------


def test_diff_defaults(monkeypatch):
    for name in ("DIFF_NAME", "ALLOW_DESTRUCTIVE", "DRY_RUN", "MIGRATIONS_DIR"):
        monkeypatch.delenv(name, raising=False)

    ctx = get_context(["diff", "--to", "schema.sql", "--db-name", "x"])

    assert ctx.command == "diff"
    assert ctx.target.name == "schema.sql"
    assert (ctx.db_name, ctx.name) == ("x", "diff")
    assert ctx.allow_destructive is False
    assert ctx.dry_run is False
    assert ctx.migrations_dir.name == "migrations"
    assert ctx.migrations_table == "schema_versions"


def test_diff_flags_and_env(monkeypatch):
    monkeypatch.setenv("DIFF_NAME", "from env")
    monkeypatch.setenv("ALLOW_DESTRUCTIVE", "1")
    monkeypatch.setenv("DRY_RUN", "yes")
    ctx = get_context(["diff", "--to", "s.sql"])
    assert (ctx.name, ctx.allow_destructive, ctx.dry_run) == ("from env", True, True)

    ctx = get_context(
        [
            "diff",
            "--to",
            "s.sql",
            "--name",
            "x",
            "--no-allow-destructive",
            "--no-dry-run",
            "--migrations-dir",
            "m",
            "--driver",
            "clickhouse-connect",
            "--db-url",
            "clickhouse://h/db",
        ]
    )
    assert (ctx.name, ctx.allow_destructive, ctx.dry_run) == ("x", False, False)
    assert (str(ctx.migrations_dir), ctx.driver, ctx.db_url) == (
        "m",
        "clickhouse-connect",
        "clickhouse://h/db",
    )


def test_diff_requires_to_and_rejects_migrate_options(capsys):
    with pytest.raises(SystemExit):
        get_context(["diff"])
    for flag in ("--cluster-name=c", "--fake", "--migrations=1", "--lock"):
        with pytest.raises(SystemExit):
            get_context(["diff", "--to", "s.sql", flag])
    capsys.readouterr()


# ---- scratch database handling without a server ----------------------------


def _gone(db_name=None):
    raise ConnectionError(f"server is gone ({db_name!r})")


def test_drop_scratch_failure_is_logged_not_raised(monkeypatch, caplog):
    cluster = ClickhouseCluster(db_host="localhost")
    monkeypatch.setattr(cluster, "connection", _gone)

    with caplog.at_level(logging.ERROR):
        cluster._drop_scratch("_chm_diff_x")  # pylint: disable=protected-access

    assert "Could not drop the scratch database _chm_diff_x" in caplog.text
    assert "server is gone" in caplog.text


def test_short_error():
    assert _short_error(
        Exception("Code: 62.\nDB::Exception: bad.\nStack trace:\n0. x")
    ) == ("Code: 62. DB::Exception: bad.")
    assert _short_error(KeyError()) == "KeyError()"


# ---- the migrator skips comment-only chunks --------------------------------


def test_comment_only_statements_are_not_sent():
    script = (
        "-- header\n-- DROP TABLE a;\n\nCREATE TABLE b (x UInt8) ENGINE = Memory;\n"
        "-- DROP COLUMN c;\n/* trailing */\n"
    )

    assert Migrator.script_to_statements(script, True) == [
        "-- header\n-- DROP TABLE a;\n\nCREATE TABLE b (x UInt8) ENGINE = Memory;"
    ]
    assert not Migrator.script_to_statements("-- only a comment;\n", True)
