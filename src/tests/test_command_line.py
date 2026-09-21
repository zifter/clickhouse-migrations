import json
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest

from clickhouse_migrations import __version__, command_line
from clickhouse_migrations.command_line import (
    cast_to_bool,
    format_status,
    format_status_json,
    get_context,
    main,
    show_status,
    status_exit_code,
)
from clickhouse_migrations.migrator import StatusRow

TESTS_DIR = Path(__file__).parent


def test_cast_to_bool_ok():
    for value in ("1", "true", "True", "TRUE", "yes", "YES", "y", "Y"):
        assert cast_to_bool(value) is True

    for value in ("0", "false", "False", "no", "n", "off", "", "garbage"):
        assert cast_to_bool(value) is False


def test_check_multistatement_arg():
    context = get_context([])
    assert context.multi_statement is True

    context = get_context(["--multi-statement"])
    assert context.multi_statement is True

    context = get_context(["--no-multi-statement"])
    assert context.multi_statement is False


def test_check_dry_run_ok():
    context = get_context([])
    assert context.dry_run is False

    context = get_context(["--dry-run"])
    assert context.dry_run is True

    context = get_context(["--no-dry-run"])
    assert context.dry_run is False


def test_check_secure_ok():
    context = get_context([])
    assert context.secure is False

    context = get_context(["--secure"])
    assert context.secure is True

    context = get_context(["--no-secure"])
    assert context.secure is False


def test_check_boolean_args_default_from_env(monkeypatch):
    monkeypatch.setenv("MULTI_STATEMENT", "0")
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("FAKE", "yes")
    monkeypatch.setenv("SECURE", "y")
    monkeypatch.setenv("CREATE_DB_IF_NOT_EXISTS", "false")

    context = get_context([])
    assert context.multi_statement is False
    assert context.dry_run is True
    assert context.fake is True
    assert context.secure is True
    assert context.create_db_if_not_exists is False


def test_check_boolean_args_cli_overrides_env(monkeypatch):
    monkeypatch.setenv("DRY_RUN", "1")
    context = get_context(["--no-dry-run"])
    assert context.dry_run is False

    monkeypatch.setenv("SECURE", "0")
    context = get_context(["--secure"])
    assert context.secure is True


def test_check_explicit_migrations_args_ok():
    context = get_context(["--migrations", "001_init", "002_test2"])
    assert context.migrations == ["001_init", "002_test2"]


def test_check_fake_ok():
    context = get_context(
        [
            "--fake",
        ]
    )
    assert context.fake is True

    context = get_context(
        [
            "--no-fake",
        ]
    )
    assert context.fake is False


def test_check_create_db_if_not_exists_ok():
    context = get_context([])
    assert context.create_db_if_not_exists is True

    context = get_context(["--create-db-if-not-exists"])
    assert context.create_db_if_not_exists is True

    context = get_context(["--no-create-db-if-not-exists"])
    assert context.create_db_if_not_exists is False


def test_check_migration_log_format_arg_ok():
    context = get_context([])
    assert context.migration_log_format == "full"

    context = get_context(["--migration-log-format", "compact"])
    assert context.migration_log_format == "compact"


def test_check_migration_log_format_env_ok(monkeypatch):
    monkeypatch.setenv("MIGRATION_LOG_FORMAT", "compact")

    context = get_context([])
    assert context.migration_log_format == "compact"


def test_check_migration_log_format_invalid_rejected():
    with pytest.raises(SystemExit):
        get_context(["--migration-log-format", "bogus"])


def test_version_flag_prints_version_and_exits(capsys):
    with pytest.raises(SystemExit) as exc_info:
        get_context(["--version"])

    assert exc_info.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_version_subcommand(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", "version"])

    assert main() == 0
    assert __version__ in capsys.readouterr().out


def test_main_returns_zero_on_success(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations"])
    monkeypatch.setattr(command_line, "migrate", lambda ctx: [])

    assert main() == 0


def test_main_returns_error_on_migration_exception(monkeypatch, tmp_path):
    (tmp_path / "001_a.sql").write_text("SELECT 1;", encoding="utf8")
    (tmp_path / "001_b.sql").write_text("SELECT 2;", encoding="utf8")
    monkeypatch.setattr(
        sys,
        "argv",
        ["clickhouse-migrations", "--migrations-dir", str(tmp_path)],
    )

    assert main() == 1


def test_bare_invocation_defaults_to_migrate():
    assert get_context([]).command == "migrate"
    assert get_context(["--db-name", "x"]).command == "migrate"
    assert get_context(["migrate", "--db-name", "x"]).command == "migrate"


def test_status_subcommand():
    context = get_context(["status", "--db-name", "x"])
    assert context.command == "status"
    assert context.db_name == "x"


def test_down_subcommand_defaults():
    context = get_context(["down", "--db-name", "x"])
    assert context.command == "down"
    assert context.db_name == "x"
    assert context.steps == 1
    assert context.to_version is None
    assert context.dry_run is False
    assert context.multi_statement is True


def test_down_subcommand_steps_and_to():
    context = get_context(["down", "--steps", "3", "--to", "5"])
    assert context.steps == 3
    assert context.to_version == 5


def test_down_subcommand_dry_run_and_no_multi_statement():
    context = get_context(["down", "--dry-run", "--no-multi-statement"])
    assert context.dry_run is True
    assert context.multi_statement is False


def test_main_down_dispatches_to_rollback(monkeypatch):
    calls = []
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", "down"])
    monkeypatch.setattr(command_line, "rollback", calls.append)

    assert main() == 0
    assert len(calls) == 1
    assert calls[0].command == "down"


def test_driver_default_and_override():
    assert get_context([]).driver == "clickhouse-driver"
    assert (
        get_context(["--driver", "clickhouse-connect"]).driver == "clickhouse-connect"
    )


def test_format_status_empty():
    assert format_status([]) == "No migrations found."


def test_format_status_table():
    rows = [
        StatusRow(1, "applied", "abc", "2024-01-01 00:00:00"),
        StatusRow(2, "pending", "def", None),
    ]

    out = format_status(rows)

    assert "VERSION" in out and "STATUS" in out
    assert "applied" in out and "pending" in out
    assert "2024-01-01 00:00:00" in out


def test_new_subcommand_parses_without_db_arguments(tmp_path):
    context = get_context(["new", "add events", "--dir", str(tmp_path)])

    assert context.command == "new"
    assert context.name == "add events"
    assert context.migrations_dir == tmp_path
    assert context.down is False
    assert context.version is None
    # Scaffolding is local only: no database arguments exist on this subcommand.
    assert not hasattr(context, "db_host")
    assert not hasattr(context, "db_url")


def test_new_subcommand_rejects_db_arguments():
    with pytest.raises(SystemExit):
        get_context(["new", "add events", "--db-name", "x"])


def test_new_subcommand_dir_defaults_to_migrations_dir_env(monkeypatch, tmp_path):
    monkeypatch.setenv("MIGRATIONS_DIR", str(tmp_path))

    context = get_context(["new", "add events"])
    assert context.migrations_dir == tmp_path

    context = get_context(
        ["new", "add events", "--migrations-dir", str(tmp_path / "x")]
    )
    assert context.migrations_dir == tmp_path / "x"


def test_new_subcommand_flags():
    context = get_context(["new", "add events", "--down", "--version", "7"])
    assert context.down is True
    assert context.version == 7


def test_main_new_creates_file(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "clickhouse-migrations",
            "new",
            "add events",
            "--dir",
            str(tmp_path),
            "--down",
        ],
    )

    assert main() == 0

    out = capsys.readouterr().out
    assert str(tmp_path / "001_add_events.sql") in out
    assert str(tmp_path / "001_add_events.down.sql") in out
    assert (tmp_path / "001_add_events.sql").exists()
    assert (tmp_path / "001_add_events.down.sql").exists()


def test_main_new_returns_error_on_taken_version(monkeypatch, tmp_path):
    (tmp_path / "001_init.sql").write_text("SELECT 1;", encoding="utf8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "clickhouse-migrations",
            "new",
            "add events",
            "--dir",
            str(tmp_path),
            "--version",
            "1",
        ],
    )

    assert main() == 1


def test_migrations_table_args_default_to_legacy_values():
    context = get_context([])

    assert context.migrations_table == "schema_versions"
    assert context.migrations_table_engine is None


@pytest.mark.parametrize("command", ["migrate", "status", "down"])
def test_migrations_table_args_available_on_every_db_subcommand(command):
    engine = "ReplicatedMergeTree('/ch/{shard}/{database}/{table}', '{replica}')"
    context = get_context(
        [
            command,
            "--migrations-table",
            "meta.my_versions",
            "--migrations-table-engine",
            engine,
        ]
    )

    assert context.migrations_table == "meta.my_versions"
    assert context.migrations_table_engine == engine


def test_migrations_table_args_default_from_env(monkeypatch):
    monkeypatch.setenv("MIGRATIONS_TABLE", "meta.my_versions")
    monkeypatch.setenv("MIGRATIONS_TABLE_ENGINE", "Memory")

    context = get_context([])

    assert context.migrations_table == "meta.my_versions"
    assert context.migrations_table_engine == "Memory"


def test_create_cluster_passes_migrations_table_options():
    context = get_context(
        [
            "--migrations-table",
            "meta.my_versions",
            "--migrations-table-engine",
            "Memory",
        ]
    )

    cluster = command_line.create_cluster(context)

    assert cluster.migrations_table == "meta.my_versions"
    assert cluster.migrations_table_engine == "Memory"


def test_migrate_to_version_default_and_parsed():
    assert get_context(["migrate"]).to_version is None
    assert get_context(["migrate", "--to", "3"]).to_version == 3
    # bare backwards-compatible invocation is shimmed to migrate
    bare = get_context(["--to", "2"])
    assert bare.command == "migrate"
    assert bare.to_version == 2


def test_migrate_to_conflicts_with_migrations(capsys):
    with pytest.raises(SystemExit) as exc_info:
        get_context(["migrate", "--to", "2", "--migrations", "1"])

    assert exc_info.value.code == 2
    assert "--to cannot be combined with --migrations" in capsys.readouterr().err


def test_migrations_alone_still_allowed():
    assert get_context(["--migrations", "1"]).to_version is None


def test_do_migrate_passes_to_version():
    calls = []
    cluster = types.SimpleNamespace(migrate=lambda **kw: calls.append(kw) or [])

    command_line.do_migrate(cluster, get_context(["--to", "4"]))

    assert calls[0]["to_version"] == 4


def _status_rows():
    return {
        "applied": StatusRow(1, "applied", "a1", datetime(2024, 1, 1, 12), True),
        "pending": StatusRow(2, "pending", "b2", None, False),
        "md5-mismatch": StatusRow(3, "md5-mismatch", "c3", datetime(2024, 1, 3), True),
        "unknown": StatusRow(4, "unknown", "d4", "2024-01-04 00:00:00", False),
    }


def test_status_flags_defaults(monkeypatch):
    for name in ("STRICT", "EXIT_CODE_PENDING", "STATUS_FORMAT"):
        monkeypatch.delenv(name, raising=False)
    context = get_context(["status"])
    assert context.strict is False
    assert context.exit_code_pending is False
    assert context.format == "table"


def test_status_flags_from_cli_and_env(monkeypatch):
    context = get_context(
        ["status", "--strict", "--exit-code-pending", "--format", "json"]
    )
    assert context.strict and context.exit_code_pending
    assert context.format == "json"

    monkeypatch.setenv("STRICT", "1")
    monkeypatch.setenv("EXIT_CODE_PENDING", "true")
    monkeypatch.setenv("STATUS_FORMAT", "json")
    context = get_context(["status"])
    assert context.strict and context.exit_code_pending
    assert context.format == "json"
    assert get_context(["status", "--no-strict"]).strict is False


def test_status_flags_rejected_for_other_subcommands():
    for command in ("migrate", "down"):
        with pytest.raises(SystemExit):
            get_context([command, "--strict"])
    with pytest.raises(SystemExit):
        get_context(["status", "--format", "xml"])


@pytest.mark.parametrize(
    "state,strict,pending,expected",
    [
        ("applied", True, True, 0),
        ("pending", False, False, 0),
        ("pending", True, False, 0),
        ("pending", False, True, 1),
        ("pending", True, True, 1),
        ("md5-mismatch", False, False, 0),
        ("md5-mismatch", True, False, 1),
        ("md5-mismatch", False, True, 0),
        ("md5-mismatch", True, True, 1),
        ("unknown", False, False, 0),
        ("unknown", True, False, 1),
        ("unknown", False, True, 0),
        ("unknown", True, True, 1),
    ],
)
def test_status_exit_code(state, strict, pending, expected):
    rows = [_status_rows()["applied"], _status_rows()[state]]
    assert status_exit_code(rows, strict, pending) == expected


def test_status_exit_code_defaults_and_empty():
    assert status_exit_code(list(_status_rows().values())) == 0
    assert status_exit_code([], True, True) == 0


def test_format_status_table_has_down_column():
    out = format_status(list(_status_rows().values()))
    lines = out.splitlines()
    assert lines[0].split()[-2:] == ["HAS", "DOWN"]
    assert [line.split()[-1] for line in lines[1:]] == ["yes", "no", "yes", "no"]


def test_format_status_json_shape():
    rows = list(_status_rows().values())

    doc = json.loads(format_status_json("test", rows))

    assert list(doc) == ["database", "migrations"]
    assert doc["database"] == "test"
    assert doc["migrations"][0] == {
        "version": 1,
        "state": "applied",
        "md5": "a1",
        "applied_at": "2024-01-01T12:00:00",
        "has_down": True,
    }
    assert doc["migrations"][1]["applied_at"] is None
    assert doc["migrations"][1]["has_down"] is False
    assert doc["migrations"][2]["applied_at"] == "2024-01-03T00:00:00"
    assert doc["migrations"][3]["applied_at"] == "2024-01-04 00:00:00"
    assert json.loads(format_status_json("test", []))["migrations"] == []


@pytest.mark.parametrize("fmt", ["table", "json"])
@pytest.mark.parametrize("state", ["applied", "pending", "md5-mismatch", "unknown"])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("pending", [False, True])
def test_main_status_matrix(monkeypatch, capsys, fmt, state, strict, pending):
    rows = [_status_rows()[state]]
    monkeypatch.setattr(command_line, "create_cluster", lambda ctx: _FakeCluster())
    monkeypatch.setattr(command_line, "do_status", lambda cluster, ctx: rows)
    argv = ["clickhouse-migrations", "status", "--db-name", "test", "--format", fmt]
    if strict:
        argv.append("--strict")
    if pending:
        argv.append("--exit-code-pending")
    monkeypatch.setattr(sys, "argv", argv)

    code = main()

    expected = 0
    if strict and state in ("md5-mismatch", "unknown"):
        expected = 1
    if pending and state == "pending":
        expected = 1
    assert code == expected
    out = capsys.readouterr().out
    if fmt == "json":
        assert json.loads(out)["migrations"][0]["state"] == state
    else:
        assert state in out


def test_show_status_json_falls_back_to_default_db_name(monkeypatch, capsys):
    monkeypatch.setattr(command_line, "create_cluster", lambda ctx: _FakeCluster())
    monkeypatch.setattr(command_line, "do_status", lambda cluster, ctx: [])

    code = show_status(get_context(["status", "--format", "json"]))

    assert code == 0
    assert json.loads(capsys.readouterr().out)["database"] == "default_db"


class _FakeCluster:  # pylint: disable=too-few-public-methods
    default_db_name = "default_db"
