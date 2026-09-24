import logging
import types

import pytest

from clickhouse_migrations import command_line
from clickhouse_migrations.command_line import (
    get_context,
    warn_debug_with_substitution,
)

SECRET = "pa55-s3cr3t"


@pytest.fixture(autouse=True)
def _no_substitution_env(monkeypatch):
    monkeypatch.delenv("MIGRATION_VARS", raising=False)
    monkeypatch.delenv("SUBSTITUTE_ENV", raising=False)


@pytest.mark.parametrize("command", [[], ["migrate"], ["down"]])
def test_substitution_is_off_by_default(command):
    ctx = get_context(command)

    assert ctx.variables is None
    assert ctx.substitute_env is False


@pytest.mark.parametrize("command", [[], ["migrate"], ["down"]])
def test_var_is_repeatable(command):
    ctx = get_context([*command, "--var", "A=1", "--var", "B=x=y", "--var", "E="])

    assert ctx.variables == {"A": "1", "B": "x=y", "E": ""}


def test_last_var_wins():
    assert get_context(["--var", "A=1", "--var", "A=2"]).variables == {"A": "2"}


@pytest.mark.parametrize("command", [["migrate"], ["down"]])
def test_var_without_equals_fails_without_echoing_it(command, capsys):
    with pytest.raises(SystemExit) as exc:
        get_context([*command, "--var", SECRET])

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "argument --var: expected NAME=VALUE, but there is no '='" in err
    assert SECRET not in err


def test_var_with_invalid_name_fails_without_echoing_value(capsys):
    with pytest.raises(SystemExit) as exc:
        get_context(["--var", f"PG-HOST={SECRET}"])

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "argument --var: invalid variable name 'PG-HOST'" in err
    assert SECRET not in err


@pytest.mark.parametrize("command", [[], ["down"]])
def test_migration_vars_env(command, monkeypatch):
    monkeypatch.setenv("MIGRATION_VARS", "A=1,,B=x=y,")

    assert get_context(command).variables == {"A": "1", "B": "x=y"}


def test_migration_vars_env_is_ignored_when_var_is_given(monkeypatch):
    monkeypatch.setenv("MIGRATION_VARS", "A=env,B=env")

    assert get_context(["--var", "A=cli"]).variables == {"A": "cli"}


def test_empty_migration_vars_env_keeps_substitution_off(monkeypatch):
    monkeypatch.setenv("MIGRATION_VARS", ",")

    assert get_context([]).variables is None


@pytest.mark.parametrize("command", [["migrate"], ["down"]])
def test_bad_migration_vars_env_fails_clearly(command, monkeypatch, capsys):
    monkeypatch.setenv("MIGRATION_VARS", f"A=1,{SECRET}")

    with pytest.raises(SystemExit) as exc:
        get_context(command)

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "MIGRATION_VARS: expected NAME=VALUE" in err
    assert SECRET not in err


@pytest.mark.parametrize("command", [[], ["down"]])
def test_substitute_env_flag(command, monkeypatch):
    assert get_context([*command, "--substitute-env"]).substitute_env is True

    monkeypatch.setenv("SUBSTITUTE_ENV", "true")
    assert get_context(command).substitute_env is True
    assert get_context([*command, "--no-substitute-env"]).substitute_env is False


@pytest.mark.parametrize("command", ["status", "dump", "unlock"])
def test_other_subcommands_reject_var(command):
    with pytest.raises(SystemExit):
        get_context([command, "--var", "A=1"])


def test_do_migrate_passes_substitution_options():
    calls = []
    cluster = types.SimpleNamespace(migrate=lambda **kw: calls.append(kw) or [])

    command_line.do_migrate(cluster, get_context(["--var", "A=1", "--substitute-env"]))

    assert calls[0]["variables"] == {"A": "1"}
    assert calls[0]["substitute_env"] is True


def test_do_rollback_passes_substitution_options():
    calls = []
    cluster = types.SimpleNamespace(rollback=lambda **kw: calls.append(kw) or [])

    command_line.do_rollback(cluster, get_context(["down", "--var", "A=1"]))

    assert calls[0]["variables"] == {"A": "1"}
    assert calls[0]["substitute_env"] is False


@pytest.mark.parametrize(
    "args, warned",
    [
        (["--log-level", "debug", "--var", "A=1"], True),
        (["down", "--log-level", "debug", "--substitute-env"], True),
        (["--log-level", "debug"], False),
        (["--log-level", "info", "--var", "A=1"], False),
    ],
)
def test_debug_with_substitution_warns(args, warned, caplog):
    with caplog.at_level(logging.WARNING):
        warn_debug_with_substitution(get_context(args))

    assert ("driver's own debug log" in caplog.text) is warned


@pytest.mark.parametrize(
    "entry, method",
    [(command_line.migrate, "migrate"), (command_line.rollback, "rollback")],
)
def test_cli_entry_points_warn(entry, method, monkeypatch, caplog):
    fake = types.SimpleNamespace(**{method: lambda **kw: []})
    monkeypatch.setattr(
        "clickhouse_migrations.cli.common.create_cluster", lambda ctx: fake
    )
    args = ["--log-level", "debug", "--var", "A=1"]

    with caplog.at_level(logging.WARNING):
        entry(get_context(["down", *args] if method == "rollback" else args))

    assert "driver's own debug log" in caplog.text
