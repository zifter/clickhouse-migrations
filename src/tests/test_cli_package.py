from pathlib import Path

from clickhouse_migrations import __version__, cli, command_line
from clickhouse_migrations.cli import app, baseline, common, render, repair, status

ROOT = Path(__file__).parents[2]

# Every name the single-module command_line.py had before the split.
OLD_NAMES = """
argparse json logging math os re sys types ArgumentParser Path List Optional
Tuple ClickhouseCluster CLICKHOUSE_DRIVER DRIVERS DB_HOST DB_PASSWORD DB_USER
LOCK_TIMEOUT LOCK_TTL MIGRATIONS_DIR MIGRATIONS_TABLE MIGRATIONS_TABLE_ENGINE
MigrationException Migration MigrationStorage slugify MIGRATION_LOG_FORMATS
STATUS_MD5_MISMATCH STATUS_PENDING STATUS_UNKNOWN Migrator StatusRow
write_diff_migration diff_dumps write_text_atomic parse_assignment
format_validate format_validate_json validate_exit_code validate_migrations
log_level migration_log_format cast_to_bool STATUS_FORMAT_TABLE
STATUS_FORMAT_JSON STATUS_FORMATS SUBCOMMANDS SETTING_NAME _add_lock_arguments
var_assignment MIGRATION_VARS_ENV _add_substitution_arguments
_resolve_var_arguments _add_common_arguments positive_float parse_setting
parse_settings _add_transport_arguments _add_migrate_arguments
_add_migrate_target_argument _add_status_arguments _add_format_argument
_add_down_arguments _add_new_arguments _add_validate_arguments
DUMP_COMMON_OPTIONS _only_options BASELINE_COMMON_OPTIONS REPAIR_COMMON_OPTIONS
_add_baseline_arguments _add_repair_arguments _add_dump_arguments
DIFF_COMMON_OPTIONS _add_diff_arguments get_context create_cluster do_migrate
do_query_applied_migrations do_status do_rollback do_baseline do_repair
format_status _isoformat format_status_json status_exit_code
warn_debug_with_substitution migrate show_status _print_rows run_baseline
run_repair rollback unlock create_migration run_dump run_validate
_generate_diff run_diff main __version__
""".split()

COMMON = [
    "--db-url",
    "--db-host",
    "--db-port",
    "--driver",
    "--db-user",
    "--db-password",
    "--db-name",
    "--migrations-dir",
    "--cluster-name",
    "--migrations-table",
    "--migrations-table-engine",
    "--log-level",
    "--secure/--no-secure",
    "--migrations",
    "--ca-cert",
    "--cert",
    "--key",
    "--verify/--no-verify",
    "--connect-timeout",
    "--query-timeout",
    "--setting",
]
# The connection options of dump/diff/baseline/repair (no --migrations & co.).
CONNECTION = [
    "--db-url",
    "--db-host",
    "--db-port",
    "--driver",
    "--db-user",
    "--db-password",
    "--db-name",
]
TRANSPORT = [
    "--log-level",
    "--secure/--no-secure",
    "--ca-cert",
    "--cert",
    "--key",
    "--verify/--no-verify",
    "--connect-timeout",
    "--query-timeout",
    "--setting",
]
LOCK = ["--lock/--no-lock", "--lock-timeout", "--lock-ttl"]
SUBSTITUTION = ["--var", "--substitute-env/--no-substitute-env"]

EXPECTED_OPTIONS = {
    "migrate": [
        *COMMON,
        "--multi-statement/--no-multi-statement",
        "--migration-log-format",
        "--dry-run/--no-dry-run",
        "--fake/--no-fake",
        "--create-db-if-not-exists/--no-create-db-if-not-exists",
        "--to",
        *LOCK,
        *SUBSTITUTION,
    ],
    "status": [
        *COMMON,
        "--strict/--no-strict",
        "--exit-code-pending/--no-exit-code-pending",
        "--format",
    ],
    "down": [
        *COMMON,
        "--steps",
        "--to",
        "--dry-run/--no-dry-run",
        "--multi-statement/--no-multi-statement",
        *LOCK,
        *SUBSTITUTION,
    ],
    "unlock": COMMON,
    "new": ["name", "--dir/--migrations-dir", "--down", "--version"],
    "dump": [
        *CONNECTION,
        "--migrations-table",
        *TRANSPORT,
        "--tables",
        "--keep-replicated-paths/--no-keep-replicated-paths",
        "--include-migrations-table/--no-include-migrations-table",
        "--out",
        "--check",
    ],
    "baseline": [
        *CONNECTION,
        "--migrations-dir",
        "--cluster-name",
        "--migrations-table",
        "--migrations-table-engine",
        *TRANSPORT,
        "--to",
        "--dry-run/--no-dry-run",
        "--create-db-if-not-exists/--no-create-db-if-not-exists",
        "--format",
        *LOCK,
    ],
    "repair": [
        *CONNECTION,
        "--migrations-dir",
        "--migrations-table",
        *TRANSPORT,
        "--write",
        "--version",
        "--prune",
        "--format",
        *LOCK,
    ],
    "validate": [
        "--dir/--migrations-dir",
        "--strict/--no-strict",
        "--require-down/--no-require-down",
        "--format",
    ],
    "diff": [
        *CONNECTION,
        "--migrations-dir",
        "--migrations-table",
        *TRANSPORT,
        "--to",
        "--name",
        "--allow-destructive/--no-allow-destructive",
        "--dry-run/--no-dry-run",
    ],
    "version": [],
}


def _options(parser):
    # pylint: disable=protected-access
    return [
        "/".join(action.option_strings) or action.dest
        for action in parser._actions
        if action.dest != "help"
    ]


def test_command_line_keeps_every_old_name():
    assert [name for name in OLD_NAMES if not hasattr(command_line, name)] == []
    # No __all__ before: "import *" gave every public name, and still does.
    assert sorted(command_line.__all__) == sorted(
        name for name in OLD_NAMES if not name.startswith("_")
    )


def test_command_line_names_are_the_cli_objects():
    assert command_line.main is app.main
    assert command_line.get_context is app.get_context
    assert command_line.create_cluster is common.create_cluster
    assert command_line.show_status is status.show_status
    # The old private names are set dynamically, hence getattr.
    assert getattr(command_line, "_only_options") is common.only_options
    assert getattr(command_line, "_print_rows") is render.print_rows
    assert getattr(command_line, "__version__") == __version__


def test_cli_modules_are_the_whole_package():
    files = sorted(
        path.stem
        for path in Path(cli.__file__).parent.glob("*.py")
        if path.stem != "__init__"
    )

    assert [module.__name__ for module in command_line.CLI_MODULES] == [
        f"clickhouse_migrations.cli.{name}" for name in files
    ]


def test_package_and_console_script_are_declared():
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert '"clickhouse_migrations.cli"' in pyproject
    assert 'clickhouse-migrations = "clickhouse_migrations.command_line:main"' in (
        pyproject
    )


def test_patching_command_line_reaches_the_cli_modules(monkeypatch):
    original = common.create_cluster
    fake = object()

    monkeypatch.setattr(command_line, "create_cluster", fake)
    assert common.create_cluster is fake

    monkeypatch.undo()
    assert common.create_cluster is original
    assert command_line.create_cluster is original


def test_patching_an_old_private_name_reaches_its_new_name(monkeypatch):
    original = render.print_rows
    fake = object()

    monkeypatch.setattr(command_line, "_print_rows", fake)
    assert render.print_rows is fake
    assert baseline.print_rows is fake
    assert repair.print_rows is fake

    monkeypatch.undo()
    assert render.print_rows is original
    assert baseline.print_rows is original


def test_new_attribute_on_command_line_is_not_forwarded(monkeypatch):
    monkeypatch.setattr(command_line, "brand_new_name", 1, raising=False)

    assert getattr(command_line, "brand_new_name") == 1
    assert not any(
        hasattr(module, "brand_new_name") for module in command_line.CLI_MODULES
    )


def test_subcommands_and_their_options_are_unchanged():
    parser, subparsers = app.build_parser()

    assert _options(parser) == ["--version", "command"]
    assert list(subparsers.choices) == list(EXPECTED_OPTIONS)
    assert sorted(subparsers.choices) == sorted(app.SUBCOMMANDS)
    assert {
        name: _options(subparser) for name, subparser in subparsers.choices.items()
    } == EXPECTED_OPTIONS
