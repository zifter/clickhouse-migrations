from pathlib import Path

from clickhouse_migrations import __version__, command_line
from clickhouse_migrations.cli import app, common, status

ROOT = Path(__file__).parents[2]

# The public names of the single-module command_line.py before the split that
# the compatibility module keeps (stdlib and private helpers are not kept).
OLD_NAMES = """
__version__ ClickhouseCluster log_level migration_log_format cast_to_bool
STATUS_FORMAT_TABLE STATUS_FORMAT_JSON STATUS_FORMATS SUBCOMMANDS SETTING_NAME
var_assignment MIGRATION_VARS_ENV positive_float parse_setting parse_settings
DUMP_COMMON_OPTIONS BASELINE_COMMON_OPTIONS REPAIR_COMMON_OPTIONS
DIFF_COMMON_OPTIONS get_context create_cluster do_migrate
do_query_applied_migrations do_status do_rollback do_baseline do_repair
format_status format_status_json status_exit_code warn_debug_with_substitution
migrate show_status run_baseline run_repair rollback unlock create_migration
run_dump run_validate run_diff main
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
    assert sorted(command_line.__all__) == sorted(OLD_NAMES)


def test_command_line_names_are_the_cli_objects():
    assert command_line.main is app.main
    assert command_line.get_context is app.get_context
    assert command_line.create_cluster is common.create_cluster
    assert command_line.show_status is status.show_status
    assert command_line.__version__ == __version__


def test_package_and_console_script_are_declared():
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert '"clickhouse_migrations.cli"' in pyproject
    assert 'clickhouse-migrations = "clickhouse_migrations.command_line:main"' in (
        pyproject
    )


def test_subcommands_and_their_options_are_unchanged():
    parser, subparsers = app.build_parser()

    assert _options(parser) == ["--version", "command"]
    assert list(subparsers.choices) == list(EXPECTED_OPTIONS)
    assert sorted(subparsers.choices) == sorted(app.SUBCOMMANDS)
    assert {
        name: _options(subparser) for name, subparser in subparsers.choices.items()
    } == EXPECTED_OPTIONS
