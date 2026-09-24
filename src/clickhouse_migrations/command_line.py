"""Compatibility module: the command line lives in ``clickhouse_migrations.cli``.

The ``clickhouse_migrations.command_line:main`` console script and the public
names of the old single module are still importable from here. To patch
something in tests, patch it where it is looked up, in the ``cli`` modules
(e.g. ``clickhouse_migrations.cli.common.create_cluster``).
"""

from clickhouse_migrations import __version__
from clickhouse_migrations.cli.app import SUBCOMMANDS, get_context, main
from clickhouse_migrations.cli.argtypes import (
    SETTING_NAME,
    cast_to_bool,
    log_level,
    migration_log_format,
    parse_setting,
    parse_settings,
    positive_float,
    var_assignment,
)
from clickhouse_migrations.cli.baseline import (
    BASELINE_COMMON_OPTIONS,
    do_baseline,
    run_baseline,
)
from clickhouse_migrations.cli.common import create_cluster
from clickhouse_migrations.cli.diff import DIFF_COMMON_OPTIONS, run_diff
from clickhouse_migrations.cli.down import do_rollback, rollback
from clickhouse_migrations.cli.dump import DUMP_COMMON_OPTIONS, run_dump
from clickhouse_migrations.cli.migrate import (
    do_migrate,
    do_query_applied_migrations,
    migrate,
)
from clickhouse_migrations.cli.new import create_migration
from clickhouse_migrations.cli.options import (
    MIGRATION_VARS_ENV,
    warn_debug_with_substitution,
)
from clickhouse_migrations.cli.render import (
    STATUS_FORMAT_JSON,
    STATUS_FORMAT_TABLE,
    STATUS_FORMATS,
    format_status,
    format_status_json,
)
from clickhouse_migrations.cli.repair import (
    REPAIR_COMMON_OPTIONS,
    do_repair,
    run_repair,
)
from clickhouse_migrations.cli.status import do_status, show_status, status_exit_code
from clickhouse_migrations.cli.unlock import unlock
from clickhouse_migrations.cli.validate import run_validate
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster

__all__ = [
    "__version__",
    "ClickhouseCluster",
    "log_level",
    "migration_log_format",
    "cast_to_bool",
    "STATUS_FORMAT_TABLE",
    "STATUS_FORMAT_JSON",
    "STATUS_FORMATS",
    "SUBCOMMANDS",
    "SETTING_NAME",
    "var_assignment",
    "MIGRATION_VARS_ENV",
    "positive_float",
    "parse_setting",
    "parse_settings",
    "DUMP_COMMON_OPTIONS",
    "BASELINE_COMMON_OPTIONS",
    "REPAIR_COMMON_OPTIONS",
    "DIFF_COMMON_OPTIONS",
    "get_context",
    "create_cluster",
    "do_migrate",
    "do_query_applied_migrations",
    "do_status",
    "do_rollback",
    "do_baseline",
    "do_repair",
    "format_status",
    "format_status_json",
    "status_exit_code",
    "warn_debug_with_substitution",
    "migrate",
    "show_status",
    "run_baseline",
    "run_repair",
    "rollback",
    "unlock",
    "create_migration",
    "run_dump",
    "run_validate",
    "run_diff",
    "main",
]
