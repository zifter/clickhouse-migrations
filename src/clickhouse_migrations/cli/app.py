"""The top-level parser, ``get_context()`` and the ``main()`` dispatch."""

import logging
import sys
from argparse import ArgumentParser

from clickhouse_migrations import __version__
from clickhouse_migrations.cli import (
    baseline,
    common,
    diff,
    down,
    dump,
    migrate,
    new,
    options,
    repair,
    status,
    unlock,
    validate,
    version,
)
from clickhouse_migrations.exceptions import MigrationException

SUBCOMMANDS = (
    "migrate",
    "status",
    "down",
    "new",
    "dump",
    "unlock",
    "baseline",
    "repair",
    "validate",
    "diff",
    "version",
)

# The subcommand modules, in the order --help lists them.
COMMANDS = (
    migrate,
    status,
    down,
    unlock,
    new,
    dump,
    baseline,
    repair,
    validate,
    diff,
    version,
)


def build_parser():
    """The ``clickhouse-migrations`` parser and its subparsers action."""
    parser = ArgumentParser(prog="clickhouse-migrations")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command")
    for command in COMMANDS:
        command.add_parser(subparsers)
    return parser, subparsers


def get_context(args):
    parser, subparsers = build_parser()

    # Default to the "migrate" subcommand so existing invocations
    # (clickhouse-migrations <flags>) keep working unchanged.
    args = list(args)
    if not args or (
        args[0] not in SUBCOMMANDS and args[0] not in ("-h", "--help", "--version")
    ):
        args = ["migrate", *args]

    ctx = parser.parse_args(args)
    command_parser = subparsers.choices[ctx.command]
    if ctx.command == "migrate" and ctx.to_version is not None and ctx.migrations:
        command_parser.error("--to cannot be combined with --migrations")
    if hasattr(ctx, "settings"):
        # Every subcommand that connects.
        common.resolve_settings(command_parser, ctx)
    if ctx.command in ("migrate", "down"):
        options.resolve_var_arguments(command_parser, ctx)
    if ctx.command == "repair" and ctx.prune and not ctx.write:
        command_parser.error("--prune requires --write")

    return ctx


def main() -> int:
    ctx = get_context(sys.argv[1:])
    if ctx.command == "version":
        return version.show_version()
    code = 0
    try:
        if ctx.command == "new":
            new.create_migration(ctx)
        elif ctx.command == "status":
            code = status.show_status(ctx)
        elif ctx.command == "dump":
            code = dump.run_dump(ctx)
        elif ctx.command == "validate":
            code = validate.run_validate(ctx)
        elif ctx.command == "diff":
            code = diff.run_diff(ctx)
        elif ctx.command == "down":
            down.rollback(ctx)
        elif ctx.command == "unlock":
            code = unlock.unlock(ctx)
        elif ctx.command == "baseline":
            code = baseline.run_baseline(ctx)
        elif ctx.command == "repair":
            code = repair.run_repair(ctx)
        else:
            migrate.migrate(ctx)
    except MigrationException as exc:
        logging.error("Migration failed: %s", exc)
        code = 1
    return code
