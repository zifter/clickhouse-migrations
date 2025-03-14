from pathlib import Path

from clickhouse_migrations.command_line import get_context

TESTS_DIR = Path(__file__).parent


def test_check_multistatement_arg():
    context = get_context(["--multi-statement"])
    assert context.multi_statement is True

    context = get_context(["--no-multi-statement"])
    assert context.multi_statement is False


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
