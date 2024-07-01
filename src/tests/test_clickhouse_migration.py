import tempfile
from pathlib import Path

import pytest
from clickhouse_driver.errors import ServerException

from clickhouse_migrations.command_line import get_context, migrate
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration

TESTS_DIR = Path(__file__).parent


def test_empty_list_of_migrations_ok(cluster):
    with tempfile.TemporaryDirectory("empty_dir") as temp_dir:
        applied = cluster.migrate("pytest", temp_dir)

        assert len(applied) == 0


def test_deleted_migrations_exception(cluster):
    cluster.init_schema("pytest")

    with cluster.connection("pytest") as conn:
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "location_to_script", "md5": "1234"}],
        )

    with pytest.raises(MigrationException):
        cluster.apply_migrations("pytest", [])


def test_missing_migration_exception(cluster):
    cluster.init_schema("pytest")

    with cluster.connection("pytest") as conn:
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "location_to_script", "md5": "1234"}],
        )

    migrations = [
        Migration(version=2, md5="12345", script="location_to_script"),
    ]

    with pytest.raises(MigrationException):
        cluster.apply_migrations("pytest", migrations)


def test_modified_committed_migrations_exception(cluster):
    cluster.init_schema("pytest")

    with cluster.connection("pytest") as conn:
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "location_to_script", "md5": "1234"}],
        )

    migrations = [
        Migration(version=1, md5="12345", script="location_to_script"),
    ]

    with pytest.raises(MigrationException):
        cluster.apply_migrations("pytest", migrations)


def test_apply_new_migration_ok(cluster):
    cluster.init_schema("pytest")

    with cluster.connection("pytest") as conn:
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "SHOW TABLES", "md5": "12345"}],
        )

    migrations = [
        Migration(version=1, md5="12345", script="SHOW TABLES"),
        Migration(version=2, md5="12345", script="SHOW TABLES"),
    ]

    results = cluster.apply_migrations("pytest", migrations)
    assert len(results) == 1
    assert results[0] == migrations[-1]


def test_apply_two_new_migration_ok(cluster):
    cluster.init_schema("pytest")

    with cluster.connection("pytest") as conn:
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "SHOW TABLES", "md5": "111"}],
        )

        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 2, "script": "SHOW TABLES", "md5": "222"}],
        )

    migrations = [
        Migration(version=1, md5="111", script="SHOW TABLES"),
        Migration(version=2, md5="222", script="SHOW TABLES"),
        Migration(version=3, md5="333", script="SHOW TABLES"),
        Migration(version=4, md5="444", script="SHOW TABLES"),
        Migration(version=5, md5="444", script="SHOW TABLES"),
    ]

    results = cluster.apply_migrations("pytest", migrations)
    assert len(results) == 3
    assert results[0] == migrations[-3]
    assert results[1] == migrations[-2]
    assert results[2] == migrations[-1]


def test_should_migrate_empty_database(cluster):
    cluster.create_db("pytest")

    tables = cluster.show_tables("pytest")
    assert len(tables) == 0

    cluster.migrate("pytest", TESTS_DIR / "migrations")

    tables = cluster.show_tables("pytest")
    assert len(tables) == 2
    assert tables[0] == "sample"
    assert tables[1] == "schema_versions"


def test_migrations_folder_is_empty_ok(cluster):
    with tempfile.TemporaryDirectory("empty_dir") as temp_dir:
        cluster.migrate("pytest", temp_dir)


def test_multi_statement_migration_by_default_ok(cluster):
    cluster.migrate("pytest", TESTS_DIR / "multi_statements_migrations")


def test_multi_statement_migration_disabled_ok(cluster):
    with pytest.raises(ServerException):
        cluster.migrate(
            "pytest", TESTS_DIR / "multi_statements_migrations", multi_statement=False
        )


def test_issue_15_ok(cluster):
    cluster.migrate("pytest", TESTS_DIR / "issue_15", multi_statement=True)


def test_complex_ok(cluster):
    cluster.migrate("pytest", TESTS_DIR / "complex_migrations")


def test_main_pass_db_name_ok():
    migrate(
        get_context(
            ["--db-name", "pytest", "--migrations-dir", str(TESTS_DIR / "migrations")]
        )
    )


def test_main_pass_db_url_ok():
    migrate(
        get_context(
            [
                "--db-url",
                "clickhouse://default:@localhost:9000/pytest",
                "--migrations-dir",
                str(TESTS_DIR / "migrations"),
            ]
        )
    )


def test_check_multistatement_arg():
    context = get_context(["--multi-statement", "false"])
    assert context.multi_statement is False

    context = get_context(["--multi-statement", "True"])
    assert context.multi_statement is True

    context = get_context(["--multi-statement", "0"])
    assert context.multi_statement is False


def test_check_explicit_migrations_ok():
    migrate(
        get_context(
            [
                "--migrations",
                "001_init",
                "002",
                "3",
                "--migrations-dir",
                str(TESTS_DIR / "complex_migrations"),
            ]
        )
    )


def test_check_explicit_migrations_args_ok():
    context = get_context(["--migrations", "001_init", "002_test2"])
    assert context.migrations == ["001_init", "002_test2"]


def test_check_fake_ok():
    context = get_context(["--fake"])
    assert context.fake is True
