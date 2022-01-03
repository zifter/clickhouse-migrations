import tempfile
from pathlib import Path

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.cmd import get_context, migrate
from clickhouse_migrations.types import Migration

TESTS_DIR = Path(__file__).parent


@pytest.fixture
def cluster():
    return ClickhouseCluster("localhost", "default", "")


@pytest.fixture(autouse=True)
def before(cluster):
    clean_slate(cluster)


def clean_slate(migrator):
    with migrator.connection("") as conn:
        conn.execute("DROP DATABASE IF EXISTS pytest")
        conn.execute("CREATE DATABASE pytest")


def test_empty_list_of_migrations_ok(cluster):
    with tempfile.TemporaryDirectory("empty_dir") as d:
        applied = cluster.migrate("pytest", d)

        assert len(applied) == 0


def test_deleted_migrations_exception(cluster):
    cluster.init_schema("pytest")

    with cluster.connection("pytest") as conn:
        conn.execute(
            "INSERT INTO schema_versions(version, script, md5) VALUES",
            [{"version": 1, "script": "location_to_script", "md5": "1234"}],
        )

    with pytest.raises(AssertionError):
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

    with pytest.raises(AssertionError):
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

    with pytest.raises(AssertionError):
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
    clean_slate(cluster)

    with tempfile.TemporaryDirectory("empty_dir") as d:
        cluster.migrate("pytest", d)


def test_main_pass_db_name_ok():
    migrate(
        get_context(
            ["--db-name", "pytest", "--migrations-dir", str(TESTS_DIR / "migrations")]
        )
    )
