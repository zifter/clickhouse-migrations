from pathlib import Path

from clickhouse_migrations.migration import Migration, MigrationStorage

TESTS_DIR = Path(__file__).parent
MIGRATIONS = MigrationStorage(TESTS_DIR / "migrations").migrations()


def test_apply_new_migration_ok(cluster):
    cluster.init_schema()

    with cluster.connection() as conn:
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
