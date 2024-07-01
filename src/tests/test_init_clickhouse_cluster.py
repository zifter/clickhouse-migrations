from pathlib import Path

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.migration import Migration, MigrationStorage

TESTS_DIR = Path(__file__).parent
MIGRATIONS = MigrationStorage(TESTS_DIR / "migrations").migrations()


@pytest.fixture
def cluster():
    return ClickhouseCluster(db_url="clickhouse://default:@localhost:9000/pytest")


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
