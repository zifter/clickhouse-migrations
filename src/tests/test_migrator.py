import pytest

from clickhouse_migrations.migration import Migration
from clickhouse_migrations.migrator import Migrator


def test_split_statements_with_multi_line_ok():
    script = """create table test
    (
        `app_id`     Int16
    )
        ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)
            PRIMARY KEY app_id
            ORDER BY (app_id, created_at)
            SETTINGS index_granularity = 8192;

    create table refund
    (
        `app_id`       Int16
    )
        ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)
            PRIMARY KEY app_id
            ORDER BY (app_id, created_at)
            SETTINGS index_granularity = 8192;
    """

    statemets = Migrator.script_to_statements(script, True)

    assert len(statemets) == 2
    assert statemets[0][-1] == ";"


def test_split_and_ignore_empy_ok():
    script = """create table test
    (
        `app_id`     Int16
    )
        ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)
            PRIMARY KEY app_id
            ORDER BY (app_id, created_at)
            SETTINGS index_granularity = 8192; ;;;
    """

    statemets = Migrator.script_to_statements(script, True)

    assert len(statemets) == 1
    assert statemets[0][-1] == ";"


def test_full_migration_log_format_keeps_previous_behavior():
    migration = Migration(
        version=1,
        md5="abc",
        script="CREATE TABLE test(value String)",
    )
    migrator = Migrator(None)

    assert migrator.format_migration_log(migration) == str(migration)


def test_compact_migration_log_format_does_not_include_script():
    migration = Migration(
        version=1,
        md5="abc",
        script="CREATE TABLE test(value String)",
    )
    migrator = Migrator(None, migration_log_format="compact")

    assert migrator.format_migration_log(migration) == "version=1, md5=abc"


def test_unknown_migration_log_format_raises_error():
    with pytest.raises(ValueError):
        Migrator(None, migration_log_format="unknown")
