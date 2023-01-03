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
