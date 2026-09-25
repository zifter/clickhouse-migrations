"""diff against a real server.

The key property: starting from a database in state A and a schema file
written by ``dump`` from a database built to state B, applying the migration
``diff`` generates (with our own ``migrate``) makes ``dump`` of A equal to the
file, i.e. diff + migrate converges A to B.
"""

import sys
import uuid

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.schema_diff import write_diff_migration

TARGET_DB = "diff_target"
# Unique per run: a crashed run can leave replicas behind in ZooKeeper.
ZK = f"/clickhouse/tables/{{shard}}/diff_{uuid.uuid4().hex}"

EVENTS = """CREATE TABLE events (
    id UInt64,
    ts DateTime,
    name String DEFAULT 'none' COMMENT 'event name' CODEC(ZSTD(1)),
    v UInt32
) ENGINE = MergeTree ORDER BY id"""

TOTALS = (
    "CREATE TABLE totals (id UInt64, s UInt64) ENGINE = SummingMergeTree ORDER BY id"
)

# 23.8 refuses the generated statement: "Alter of type 'MODIFY_QUERY' is not
# supported by storage MaterializedView". 24.3 is the oldest version tested OK.
MODIFY_MV_QUERIES = "modify MV queries"
NEEDS_MODIFY_QUERY = pytest.mark.clickhouse_min_version(
    "24.3", reason="ALTER TABLE ... MODIFY QUERY of a materialized view"
)

# Change class -> (state A, state B). Every B is reachable from A in place.
CONVERGING = {
    "create objects in dependency order": (
        [],
        [
            EVENTS,
            TOTALS,
            "CREATE MATERIALIZED VIEW mv_totals TO totals AS "
            "SELECT id, sum(v) AS s FROM events GROUP BY id",
            "CREATE MATERIALIZED VIEW mv_inner ENGINE = MergeTree ORDER BY id AS "
            "SELECT id FROM events",
            "CREATE VIEW a_view AS SELECT id, name FROM events",
            "CREATE DICTIONARY names (id UInt64, name String) PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(TABLE 'events' USER 'default')) "
            "LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)",
        ],
    ),
    "add columns (first, middle, last)": (
        [EVENTS],
        [
            """CREATE TABLE events (
                first_col UInt8,
                id UInt64,
                ts DateTime,
                middle Nullable(String) COMMENT 'm',
                name String DEFAULT 'none' COMMENT 'event name' CODEC(ZSTD(1)),
                v UInt32,
                last_col Float64 DEFAULT v * 2 CODEC(Gorilla, ZSTD)
            ) ENGINE = MergeTree ORDER BY id"""
        ],
    ),
    "modify columns": (
        [EVENTS],
        [
            """CREATE TABLE events (
                id UInt64,
                ts DateTime COMMENT 'now documented',
                name LowCardinality(String),
                v UInt64 MATERIALIZED id + 1 CODEC(LZ4)
            ) ENGINE = MergeTree ORDER BY id"""
        ],
    ),
    "change defaults": (
        [
            "CREATE TABLE d (id UInt64, a UInt8 DEFAULT 1, b UInt8 MATERIALIZED 2, "
            "c UInt8, e UInt8 ALIAS id + 1) ENGINE = MergeTree ORDER BY id"
        ],
        [
            "CREATE TABLE d (id UInt64, a UInt8, b UInt8 DEFAULT 3, "
            "c UInt8 DEFAULT 4, e UInt8 ALIAS id + 2) ENGINE = MergeTree ORDER BY id"
        ],
    ),
    "reorder columns": (
        [EVENTS],
        [
            """CREATE TABLE events (
                v UInt32,
                id UInt64,
                name String DEFAULT 'none' COMMENT 'event name' CODEC(ZSTD(1)),
                ts DateTime
            ) ENGINE = MergeTree ORDER BY id"""
        ],
    ),
    "indices, TTL and table comment": (
        [
            """CREATE TABLE t (id UInt64, ts DateTime, s String,
                INDEX ix_keep id TYPE minmax GRANULARITY 1)
            ENGINE = MergeTree ORDER BY id"""
        ],
        [
            """CREATE TABLE t (id UInt64, ts DateTime, s String,
                INDEX ix_first lower(s) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4,
                INDEX ix_keep id TYPE minmax GRANULARITY 1,
                INDEX ix_last s TYPE bloom_filter GRANULARITY 2)
            ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 YEAR
            COMMENT 'with comment'"""
        ],
    ),
    "change and remove TTL": (
        [
            "CREATE TABLE a (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id "
            "TTL ts + INTERVAL 1 DAY",
            "CREATE TABLE b (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id "
            "TTL ts + INTERVAL 1 DAY COMMENT 'x'",
        ],
        [
            "CREATE TABLE a (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id "
            "TTL ts + INTERVAL 2 DAY",
            "CREATE TABLE b (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id",
        ],
    ),
    "replace views and dictionaries": (
        [
            EVENTS,
            "CREATE VIEW v AS SELECT id FROM events",
            "CREATE DICTIONARY names (id UInt64, name String) PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(TABLE 'events' USER 'default')) "
            "LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)",
        ],
        [
            EVENTS,
            "CREATE VIEW v AS SELECT id, name FROM events WHERE v > 1",
            "CREATE DICTIONARY names (id UInt64, name String DEFAULT '?') PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(TABLE 'events' USER 'default')) "
            "LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 300)",
        ],
    ),
    MODIFY_MV_QUERIES: (
        [
            EVENTS,
            TOTALS,
            "CREATE MATERIALIZED VIEW mv_totals TO totals AS "
            "SELECT id, sum(v) AS s FROM events GROUP BY id",
            "CREATE MATERIALIZED VIEW mv_inner ENGINE = MergeTree ORDER BY id AS "
            "SELECT id FROM events",
        ],
        [
            EVENTS,
            TOTALS,
            "CREATE MATERIALIZED VIEW mv_totals TO totals AS "
            "SELECT id, sum(v) * 2 AS s FROM events WHERE v > 0 GROUP BY id",
            "CREATE MATERIALIZED VIEW mv_inner ENGINE = MergeTree ORDER BY id AS "
            "SELECT id FROM events WHERE id > 10",
        ],
    ),
    "replicated table (compared by engine family)": (
        [
            "CREATE TABLE r (id UInt64, ver UInt32) ENGINE = ReplicatedReplacingMergeTree("
            f"'{ZK}/{{database}}/r', '{{replica}}', ver) ORDER BY id"
        ],
        [
            "CREATE TABLE r (id UInt64, ver UInt32, extra String) ENGINE = "
            f"ReplicatedReplacingMergeTree('{ZK}/{{database}}/r', "
            "'{replica}', ver) ORDER BY id"
        ],
    ),
}

DESTRUCTIVE = {
    "drop objects": (
        [
            EVENTS,
            TOTALS,
            "CREATE MATERIALIZED VIEW mv_totals TO totals AS "
            "SELECT id, sum(v) AS s FROM events GROUP BY id",
            "CREATE VIEW v AS SELECT id FROM events",
            "CREATE DICTIONARY names (id UInt64, name String) PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(TABLE 'events' USER 'default')) "
            "LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)",
        ],
        [EVENTS],
    ),
    "drop columns and indices, replace an index": (
        [
            """CREATE TABLE t (id UInt64, gone String, s String,
                INDEX ix_gone gone TYPE minmax GRANULARITY 1,
                INDEX ix_changed s TYPE minmax GRANULARITY 1)
            ENGINE = MergeTree ORDER BY id"""
        ],
        [
            """CREATE TABLE t (id UInt64, s String,
                INDEX ix_changed s TYPE bloom_filter GRANULARITY 3)
            ENGINE = MergeTree ORDER BY id"""
        ],
    ),
}

REFUSED = {
    "ORDER BY": (
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY (id, ts)",
    ),
    "PARTITION BY": (
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree "
        "PARTITION BY toYYYYMM(ts) ORDER BY id",
    ),
    "PRIMARY KEY": (
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY (id, ts)",
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree "
        "PRIMARY KEY id ORDER BY (id, ts)",
    ),
    "SAMPLE BY": (
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id "
        "SAMPLE BY id",
    ),
    "the engine changes": (
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = ReplacingMergeTree(ts) "
        "ORDER BY id",
    ),
    "SETTINGS change": (
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id "
        "SETTINGS index_granularity = 1024",
    ),
    "a table in the database but a view": (
        "CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id",
        "CREATE VIEW t AS SELECT 1 AS id",
    ),
}


@pytest.fixture(name="target_db", autouse=True)
def target_database(cluster):
    def drop():
        with cluster.connection("") as conn:
            conn.command(f"DROP DATABASE IF EXISTS {TARGET_DB} SYNC")

    drop()
    yield TARGET_DB
    drop()
    assert not _scratch_databases(cluster)


@pytest.fixture(
    name="driver_cluster", params=["clickhouse-driver", "clickhouse-connect"]
)
def driver_cluster_fixture(request):
    return ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        driver=request.param,
    )


def _scratch_databases(cluster):
    with cluster.connection("") as conn:
        return conn.query(
            "SELECT name FROM system.databases WHERE name LIKE '_chm_diff_%'"
        )


def _build(cluster, db_name, statements):
    with cluster.connection("") as conn:
        conn.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    with cluster.connection(db_name) as conn:
        for statement in statements:
            conn.command(statement)


def _schema_file(cluster, tmp_path, statements):
    """``dump`` of a database built to ``statements``, written to a file."""
    _build(cluster, TARGET_DB, statements)
    schema = cluster.dump(TARGET_DB)
    path = tmp_path / "schema.sql"
    path.write_text(schema, encoding="utf8")
    return schema, path


def _apply(cluster, tmp_path, sql):
    migrations = tmp_path / "migrations"
    path = write_diff_migration(migrations, sql)
    cluster.migrate("pytest", migrations)
    return path


@pytest.mark.parametrize(
    "case",
    [
        (
            pytest.param(case, marks=NEEDS_MODIFY_QUERY)
            if case == MODIFY_MV_QUERIES
            else case
        )
        for case in CONVERGING
    ],
)
def test_diff_then_migrate_converges(cluster, tmp_path, case):
    state_a, state_b = CONVERGING[case]
    schema, _ = _schema_file(cluster, tmp_path, state_b)
    _build(cluster, "pytest", state_a)

    sql = cluster.diff(schema, "pytest")
    assert sql.startswith("-- Generated by `clickhouse-migrations diff`")
    assert "_chm_diff_" not in sql and "diff_target" not in sql
    assert "-- DROP" not in sql
    _apply(cluster, tmp_path, sql)

    assert cluster.dump("pytest") == schema
    assert cluster.diff(schema, "pytest") == ""


@pytest.mark.parametrize("case", list(DESTRUCTIVE))
def test_destructive_is_commented_out_by_default(cluster, tmp_path, case):
    state_a, state_b = DESTRUCTIVE[case]
    schema, _ = _schema_file(cluster, tmp_path, state_b)
    _build(cluster, "pytest", state_a)
    before = cluster.dump("pytest")

    sql = cluster.diff(schema, "pytest")
    assert "-- Destructive statements are commented out" in sql
    code = [line for line in sql.splitlines() if line and not line.startswith("--")]
    assert not any("DROP" in line for line in code)
    _apply(cluster, tmp_path, sql)
    # Only the non-destructive part ran.
    assert cluster.dump("pytest") != schema
    assert cluster.dump("pytest") == before

    sql = cluster.diff(schema, "pytest", allow_destructive=True)
    assert "-- Destructive statements are included" in sql
    _apply(cluster, tmp_path, sql)
    assert cluster.dump("pytest") == schema


@pytest.mark.parametrize("case", list(REFUSED))
def test_refused_changes_generate_no_sql(cluster, tmp_path, case):
    state_a, state_b = REFUSED[case]
    schema, _ = _schema_file(cluster, tmp_path, [state_b])
    _build(cluster, "pytest", [state_a])

    plan = cluster.diff_plan(schema, "pytest")

    assert not plan.changes
    # ORDER BY also moves the implicit PRIMARY KEY: one or two reasons.
    assert case in plan.refusals[0]
    sql = plan.render()
    assert all(line.startswith("--") or not line for line in sql.splitlines())
    assert "-- Refused (no SQL generated for these, write them by hand):" in sql


def test_refused_table_does_not_block_other_changes(cluster, tmp_path):
    schema, _ = _schema_file(
        cluster,
        tmp_path,
        [
            "CREATE TABLE t (id UInt64, x UInt8) ENGINE = MergeTree ORDER BY (id, x)",
            "CREATE TABLE u (id UInt64, y UInt8) ENGINE = MergeTree ORDER BY id",
        ],
    )
    _build(
        cluster,
        "pytest",
        [
            "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id",
            "CREATE TABLE u (id UInt64) ENGINE = MergeTree ORDER BY id",
        ],
    )

    plan = cluster.diff_plan(schema, "pytest")

    assert [change.name for change in plan.changes] == ["u"]
    assert "ORDER BY changes from id to id, x" in plan.refusals[0]
    assert "`t`" not in "".join(
        line for line in plan.render().splitlines() if not line.startswith("--")
    )


def test_new_replicated_table_keeps_its_engine(cluster):
    schema = (
        "CREATE TABLE r (id UInt64, ver UInt32)\n"
        "ENGINE = ReplicatedReplacingMergeTree(ver)\nORDER BY id;\n"
    )

    sql = cluster.diff(schema, "pytest")

    assert "ENGINE = ReplicatedReplacingMergeTree(ver)" in sql
    assert "CREATE TABLE r\n" in sql


def test_replicated_to_local_is_refused(cluster, tmp_path):
    schema, _ = _schema_file(
        cluster,
        tmp_path,
        ["CREATE TABLE r (id UInt64) ENGINE = MergeTree ORDER BY id"],
    )
    _build(
        cluster,
        "pytest",
        [
            "CREATE TABLE r (id UInt64) ENGINE = ReplicatedMergeTree("
            f"'{ZK}/{{database}}/r2', '{{replica}}') ORDER BY id"
        ],
    )

    plan = cluster.diff_plan(schema, "pytest")

    assert (
        "the engine changes from ReplicatedMergeTree to MergeTree" in plan.refusals[0]
    )


def test_materialized_view_with_new_target_is_refused(cluster, tmp_path):
    other = "CREATE TABLE other (id UInt64, s UInt64) ENGINE = MergeTree ORDER BY id"
    schema, _ = _schema_file(
        cluster,
        tmp_path,
        [
            EVENTS,
            TOTALS,
            other,
            "CREATE MATERIALIZED VIEW mv TO other AS SELECT id, v AS s FROM events",
        ],
    )
    _build(
        cluster,
        "pytest",
        [
            EVENTS,
            TOTALS,
            other,
            "CREATE MATERIALIZED VIEW mv TO totals AS SELECT id, v AS s FROM events",
        ],
    )

    plan = cluster.diff_plan(schema, "pytest")

    assert not plan.changes
    assert "MODIFY QUERY cannot" in plan.refusals[0]


def test_migrations_and_lock_tables_are_ignored(cluster, tmp_path):
    schema, _ = _schema_file(cluster, tmp_path, [TOTALS])
    _build(cluster, "pytest", [TOTALS])
    cluster.init_schema("pytest")
    with cluster.connection("pytest") as conn:
        conn.command(
            "CREATE TABLE schema_versions_lock (k String, v String) "
            "ENGINE = MergeTree ORDER BY k"
        )

    assert cluster.diff(schema, "pytest") == ""


def test_invalid_schema_file_cleans_up(cluster):
    with pytest.raises(
        MigrationException,
        match=r"statement 2 \(table broken\) failed: .*Syntax error",
    ):
        cluster.diff(
            "CREATE TABLE fine (id UInt64) ENGINE = Memory;\n"
            "CREATE TABLE broken (id UInt64 ENGINE = Memory;\n",
            "pytest",
        )
    assert not _scratch_databases(cluster)
    assert cluster.show_tables("pytest") == []


def test_interrupt_cleans_up(cluster, monkeypatch):
    def interrupted(*_):
        assert len(_scratch_databases(cluster)) == 1
        raise KeyboardInterrupt

    monkeypatch.setattr(ClickhouseCluster, "_replay_target", interrupted)

    with pytest.raises(KeyboardInterrupt):
        cluster.diff("CREATE TABLE t (id UInt64) ENGINE = Memory;", "pytest")
    assert not _scratch_databases(cluster)


def test_missing_database(cluster):
    with pytest.raises(MigrationException, match="does not exist"):
        cluster.diff("", "diff_no_such_database")


def test_diff_requires_a_database_name():
    with pytest.raises(MigrationException, match="database name is required"):
        ClickhouseCluster(db_host="localhost").diff("")


def _cli(monkeypatch, capsys, *args):
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", "diff", *args])
    code = main()
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_cli_writes_the_next_migration(driver_cluster, tmp_path, monkeypatch, capsys):
    _, path = _schema_file(
        driver_cluster, tmp_path, CONVERGING["add columns (first, middle, last)"][1]
    )
    _build(driver_cluster, "pytest", [EVENTS])
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "001_init.sql").write_text("SELECT 1;\n", encoding="utf8")
    driver_cluster.migrate("pytest", migrations)

    code, out, err = _cli(
        monkeypatch,
        capsys,
        "--db-name",
        "pytest",
        "--driver",
        driver_cluster.driver,
        "--to",
        str(path),
        "--migrations-dir",
        str(migrations),
        "--name",
        "add columns",
    )

    created = migrations / "002_add_columns.sql"
    assert (code, out, err) == (0, f"Created {created}\n", "")
    text = created.read_text(encoding="utf8")
    assert text.startswith("-- add columns\n-- created: ")
    assert driver_cluster.diff(path.read_text(encoding="utf8"), "pytest") in text

    driver_cluster.migrate("pytest", migrations)
    code, _, err = _dump_check(monkeypatch, capsys, path)
    assert code == 0, err


def _dump_check(monkeypatch, capsys, path):
    monkeypatch.setattr(
        sys,
        "argv",
        ["clickhouse-migrations", "dump", "--db-name", "pytest", "--check", str(path)],
    )
    code = main()
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_cli_dry_run_prints_only_sql(cluster, tmp_path, monkeypatch, capsys):
    schema, path = _schema_file(cluster, tmp_path, [TOTALS])

    code, out, err = _cli(
        monkeypatch,
        capsys,
        "--db-name",
        "pytest",
        "--to",
        str(path),
        "--migrations-dir",
        str(tmp_path / "migrations"),
        "--dry-run",
    )

    assert (code, err) == (0, "")
    assert out == cluster.diff(schema, "pytest")
    assert not (tmp_path / "migrations").exists()


def test_cli_no_changes_creates_no_file(cluster, tmp_path, monkeypatch, capsys):
    _, path = _schema_file(cluster, tmp_path, [TOTALS])
    _build(cluster, "pytest", [TOTALS])

    code, out, err = _cli(
        monkeypatch,
        capsys,
        "--db-name",
        "pytest",
        "--to",
        str(path),
        "--migrations-dir",
        str(tmp_path / "migrations"),
    )

    assert (code, out) == (0, "")
    assert err == f"No changes: pytest matches {path}\n"
    assert not (tmp_path / "migrations").exists()


def test_cli_refusal_exits_1(cluster, tmp_path, monkeypatch, capsys, caplog):
    state_a, state_b = REFUSED["ORDER BY"]
    _, path = _schema_file(cluster, tmp_path, [state_b, TOTALS])
    _build(cluster, "pytest", [state_a])

    code, out, _ = _cli(
        monkeypatch,
        capsys,
        "--db-name",
        "pytest",
        "--to",
        str(path),
        "--migrations-dir",
        str(tmp_path / "migrations"),
    )

    assert code == 1
    assert out == f"Created {tmp_path / 'migrations' / '001_diff.sql'}\n"
    assert "Refused: table `t`: ORDER BY changes" in caplog.text
    text = (tmp_path / "migrations" / "001_diff.sql").read_text(encoding="utf8")
    assert "CREATE TABLE totals" in text
    assert "--   * table `t`: ORDER BY changes from id to id, ts" in text


def test_cli_everything_refused_creates_no_file(cluster, tmp_path, monkeypatch, capsys):
    state_a, state_b = REFUSED["ORDER BY"]
    _, path = _schema_file(cluster, tmp_path, [state_b])
    _build(cluster, "pytest", [state_a])

    code, out, err = _cli(
        monkeypatch,
        capsys,
        "--db-name",
        "pytest",
        "--to",
        str(path),
        "--migrations-dir",
        str(tmp_path / "migrations"),
    )

    assert (code, out) == (1, "")
    assert "every change was refused" in err
    assert not (tmp_path / "migrations").exists()


def test_cli_destructive_hint(cluster, tmp_path, monkeypatch, capsys):
    _, path = _schema_file(cluster, tmp_path, [TOTALS])
    _build(cluster, "pytest", [TOTALS, EVENTS])

    code, out, err = _cli(
        monkeypatch,
        capsys,
        "--db-name",
        "pytest",
        "--to",
        str(path),
        "--migrations-dir",
        str(tmp_path / "migrations"),
    )

    assert code == 0
    assert out.startswith("Created ")
    assert "Destructive statements are commented out" in err
    text = (tmp_path / "migrations" / "001_diff.sql").read_text(encoding="utf8")
    assert "\n-- DROP TABLE `events`;\n" in text

    # A file of comments only applies as a no-op.
    cluster.migrate("pytest", tmp_path / "migrations")
    assert "events" in cluster.show_tables("pytest")


def test_cli_errors(cluster, tmp_path, monkeypatch, capsys, caplog):
    args = ["--db-name", "pytest", "--migrations-dir", str(tmp_path)]

    code, out, _ = _cli(monkeypatch, capsys, *args, "--to", str(tmp_path / "nope.sql"))
    assert (code, out) == (1, "")
    assert "Cannot read the schema file" in caplog.text

    bad = tmp_path / "bad.sql"
    bad.write_text("INSERT INTO t VALUES (1);\n", encoding="utf8")
    code, out, _ = _cli(monkeypatch, capsys, *args, "--to", str(bad))
    assert (code, out) == (1, "")
    assert "Diff failed: Statement 1 of the schema file is not supported" in caplog.text

    code, out, _ = _cli(monkeypatch, capsys, *args, "--to", str(bad), "--name", "!!")
    assert (code, out) == (1, "")
    assert "Migration name must contain letters or digits" in caplog.text
    assert not _scratch_databases(cluster)
