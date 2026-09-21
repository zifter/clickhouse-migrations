import re
import sys

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.schema_dump import KIND_STRING, tokenize

REPLAY_DB = "dump_replay"
CLUSTER = "company_cluster"

# A realistic schema. Everything is qualified with the "pytest" database on
# purpose: the dump must drop the qualifier, and must NOT touch the "pytest"
# that sits inside string literals and comments.
SCHEMA = [
    """CREATE TABLE pytest.events (
        id UInt64,
        ts DateTime CODEC(Delta, ZSTD),
        name String COMMENT 'pytest.events "quoted" it''s',
        v UInt32,
        INDEX ix_v v TYPE minmax GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    TTL ts + INTERVAL 1 YEAR
    COMMENT 'Events of pytest.events'""",
    """CREATE TABLE pytest.with_projection (
        id UInt64,
        v UInt32,
        PROJECTION by_id (SELECT id, sum(v) GROUP BY id)
    ) ENGINE = MergeTree ORDER BY id""",
    """CREATE TABLE pytest.latest (id UInt64, ver UInt32) ENGINE = ReplacingMergeTree(ver)
    ORDER BY id""",
    """CREATE TABLE pytest.totals (id UInt64, s UInt64) ENGINE = SummingMergeTree ORDER BY id""",
    f"""CREATE TABLE pytest.replicated ON CLUSTER {CLUSTER} (id UInt64, ver UInt32)
    ENGINE = ReplicatedReplacingMergeTree(ver) ORDER BY id""",
    f"""CREATE TABLE pytest.dist (id UInt64)
    ENGINE = Distributed('{CLUSTER}', 'pytest', 'events', rand())""",
    "CREATE TABLE pytest.merged (id UInt64) ENGINE = Merge('pytest', '^events$')",
    "CREATE TABLE pytest.buffered (id UInt64, s UInt64) ENGINE = "
    "Buffer('pytest', 'totals', 1, 10, 100, 10000, 1000000, 10000000, 100000000)",
    """CREATE MATERIALIZED VIEW pytest.mv_totals TO pytest.totals AS
    SELECT id, sum(v) AS s FROM pytest.events WHERE name != 'pytest.events' GROUP BY id""",
    """CREATE MATERIALIZED VIEW pytest.mv_inner ENGINE = MergeTree ORDER BY id AS
    SELECT id FROM pytest.events""",
    "CREATE VIEW pytest.v_events AS SELECT id, name FROM pytest.events",
    # Created before what it reads from, alphabetically first: order must
    # still follow the dependencies.
    "CREATE VIEW pytest.a_over_view AS SELECT * FROM pytest.v_events AS a "
    "INNER JOIN pytest.totals AS t ON a.id = t.id",
    """CREATE DICTIONARY pytest.names (id UInt64, name String) PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'events' USER 'default'))
    LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)""",
    """CREATE DICTIONARY pytest.names_explicit (id UInt64, name String) PRIMARY KEY id
    SOURCE(CLICKHOUSE(DB 'pytest' TABLE 'events' USER 'default'))
    LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)""",
]

# Objects that must come after the ones they read from.
ORDER_CONSTRAINTS = [
    ("events", "mv_totals"),
    ("totals", "mv_totals"),
    ("events", "mv_inner"),
    ("events", "v_events"),
    ("v_events", "a_over_view"),
    ("totals", "a_over_view"),
    ("events", "names"),
    ("events", "names_explicit"),
]


def _run(conn, statement):
    conn.command(statement)


@pytest.fixture(name="source")
def source_schema(cluster, _schema):
    """The schema above in the ``pytest`` database (created on every node)."""
    with cluster.connection("pytest") as conn:
        for statement in SCHEMA:
            _run(conn, statement)
    return cluster


@pytest.fixture(name="replay_db", autouse=True)
def replay_database(cluster):
    def drop():
        with cluster.connection("") as conn:
            conn.command(
                f"DROP DATABASE IF EXISTS {REPLAY_DB} ON CLUSTER {CLUSTER} SYNC"
            )

    drop()
    yield REPLAY_DB
    drop()


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


def _statements(dump):
    return dump.split(";\n\n")


def _position(dump, name):
    match = re.search(
        rf"^CREATE (?:TABLE|VIEW|MATERIALIZED VIEW|DICTIONARY) {name}(?=\s|$)",
        dump,
        flags=re.MULTILINE,
    )
    assert match, f"{name} is not in the dump:\n{dump}"
    return match.start()


def _replay(cluster, dump):
    """Run a dump into an empty database, the way a user would."""
    with cluster.connection("") as conn:
        conn.command(f"CREATE DATABASE {REPLAY_DB} ON CLUSTER {CLUSTER}")
    with cluster.connection(REPLAY_DB) as conn:
        for statement in _statements(dump):
            statement = statement.rstrip().rstrip(";")
            if "Replicated" in statement:
                # ReplicatedMergeTree without arguments uses the {uuid} macro,
                # which the server only allows ON CLUSTER.
                statement = re.sub(
                    r"^(CREATE TABLE \S+)",
                    rf"\1 ON CLUSTER {CLUSTER}",
                    statement,
                )
            conn.command(statement)


@pytest.mark.usefixtures("source")
def test_dump_content_is_portable(driver_cluster):
    dump = driver_cluster.dump("pytest")

    code_only = "".join(t.text for t in tokenize(dump) if t.kind != KIND_STRING)
    assert "pytest." not in code_only
    assert "UUID" not in dump
    assert dump.endswith(";\n")
    assert not dump.endswith("\n\n")
    # String literals and comments keep the database name.
    assert "WHERE name != 'pytest.events'" in dump
    assert "COMMENT 'Events of pytest.events'" in dump
    assert "COMMENT 'pytest.events \"quoted\" it\\'s'" in dump
    # Replicated paths are gone, the engine argument that is not a path stays.
    assert "ENGINE = ReplicatedReplacingMergeTree(ver)" in dump
    assert "/clickhouse/tables" not in dump
    assert "Distributed('company_cluster', currentDatabase(), 'events', rand())" in dump
    assert "ENGINE = Merge(currentDatabase(), '^events$')" in dump
    assert "ENGINE = Buffer(currentDatabase(), 'totals', 1, 10" in dump
    assert "CREATE MATERIALIZED VIEW mv_totals TO totals" in dump
    assert "FROM events" in dump
    # A local dictionary source does not pin the database...
    assert "SOURCE(CLICKHOUSE(TABLE 'events' USER 'default'))" in dump
    # ...and the storage of a materialized view without TO is not dumped.
    assert ".inner" not in dump
    assert "schema_versions" not in dump
    assert "TTL ts + toIntervalYear(1)" in dump
    assert "PROJECTION by_id" in dump
    assert "CODEC(Delta(4), ZSTD(1))" in dump


@pytest.mark.usefixtures("source")
def test_dump_orders_by_dependency(driver_cluster):
    dump = driver_cluster.dump("pytest")

    for first, second in ORDER_CONSTRAINTS:
        assert _position(dump, first) < _position(dump, second), (first, second)
    # Everything else is alphabetical: deterministic output.
    assert dump == driver_cluster.dump("pytest")


@pytest.mark.usefixtures("source")
def test_dump_keep_replicated_paths(driver_cluster):
    dump = driver_cluster.dump("pytest", keep_replicated_paths=True)

    assert (
        "ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ver)"
        in dump
    )


@pytest.mark.usefixtures("source")
def test_dump_tables_filter(driver_cluster, caplog):
    dump = driver_cluster.dump("pytest", tables=["events", "v_events", "mv_totals"])

    assert [_position(dump, n) for n in ("events", "mv_totals", "v_events")] == sorted(
        _position(dump, n) for n in ("events", "mv_totals", "v_events")
    )
    assert "CREATE TABLE totals" not in dump
    assert "mv_totals depends on totals, which is not part of the dump" in caplog.text


def test_dump_unknown_table(source):
    with pytest.raises(MigrationException, match="nope"):
        source.dump("pytest", tables=["events", "nope"])


def test_dump_migrations_table(source):
    source.init_schema("pytest")

    assert "schema_versions" not in source.dump("pytest")
    assert "CREATE TABLE schema_versions" in source.dump(
        "pytest", include_migrations_table=True
    )


def test_dump_missing_database_is_read_only(cluster):
    with pytest.raises(MigrationException, match="does not exist"):
        cluster.dump("dump_no_such_database")

    with cluster.connection("") as conn:
        assert not conn.query(
            "SELECT 1 FROM system.databases WHERE name = 'dump_no_such_database'"
        )


def test_dump_replay_gives_identical_dump(source, driver_cluster):
    dump = driver_cluster.dump("pytest")

    _replay(source, dump)

    assert driver_cluster.dump(REPLAY_DB) == dump


def test_replayed_dictionary_reads_its_own_database(source):
    dump = source.dump("pytest")
    _replay(source, dump)
    with source.connection("pytest") as conn:
        conn.command("INSERT INTO events (id, ts, name) VALUES (1, now(), 'source')")
    with source.connection(REPLAY_DB) as conn:
        conn.command("INSERT INTO events (id, ts, name) VALUES (1, now(), 'replayed')")
        conn.command("SYSTEM RELOAD DICTIONARY names")
        rows = conn.query("SELECT dictGet('names', 'name', toUInt64(1)) AS name")

    assert rows == [{"name": "replayed"}]


def test_replayed_distributed_points_to_its_own_database(source):
    _replay(source, source.dump("pytest"))

    with source.connection(REPLAY_DB) as conn:
        rows = conn.query(
            "SELECT create_table_query FROM system.tables "
            f"WHERE database = '{REPLAY_DB}' AND name = 'dist'"
        )

    assert f"'{CLUSTER}', '{REPLAY_DB}', 'events'" in rows[0]["create_table_query"]


def _cli(monkeypatch, capsys, *args):
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", "dump", *args])
    code = main()
    captured = capsys.readouterr()
    return code, captured.out, captured.err


@pytest.mark.parametrize("driver", ["clickhouse-driver", "clickhouse-connect"])
def test_cli_dump_prints_only_sql_to_stdout(source, monkeypatch, capsys, driver):
    code, out, err = _cli(
        monkeypatch, capsys, "--db-name", "pytest", "--driver", driver
    )

    assert code == 0
    assert out == source.dump("pytest")
    assert err == ""


def test_cli_dump_with_db_url(source, monkeypatch, capsys):
    code, out, _ = _cli(
        monkeypatch, capsys, "--db-url", "clickhouse://default:@localhost:9000/pytest"
    )

    assert code == 0
    assert out == source.dump("pytest")


def test_cli_dump_out_then_check(source, tmp_path, monkeypatch, capsys):
    target = tmp_path / "schema.sql"

    code, out, err = _cli(
        monkeypatch, capsys, "--db-name", "pytest", "--out", str(target)
    )
    assert (code, out) == (0, "")
    assert "Wrote the schema of pytest" in err
    assert target.read_text(encoding="utf8") == source.dump("pytest")

    code, out, err = _cli(
        monkeypatch, capsys, "--db-name", "pytest", "--check", str(target)
    )
    assert (code, out) == (0, "")
    assert "matches" in err


def test_cli_check_detects_drift(source, tmp_path, monkeypatch, capsys):
    target = tmp_path / "schema.sql"
    target.write_text(source.dump("pytest"), encoding="utf8")
    with source.connection("pytest") as conn:
        conn.command("ALTER TABLE events ADD COLUMN extra_col String")

    code, out, err = _cli(
        monkeypatch, capsys, "--db-name", "pytest", "--check", str(target)
    )

    assert code == 1
    assert out == ""
    assert "+    `extra_col` String" in err
    assert f"--- {target}" in err
    assert "+++ database pytest" in err
    assert "Schema drift" in err


@pytest.mark.usefixtures("source")
def test_cli_check_missing_file(tmp_path, monkeypatch, capsys, caplog):
    code, out, _ = _cli(
        monkeypatch,
        capsys,
        "--db-name",
        "pytest",
        "--check",
        str(tmp_path / "missing.sql"),
    )

    assert (code, out) == (1, "")
    assert "Cannot read the schema file" in caplog.text


def test_cli_missing_database(monkeypatch, capsys, caplog):
    code, out, _ = _cli(monkeypatch, capsys, "--db-name", "dump_no_such_database")

    assert (code, out) == (1, "")
    assert "Dump failed: Database 'dump_no_such_database' does not exist" in caplog.text


@pytest.mark.usefixtures("source")
def test_cli_tables_filter(monkeypatch, capsys):
    code, out, _ = _cli(
        monkeypatch, capsys, "--db-name", "pytest", "--tables", "events", "totals"
    )

    assert code == 0
    assert _statements(out)[0].startswith("CREATE TABLE events")
    assert len(_statements(out)) == 2
