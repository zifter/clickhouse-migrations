import logging
import re
import sys

import pytest

from clickhouse_migrations import command_line
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import get_context, main
from clickhouse_migrations.connection import Connection
from clickhouse_migrations.exceptions import MigrationException

# ---- CLI parsing -----------------------------------------------------------


def test_dump_defaults(monkeypatch):
    for name in ("DUMP_TABLES", "KEEP_REPLICATED_PATHS", "INCLUDE_MIGRATIONS_TABLE"):
        monkeypatch.delenv(name, raising=False)

    ctx = get_context(["dump", "--db-name", "x"])

    assert ctx.command == "dump"
    assert ctx.db_name == "x"
    assert ctx.tables == []
    assert ctx.keep_replicated_paths is False
    assert ctx.include_migrations_table is False
    assert ctx.out is None and ctx.check is None
    assert ctx.migrations_table == "schema_versions"


def test_dump_flags_and_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DUMP_TABLES", "a,b")
    monkeypatch.setenv("KEEP_REPLICATED_PATHS", "1")
    monkeypatch.setenv("INCLUDE_MIGRATIONS_TABLE", "yes")
    ctx = get_context(["dump"])
    assert (ctx.tables, ctx.keep_replicated_paths, ctx.include_migrations_table) == (
        ["a", "b"],
        True,
        True,
    )

    ctx = get_context(
        [
            "dump",
            "--tables",
            "c",
            "d",
            "--no-keep-replicated-paths",
            "--no-include-migrations-table",
            "--out",
            str(tmp_path / "s.sql"),
        ]
    )
    assert ctx.tables == ["c", "d"]
    assert ctx.keep_replicated_paths is False
    assert ctx.include_migrations_table is False
    assert ctx.out == tmp_path / "s.sql"

    assert get_context(["dump", "--check", "f.sql"]).check.name == "f.sql"


def test_dump_out_and_check_are_mutually_exclusive(capsys):
    with pytest.raises(SystemExit) as excinfo:
        get_context(["dump", "--out", "a.sql", "--check", "b.sql"])

    assert excinfo.value.code == 2
    assert "not allowed with argument" in capsys.readouterr().err


def test_dump_takes_no_migrate_only_options(capsys):
    for flag in ("--dry-run", "--migrations-dir=x", "--cluster-name=c", "--fake"):
        with pytest.raises(SystemExit):
            get_context(["dump", flag])
    capsys.readouterr()


def test_dump_takes_connection_options():
    ctx = get_context(
        [
            "dump",
            "--db-host",
            "h",
            "--db-port",
            "1",
            "--db-user",
            "u",
            "--db-password",
            "p",
            "--driver",
            "clickhouse-connect",
            "--secure",
            "--db-url",
            "clickhouse://u:p@h:1/db",
            "--migrations-table",
            "meta.versions",
            "--log-level",
            "info",
        ]
    )

    assert ctx.driver == "clickhouse-connect"
    assert ctx.secure is True
    assert ctx.migrations_table == "meta.versions"
    assert ctx.db_url == "clickhouse://u:p@h:1/db"


def test_bare_invocation_still_means_migrate():
    assert get_context([]).command == "migrate"
    assert get_context(["--db-name", "x"]).command == "migrate"


def test_create_cluster_without_engine_option():
    cluster = command_line.create_cluster(get_context(["dump", "--db-name", "x"]))

    assert cluster.migrations_table_engine is None


# ---- run_dump / main -------------------------------------------------------


class _FakeCluster:  # pylint: disable=too-few-public-methods
    default_db_name = "from_url"

    def __init__(self, text="CREATE TABLE a (`id` UInt8) ENGINE = Memory;\n"):
        self.text = text
        self.calls = []

    def dump(self, **kwargs):
        self.calls.append(kwargs)
        return self.text


def _main(monkeypatch, capsys, cluster, *args):
    monkeypatch.setattr(
        "clickhouse_migrations.cli.common.create_cluster", lambda ctx: cluster
    )
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", "dump", *args])
    code = main()
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_main_dump_prints_only_the_sql_to_stdout(monkeypatch, capsys):
    cluster = _FakeCluster()

    code, out, err = _main(
        monkeypatch,
        capsys,
        cluster,
        "--db-name",
        "x",
        "--tables",
        "a",
        "--keep-replicated-paths",
        "--include-migrations-table",
    )

    assert (code, out, err) == (0, cluster.text, "")
    assert cluster.calls == [
        {
            "db_name": "x",
            "tables": ["a"],
            "keep_replicated_paths": True,
            "include_migrations_table": True,
        }
    ]


def test_main_dump_uses_the_database_of_the_url(monkeypatch, capsys):
    cluster = _FakeCluster()

    _main(monkeypatch, capsys, cluster)

    assert cluster.calls[0]["db_name"] == "from_url"


def test_main_dump_out_writes_the_file(monkeypatch, capsys, tmp_path):
    target = tmp_path / "schema.sql"

    code, out, err = _main(monkeypatch, capsys, _FakeCluster(), "--out", str(target))

    assert (code, out) == (0, "")
    assert "Wrote the schema of from_url to" in err
    assert target.read_text(encoding="utf8") == _FakeCluster().text


def test_main_dump_out_unwritable_is_an_error(monkeypatch, capsys, tmp_path, caplog):
    code, out, _ = _main(
        monkeypatch, capsys, _FakeCluster(), "--out", str(tmp_path / "no" / "s.sql")
    )

    assert (code, out) == (1, "")
    assert "Dump failed: Cannot write" in caplog.text


def test_main_dump_check_matches_ignoring_whitespace(monkeypatch, capsys, tmp_path):
    target = tmp_path / "schema.sql"
    target.write_bytes(b"CREATE TABLE a (`id` UInt8) ENGINE = Memory;  \r\n\r\n")

    code, out, err = _main(monkeypatch, capsys, _FakeCluster(), "--check", str(target))

    assert (code, out) == (0, "")
    assert "matches" in err


def test_main_dump_check_drift_prints_a_diff_and_exits_1(monkeypatch, capsys, tmp_path):
    target = tmp_path / "schema.sql"
    target.write_text("CREATE TABLE b (`id` UInt8) ENGINE = Memory;\n", encoding="utf8")

    code, out, err = _main(monkeypatch, capsys, _FakeCluster(), "--check", str(target))

    assert (code, out) == (1, "")
    assert f"--- {target}" in err
    assert "+++ database from_url" in err
    assert "-CREATE TABLE b" in err and "+CREATE TABLE a" in err
    assert "Schema drift" in err


def test_main_dump_check_missing_file_is_an_error_not_a_traceback(
    monkeypatch, capsys, tmp_path, caplog
):
    cluster = _FakeCluster()

    code, out, _ = _main(
        monkeypatch, capsys, cluster, "--check", str(tmp_path / "missing.sql")
    )

    assert (code, out) == (1, "")
    assert "Dump failed: Cannot read the schema file" in caplog.text
    assert not cluster.calls


def test_main_dump_error_from_the_cluster(monkeypatch, capsys, caplog):
    cluster = _FakeCluster()

    def boom(**_):
        raise MigrationException("Database 'x' does not exist.")

    cluster.dump = boom

    code, out, _ = _main(monkeypatch, capsys, cluster)

    assert (code, out) == (1, "")
    assert "Dump failed: Database 'x' does not exist." in caplog.text


# ---- ClickhouseCluster.dump with a scripted connection ----------------------


class _ScriptedConnection(Connection):
    """Answers the queries dump() issues from canned data."""

    def __init__(self, tables, statements, databases=("db",)):
        self.tables = tables
        self.statements = statements
        self.databases = databases
        self.queries = []

    # dump must be read-only: calling either would raise TypeError.
    command = None
    insert = None

    def query(self, statement):
        self.queries.append(statement)
        if "system.databases" in statement:
            return (
                [{"n": 1}] if any(f"'{d}'" in statement for d in self.databases) else []
            )
        if "system.tables" in statement:
            return self.tables
        name = statement.rsplit(".", 1)[1].strip('"')
        return [{"statement": self.statements[name]}]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return None


def _table(name, **extra):
    return {"name": name, **extra}


def _cluster(monkeypatch, conn, **kwargs):
    cluster = ClickhouseCluster(db_name="db", **kwargs)
    monkeypatch.setattr(cluster, "connection", lambda db_name=None: conn)
    return cluster


STATEMENTS = {
    "events": "CREATE TABLE db.events (`id` UInt8) ENGINE = MergeTree ORDER BY id",
    "mv": "CREATE MATERIALIZED VIEW db.mv TO db.totals AS SELECT id FROM db.events",
    "totals": "CREATE TABLE db.totals (`id` UInt8) ENGINE = MergeTree ORDER BY id",
    "names": "CREATE DICTIONARY db.names (`id` UInt8) PRIMARY KEY id "
    "SOURCE(CLICKHOUSE(TABLE 'events')) LAYOUT(FLAT()) LIFETIME(0)",
    "v": "CREATE VIEW db.v AS SELECT * FROM db.mv",
    "schema_versions": "CREATE TABLE db.schema_versions (`v` UInt8) ENGINE = Memory",
    "schema_lock": "CREATE TABLE db.schema_lock (`v` UInt8) ENGINE = Memory",
    "schema_versions_lock": "CREATE TABLE db.schema_versions_lock (`v` UInt8) ENGINE = Memory",
    "other": "CREATE TABLE db.other (`id` UInt8) ENGINE = Memory",
}


def _names(dump):
    return re.findall(
        r"^CREATE (?:MATERIALIZED VIEW|TABLE|VIEW|DICTIONARY) (\S+)", dump, re.MULTILINE
    )


def test_dump_orders_by_parsed_references_without_server_dependencies(monkeypatch):
    rows = [_table(n) for n in ("v", "totals", "names", "mv", "events", "other")]
    conn = _ScriptedConnection(rows, STATEMENTS)

    dump = _cluster(monkeypatch, conn).dump()

    assert _names(dump) == ["events", "names", "other", "totals", "mv", "v"]
    assert dump.endswith(";\n")


def test_dump_uses_server_dependency_columns(monkeypatch):
    rows = [
        _table(
            "a_dep",
            loading_dependencies_database=["db"],
            loading_dependencies_table=["z"],
        ),
        _table(
            "z",
            dependencies_database=["db", "elsewhere"],
            dependencies_table=["b_dep", "x"],
        ),
        _table(
            "b_dep",
            loading_dependencies_database=["elsewhere"],
            loading_dependencies_table=["a_dep"],
        ),
    ]
    statements = {
        "a_dep": "CREATE TABLE db.a_dep (`id` UInt8) ENGINE = Memory",
        "z": "CREATE TABLE db.z (`id` UInt8) ENGINE = Memory",
        "b_dep": "CREATE TABLE db.b_dep (`id` UInt8) ENGINE = Memory",
    }

    dump = _cluster(monkeypatch, _ScriptedConnection(rows, statements)).dump()

    assert _names(dump) == ["z", "a_dep", "b_dep"]


def test_dump_excludes_bookkeeping_and_inner_tables(monkeypatch):
    rows = [
        _table(n)
        for n in (
            "events",
            "schema_versions",
            "schema_lock",
            "schema_versions_lock",
            ".inner_id.1234",
            ".inner.mv",
        )
    ]
    statements = dict(STATEMENTS)
    conn = _ScriptedConnection(rows, statements)

    assert _names(_cluster(monkeypatch, conn).dump()) == ["events"]
    assert _names(_cluster(monkeypatch, conn, migrations_table="db.meta").dump()) == [
        "events",
        "schema_versions",
        "schema_versions_lock",
    ]
    assert _names(
        _cluster(monkeypatch, conn, migrations_table="other_db.meta").dump()
    ) == ["events", "schema_versions", "schema_versions_lock"]
    assert _names(_cluster(monkeypatch, conn).dump(include_migrations_table=True)) == [
        "events",
        "schema_lock",
        "schema_versions",
        "schema_versions_lock",
    ]


def test_dump_tables_filter_warns_about_unlisted_dependencies(monkeypatch, caplog):
    rows = [_table(n) for n in ("events", "mv", "totals", "other")]
    conn = _ScriptedConnection(rows, STATEMENTS)
    caplog.set_level(logging.WARNING)

    dump = _cluster(monkeypatch, conn).dump(tables=["mv", "events", "events"])

    assert _names(dump) == ["events", "mv"]
    assert "mv depends on totals, which is not part of the dump" in caplog.text
    assert "events depends" not in caplog.text


def test_dump_tables_filter_unknown_and_excluded(monkeypatch):
    rows = [_table("events"), _table("schema_versions")]
    cluster = _cluster(monkeypatch, _ScriptedConnection(rows, STATEMENTS))

    with pytest.raises(MigrationException) as excinfo:
        cluster.dump(tables=["schema_versions", "nope", "events"])

    assert "nope" in str(excinfo.value) and "schema_versions" in str(excinfo.value)


def test_dump_detects_cycles(monkeypatch):
    rows = [
        _table(
            "a", loading_dependencies_database=["db"], loading_dependencies_table=["b"]
        ),
        _table(
            "b", loading_dependencies_database=["db"], loading_dependencies_table=["a"]
        ),
    ]
    statements = {
        "a": "CREATE TABLE db.a (`id` UInt8) ENGINE = Memory",
        "b": "CREATE TABLE db.b (`id` UInt8) ENGINE = Memory",
    }

    with pytest.raises(MigrationException, match="a -> b -> a"):
        _cluster(monkeypatch, _ScriptedConnection(rows, statements)).dump()


def test_dump_keeps_replicated_paths_on_request(monkeypatch):
    rows = [_table("r")]
    statements = {
        "r": "CREATE TABLE db.r (`id` UInt8) ENGINE = ReplicatedMergeTree('/p/{uuid}', '{replica}') ORDER BY id"
    }
    conn = _ScriptedConnection(rows, statements)

    assert "ReplicatedMergeTree ORDER BY id" in _cluster(monkeypatch, conn).dump()
    assert "('/p/{uuid}', '{replica}')" in _cluster(monkeypatch, conn).dump(
        keep_replicated_paths=True
    )


def test_dump_of_an_empty_database_is_empty(monkeypatch):
    assert _cluster(monkeypatch, _ScriptedConnection([], {})).dump() == ""


def test_dump_requires_an_existing_database(monkeypatch):
    conn = _ScriptedConnection([], {}, databases=())

    with pytest.raises(MigrationException, match="Database 'db' does not exist"):
        _cluster(monkeypatch, conn).dump()

    assert not any("system.tables" in q for q in conn.queries)


def test_dump_requires_a_database_name():
    with pytest.raises(MigrationException, match="database name is required"):
        ClickhouseCluster().dump()


def test_dump_queries_are_read_only_and_escape_names(monkeypatch):
    conn = _ScriptedConnection(
        [_table("we'ird")],
        {"we'ird": "CREATE TABLE `d'b`.`we'ird` (`i` UInt8) ENGINE = Memory"},
    )
    cluster = ClickhouseCluster(db_name="d'b")
    monkeypatch.setattr(cluster, "connection", lambda db_name=None: conn)
    monkeypatch.setattr(conn, "databases", ("d\\'b",))

    dump = cluster.dump()

    assert "d\\'b" in conn.queries[0]
    assert dump == "CREATE TABLE `we'ird` (`i` UInt8) ENGINE = Memory;\n"
