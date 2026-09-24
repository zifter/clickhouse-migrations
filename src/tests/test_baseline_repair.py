import json
import sys
import types
from datetime import datetime

import pytest

from clickhouse_migrations import command_line
from clickhouse_migrations.command_line import get_context, main
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import Migration
from clickhouse_migrations.migrator import (
    STATUS_APPLIED,
    STATUS_MD5_MISMATCH,
    STATUS_PENDING,
    STATUS_PRUNED,
    STATUS_UNKNOWN,
    Migrator,
    StatusRow,
)

APPLIED_AT = datetime(2024, 1, 1, 12)


class _BookkeepingConn:
    """In-memory stand-in for the bookkeeping table.

    Understands just enough of the statements the migrator sends (count,
    status query, insert, ALTER ... DELETE) to follow a repair end to end.
    """

    def __init__(self, rows=None):
        # version -> md5 of the stored rows
        self.rows = dict(rows or {})
        self.commands = []
        self.inserts = []

    def query(self, statement):
        if "count()" in statement:
            return [{"n": len(self.rows)}]
        return [
            {"version": v, "md5": md5, "applied_at": APPLIED_AT}
            for v, md5 in sorted(self.rows.items())
        ]

    def insert(self, table, rows):
        self.inserts.append((table, rows))
        for row in rows:
            self.rows[row["version"]] = row["md5"]

    def command(self, statement):
        self.commands.append(statement)
        if "DELETE WHERE" in statement:
            version = int(statement.split("version = ")[1].split()[0])
            if "md5 !=" not in statement:
                del self.rows[version]


def _migs(**md5_by_version):
    return [
        Migration(int(v[1:]), md5, f"SELECT {v[1:]};")
        for v, md5 in md5_by_version.items()
    ]


LOCAL = _migs(v1="a", v2="b", v3="c")


# --- baseline ------------------------------------------------------------


def test_baseline_migrations_selects_up_to_the_target():
    assert [m.version for m in Migrator.baseline_migrations(LOCAL, 2)] == [1, 2]
    assert [m.version for m in Migrator.baseline_migrations(LOCAL, 3)] == [1, 2, 3]


def test_baseline_migrations_rejects_an_unknown_target():
    with pytest.raises(MigrationException, match="Baseline version 4 is not among"):
        Migrator.baseline_migrations(LOCAL, 4)


def test_baseline_records_in_one_insert_without_executing():
    conn = _BookkeepingConn()

    recorded = Migrator(conn, migrations_table="meta.v").baseline(LOCAL, 2)

    assert [m.version for m in recorded] == [1, 2]
    assert not conn.commands
    assert len(conn.inserts) == 1
    assert conn.inserts[0][0] == '"meta"."v"'
    assert conn.inserts[0][1] == [
        {"version": 1, "script": "SELECT 1;", "md5": "a"},
        {"version": 2, "script": "SELECT 2;", "md5": "b"},
    ]


def test_baseline_dry_run_writes_nothing():
    conn = _BookkeepingConn()

    recorded = Migrator(conn, dryrun=True).baseline(LOCAL, 3)

    assert len(recorded) == 3
    assert not conn.inserts


@pytest.mark.parametrize("dryrun", [False, True])
def test_baseline_refuses_a_non_empty_history(dryrun):
    conn = _BookkeepingConn({1: "a"})

    with pytest.raises(MigrationException, match="already records 1 migration"):
        Migrator(conn, dryrun=dryrun).baseline(LOCAL, 2)

    assert not conn.inserts


# --- repair --------------------------------------------------------------


def _status(*rows):
    return [StatusRow(v, state, "x", None) for v, state in rows]


def test_select_repair_rows_keeps_only_out_of_sync():
    rows = _status(
        (1, STATUS_APPLIED),
        (2, STATUS_MD5_MISMATCH),
        (3, STATUS_PENDING),
        (4, STATUS_UNKNOWN),
    )

    assert [r.version for r in Migrator.select_repair_rows(rows)] == [2, 4]
    assert [r.version for r in Migrator.select_repair_rows(rows, [4])] == [4]
    assert [r.version for r in Migrator.select_repair_rows(rows, [4, 2, 2])] == [2, 4]


def test_select_repair_rows_rejects_versions_that_are_not_out_of_sync():
    rows = _status((1, STATUS_APPLIED), (2, STATUS_MD5_MISMATCH), (3, STATUS_PENDING))

    with pytest.raises(MigrationException) as exc_info:
        Migrator.select_repair_rows(rows, [2, 3, 1, 9])

    message = str(exc_info.value)
    assert "1 (applied), 3 (pending), 9 (not found)" in message
    assert "md5-mismatch and unknown" in message


def test_repair_report_changes_nothing():
    conn = _BookkeepingConn({1: "a", 2: "old", 4: "gone"})

    rows = Migrator(conn).repair(LOCAL)

    assert [(r.version, r.state, r.md5) for r in rows] == [
        (2, STATUS_MD5_MISMATCH, "old"),
        (4, STATUS_UNKNOWN, "gone"),
    ]
    assert not conn.commands
    assert not conn.inserts


def test_repair_prune_needs_write():
    with pytest.raises(MigrationException, match="prune only works together"):
        Migrator(_BookkeepingConn()).repair(LOCAL, prune=True)


def test_repair_write_inserts_then_deletes_only_stale_rows():
    conn = _BookkeepingConn({1: "a", 2: "old", 3: "older", 4: "gone"})

    rows = Migrator(conn, migrations_table="v").repair(
        LOCAL, versions=[2], write=True, down_versions={2}
    )

    assert rows == [StatusRow(2, STATUS_APPLIED, "b", APPLIED_AT, True)]
    assert conn.inserts == [
        ('"v"', [{"version": 2, "script": "SELECT 2;", "md5": "b"}])
    ]
    assert conn.commands == [
        "ALTER TABLE \"v\" DELETE WHERE version = 2 AND md5 != 'b' "
        "SETTINGS mutations_sync = 2",
        'OPTIMIZE TABLE "v" FINAL',
    ]
    # Version 3 was out of sync too, but was not asked for.
    assert conn.rows == {1: "a", 2: "b", 3: "older", 4: "gone"}


def test_repair_write_leaves_unknown_rows_without_prune(caplog):
    conn = _BookkeepingConn({1: "a", 4: "gone"})

    rows = Migrator(conn).repair(LOCAL, write=True)

    assert [(r.version, r.state) for r in rows] == [(4, STATUS_UNKNOWN)]
    assert not conn.commands
    assert "pass prune to delete its row" in caplog.text


def test_repair_write_prune_deletes_unknown_rows():
    conn = _BookkeepingConn({1: "a", 2: "old", 4: "gone"})

    rows = Migrator(conn).repair(LOCAL, write=True, prune=True)

    assert [(r.version, r.state) for r in rows] == [
        (2, STATUS_APPLIED),
        (4, STATUS_PRUNED),
    ]
    assert (
        'ALTER TABLE "schema_versions" DELETE WHERE version = 4 '
        "SETTINGS mutations_sync = 2" in conn.commands
    )
    assert conn.rows == {1: "a", 2: "b"}


# --- command line --------------------------------------------------------


def test_baseline_arguments():
    context = get_context(["baseline", "--to", "3"])

    assert context.command == "baseline"
    assert context.to_version == 3
    assert context.dry_run is False
    assert context.create_db_if_not_exists is True
    assert context.lock is False
    assert context.format == "table"

    context = get_context(
        [
            "baseline",
            "--to",
            "1",
            "--dry-run",
            "--cluster-name",
            "c",
            "--migrations-table-engine",
            "Memory",
            "--no-create-db-if-not-exists",
            "--lock",
            "--format",
            "json",
        ]
    )
    assert context.dry_run and context.lock
    assert context.cluster_name == "c"
    assert context.migrations_table_engine == "Memory"
    assert context.create_db_if_not_exists is False
    assert context.format == "json"


def test_baseline_needs_to(capsys):
    with pytest.raises(SystemExit):
        get_context(["baseline"])

    assert "--to" in capsys.readouterr().err


def test_repair_arguments():
    context = get_context(["repair"])

    assert context.command == "repair"
    assert context.write is False
    assert context.prune is False
    assert context.versions == []

    context = get_context(
        ["repair", "--write", "--prune", "--version", "2", "--version", "5", "--lock"]
    )
    assert context.write and context.prune and context.lock
    assert context.versions == [2, 5]


def test_repair_prune_requires_write(capsys):
    with pytest.raises(SystemExit):
        get_context(["repair", "--prune"])

    assert "--prune requires --write" in capsys.readouterr().err


@pytest.mark.parametrize("command", [["baseline", "--to", "1"], ["repair"]])
def test_baseline_and_repair_reject_explicit_migrations(command):
    with pytest.raises(SystemExit):
        get_context([*command, "--migrations", "1"])


def test_repair_rejects_create_options():
    for flag in ("--cluster-name", "--migrations-table-engine"):
        with pytest.raises(SystemExit):
            get_context(["repair", flag, "x"])


def test_do_baseline_and_do_repair_pass_every_option():
    calls = []
    cluster = types.SimpleNamespace(
        baseline=lambda **kw: calls.append(kw) or [],
        repair=lambda **kw: calls.append(kw) or [],
    )

    command_line.do_baseline(
        cluster,
        get_context(
            ["baseline", "--to", "2", "--db-name", "d", "--lock", "--lock-ttl", "9"]
        ),
    )
    command_line.do_repair(
        cluster, get_context(["repair", "--write", "--version", "3", "--prune"])
    )

    baseline, repair = calls[0], calls[1]
    assert baseline["to_version"] == 2
    assert baseline["db_name"] == "d"
    assert baseline["create_db_if_no_exists"] is True
    assert baseline["dryrun"] is False
    assert (baseline["lock"], baseline["lock_ttl"]) == (True, 9)
    assert repair["versions"] == [3]
    assert repair["write"] and repair["prune"]
    assert repair["lock"] is False


ROWS = [
    StatusRow(1, STATUS_APPLIED, "a1", APPLIED_AT, True),
    StatusRow(2, STATUS_MD5_MISMATCH, "b2", APPLIED_AT, False),
]


def _run(monkeypatch, capsys, rows, *argv):
    cluster = types.SimpleNamespace(
        default_db_name="default_db",
        baseline=lambda **kw: rows,
        repair=lambda **kw: rows,
    )
    monkeypatch.setattr(
        "clickhouse_migrations.cli.common.create_cluster", lambda ctx: cluster
    )
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", *argv])
    code = main()
    return code, capsys.readouterr()


@pytest.mark.parametrize(
    "argv, rows, expected",
    [
        (["repair"], ROWS, 1),
        (["repair"], [], 0),
        (["repair", "--write"], ROWS, 0),
        (["repair", "--write"], [], 0),
    ],
)
def test_repair_exit_codes(monkeypatch, capsys, argv, rows, expected):
    code, captured = _run(monkeypatch, capsys, rows, *argv)

    assert code == expected
    if rows:
        assert captured.out.splitlines()[0].split()[:2] == ["VERSION", "STATUS"]
        assert STATUS_MD5_MISMATCH in captured.out
    else:
        assert "Nothing to repair" in captured.out


def test_repair_report_hints_at_write(monkeypatch, capsys, caplog):
    _run(monkeypatch, capsys, ROWS, "repair")

    assert "--write" in caplog.text


def test_repair_json(monkeypatch, capsys):
    code, captured = _run(monkeypatch, capsys, ROWS, "repair", "--format", "json")

    assert code == 1
    doc = json.loads(captured.out)
    assert doc["database"] == "default_db"
    assert [m["state"] for m in doc["migrations"]] == ["applied", "md5-mismatch"]


def test_repair_error_exits_1(monkeypatch, caplog):
    def fail(**_kw):
        raise MigrationException("boom")

    cluster = types.SimpleNamespace(repair=fail)
    monkeypatch.setattr(
        "clickhouse_migrations.cli.common.create_cluster", lambda ctx: cluster
    )
    monkeypatch.setattr(sys, "argv", ["clickhouse-migrations", "repair"])

    assert main() == 1
    assert "boom" in caplog.text


def test_baseline_prints_the_status_table(monkeypatch, capsys, caplog):
    code, captured = _run(monkeypatch, capsys, ROWS, "baseline", "--to", "2")

    assert code == 0
    assert captured.out.splitlines()[1].split()[:2] == ["1", "applied"]
    assert "Dry run" not in caplog.text


def test_baseline_dry_run_says_so(monkeypatch, capsys, caplog):
    code, captured = _run(
        monkeypatch,
        capsys,
        ROWS,
        "baseline",
        "--to",
        "2",
        "--dry-run",
        "--format",
        "json",
        "--db-name",
        "mydb",
    )

    assert code == 0
    assert json.loads(captured.out)["database"] == "mydb"
    assert "Dry run: nothing was recorded" in caplog.text


def test_baseline_without_migrations_prints_a_message(monkeypatch, capsys):
    code, captured = _run(monkeypatch, capsys, [], "baseline", "--to", "2")

    assert code == 0
    assert "No migrations found." in captured.out
