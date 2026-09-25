import json
import sys

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.lock import KeeperMapLock
from clickhouse_migrations.migration import MigrationStorage
from clickhouse_migrations.migrator import (
    STATUS_APPLIED,
    STATUS_MD5_MISMATCH,
    STATUS_PENDING,
    STATUS_PRUNED,
    STATUS_UNKNOWN,
)
from tests.clickhouse_version import NEEDS_LOCK

CLICKHOUSE_SERVERS = (
    "clickhouse01",
    "clickhouse02",
    "clickhouse03",
    "clickhouse04",
)
OTHER_OWNER = "dead-pod:4242:11111111-2222-3333-4444-555555555555"

# Plain CREATE TABLE (no IF NOT EXISTS): executing one of them on a database
# that already has the table fails, so a passing migrate proves baseline
# recorded them without running them.
SCHEMA_MIGRATIONS = {
    "001_events.sql": "CREATE TABLE events (id UInt32) ENGINE = MergeTree ORDER BY id;\n",
    "002_users.sql": "CREATE TABLE users (id UInt32) ENGINE = MergeTree ORDER BY id;\n",
    "003_orders.sql": "CREATE TABLE orders (id UInt32) ENGINE = MergeTree ORDER BY id;\n",
}

# Side-effect free migrations for the repair scenarios.
SELECT_MIGRATIONS = {
    "001_one.sql": "SELECT 1;\n",
    "002_two.sql": "SELECT 2;\n",
    "003_three.sql": "SELECT 3;\n",
}


def _write(directory, files):
    directory.mkdir(exist_ok=True)
    for name, script in files.items():
        (directory / name).write_text(script, encoding="utf8")
    return directory


@pytest.fixture(name="schema_dir")
def schema_dir_fixture(tmp_path):
    return _write(tmp_path / "schema", SCHEMA_MIGRATIONS)


@pytest.fixture(name="select_dir")
def select_dir_fixture(tmp_path):
    return _write(tmp_path / "select", SELECT_MIGRATIONS)


def _cli(monkeypatch, capsys, *argv):
    monkeypatch.setattr(
        sys, "argv", ["clickhouse-migrations", *argv, "--db-name", "pytest"]
    )
    code = main()
    return code, capsys.readouterr().out


def _states(cluster, directory):
    return {r.version: r.state for r in cluster.status("pytest", directory)}


def _stored(cluster, table="schema_versions"):
    with cluster.connection("pytest") as conn:
        return [
            (row["version"], row["md5"], row["script"])
            for row in conn.query(
                f"SELECT version, md5, script FROM {table} ORDER BY version, md5"
            )
        ]


def _local(directory):
    return {m.version: m for m in MigrationStorage(directory).migrations()}


def _edit(directory, name, script):
    (directory / name).write_text(script, encoding="utf8")


def _existing_schema(cluster):
    with cluster.connection("pytest") as conn:
        conn.command("CREATE TABLE events (id UInt32) ENGINE = MergeTree ORDER BY id")
        conn.command("CREATE TABLE users (id UInt32) ENGINE = MergeTree ORDER BY id")


# --- baseline ------------------------------------------------------------


def test_baseline_then_migrate_applies_only_newer(
    cluster: ClickhouseCluster, schema_dir, monkeypatch, capsys
):
    _existing_schema(cluster)

    code, out = _cli(
        monkeypatch,
        capsys,
        "baseline",
        "--to",
        "2",
        "--migrations-dir",
        str(schema_dir),
    )

    assert code == 0
    lines = out.splitlines()
    assert lines[0].split()[:2] == ["VERSION", "STATUS"]
    assert [line.split()[:2] for line in lines[1:]] == [
        ["1", STATUS_APPLIED],
        ["2", STATUS_APPLIED],
        ["3", STATUS_PENDING],
    ]
    local = _local(schema_dir)
    # md5 and raw script are recorded, exactly like --fake does.
    assert _stored(cluster) == [(v, local[v].md5, local[v].script) for v in (1, 2)]
    assert "orders" not in cluster.show_tables("pytest")

    applied = cluster.migrate("pytest", schema_dir)

    assert [m.version for m in applied] == [3]
    assert "orders" in cluster.show_tables("pytest")
    assert set(_states(cluster, schema_dir).values()) == {STATUS_APPLIED}


def test_baseline_creates_the_database_like_migrate(cluster, schema_dir):
    with cluster.connection("") as conn:
        conn.command("DROP DATABASE IF EXISTS pytest SYNC")

    rows = cluster.baseline("pytest", schema_dir, to_version=3)

    assert [r.state for r in rows] == [STATUS_APPLIED] * 3
    assert cluster.show_tables("pytest") == ["schema_versions"]


def test_baseline_without_create_db_fails_on_a_missing_database(cluster, schema_dir):
    with cluster.connection("") as conn:
        conn.command("DROP DATABASE IF EXISTS pytest SYNC")

    with pytest.raises(Exception, match="(?i)database"):  # driver error
        cluster.baseline(
            "pytest", schema_dir, to_version=1, create_db_if_no_exists=False
        )


def test_baseline_is_refused_on_a_non_empty_history(
    cluster: ClickhouseCluster, schema_dir, monkeypatch, capsys, caplog
):
    cluster.migrate("pytest", schema_dir, to_version=1)
    before = _stored(cluster)

    with pytest.raises(MigrationException, match="Refusing to baseline"):
        cluster.baseline("pytest", schema_dir, to_version=2)
    with pytest.raises(MigrationException, match="Refusing to baseline"):
        cluster.baseline("pytest", schema_dir, to_version=2, dryrun=True)

    code, out = _cli(
        monkeypatch,
        capsys,
        "baseline",
        "--to",
        "3",
        "--migrations-dir",
        str(schema_dir),
    )
    assert code == 1
    assert out == ""
    assert "already records 1 migration" in caplog.text
    assert _stored(cluster) == before


def test_baseline_is_allowed_after_everything_was_rolled_back(cluster, tmp_path):
    directory = _write(tmp_path / "down", SELECT_MIGRATIONS)
    (directory / "001_one.down.sql").write_text("SELECT 0;\n", encoding="utf8")
    cluster.migrate("pytest", directory, to_version=1)
    assert cluster.rollback("pytest", directory, steps=1) == [1]

    rows = cluster.baseline("pytest", directory, to_version=3)

    assert [(r.version, r.state, r.has_down) for r in rows] == [
        (1, STATUS_APPLIED, True),
        (2, STATUS_APPLIED, False),
        (3, STATUS_APPLIED, False),
    ]


def test_baseline_dry_run_writes_nothing(
    cluster: ClickhouseCluster, schema_dir, monkeypatch, capsys
):
    with cluster.connection("") as conn:
        conn.command("DROP DATABASE IF EXISTS pytest SYNC")

    code, out = _cli(
        monkeypatch,
        capsys,
        "baseline",
        "--to",
        "2",
        "--dry-run",
        "--format",
        "json",
        "--migrations-dir",
        str(schema_dir),
    )

    assert code == 0
    doc = json.loads(out)
    assert [(m["version"], m["state"]) for m in doc["migrations"]] == [
        (1, STATUS_APPLIED),
        (2, STATUS_APPLIED),
        (3, STATUS_PENDING),
    ]
    assert all(m["applied_at"] is None for m in doc["migrations"])
    # Not even the database (let alone the table) was created.
    with cluster.connection("") as conn:
        assert not conn.query("SELECT 1 FROM system.databases WHERE name = 'pytest'")


def test_baseline_dry_run_on_an_empty_table(cluster, schema_dir):
    cluster.init_schema("pytest")

    rows = cluster.baseline("pytest", schema_dir, to_version=1, dryrun=True)

    assert [r.state for r in rows] == [STATUS_APPLIED, STATUS_PENDING, STATUS_PENDING]
    assert _stored(cluster) == []


def test_baseline_rejects_an_unknown_version(cluster, schema_dir, monkeypatch, capsys):
    code, out = _cli(
        monkeypatch,
        capsys,
        "baseline",
        "--to",
        "7",
        "--migrations-dir",
        str(schema_dir),
    )

    assert code == 1
    assert out == ""
    # Validated before anything was created.
    assert cluster.show_tables("pytest") == []


# --- repair --------------------------------------------------------------


def test_repair_report_on_an_edited_file(
    cluster: ClickhouseCluster, select_dir, monkeypatch, capsys
):
    cluster.migrate("pytest", select_dir)
    stored_md5 = _local(select_dir)[2].md5
    _edit(select_dir, "002_two.sql", "-- reformatted\nSELECT 2;\n")
    before = _stored(cluster)

    code, out = _cli(monkeypatch, capsys, "repair", "--migrations-dir", str(select_dir))

    assert code == 1
    lines = out.splitlines()
    assert lines[0].split()[:3] == ["VERSION", "STATUS", "MD5"]
    assert lines[1].split()[:3] == ["2", STATUS_MD5_MISMATCH, stored_md5]
    assert len(lines) == 2
    assert _stored(cluster) == before


def test_repair_report_in_sync(cluster, select_dir, monkeypatch, capsys):
    cluster.migrate("pytest", select_dir)

    code, out = _cli(monkeypatch, capsys, "repair", "--migrations-dir", str(select_dir))

    assert code == 0
    assert "Nothing to repair" in out


def test_repair_without_history_is_a_noop(cluster, select_dir, monkeypatch, capsys):
    assert cluster.repair("pytest", select_dir) == []
    with pytest.raises(MigrationException, match=r"1 \(pending\)"):
        cluster.repair("pytest", select_dir, versions=[1], write=True)

    # Read-only: the report never creates the bookkeeping table.
    assert cluster.show_tables("pytest") == []
    code, _ = _cli(monkeypatch, capsys, "repair", "--migrations-dir", str(select_dir))
    assert code == 0


def test_repair_write_fixes_exactly_the_requested_version(
    cluster: ClickhouseCluster, select_dir, monkeypatch, capsys
):
    cluster.migrate("pytest", select_dir)
    _edit(select_dir, "002_two.sql", "-- reformatted\nSELECT 2;\n")
    _edit(select_dir, "003_three.sql", "-- also changed\nSELECT 3;\n")
    local = _local(select_dir)
    untouched = [row for row in _stored(cluster) if row[0] != 2]

    code, out = _cli(
        monkeypatch,
        capsys,
        "repair",
        "--write",
        "--version",
        "2",
        "--migrations-dir",
        str(select_dir),
    )

    assert code == 0
    assert [line.split()[:3] for line in out.splitlines()[1:]] == [
        ["2", STATUS_APPLIED, local[2].md5]
    ]
    # md5 AND script are updated for 2; 1 and the other mismatch (3) are not.
    stored = _stored(cluster)
    assert (2, local[2].md5, local[2].script) in stored
    assert [row for row in stored if row[0] != 2] == untouched
    assert len(stored) == 3
    assert _states(cluster, select_dir) == {
        1: STATUS_APPLIED,
        2: STATUS_APPLIED,
        3: STATUS_MD5_MISMATCH,
    }

    # The rest in one go, then status --strict passes and migrate works again.
    code, _ = _cli(
        monkeypatch, capsys, "repair", "--write", "--migrations-dir", str(select_dir)
    )
    assert code == 0
    code, _ = _cli(
        monkeypatch, capsys, "status", "--strict", "--migrations-dir", str(select_dir)
    )
    assert code == 0
    _edit(select_dir, "004_four.sql", "SELECT 4;\n")
    assert [m.version for m in cluster.migrate("pytest", select_dir)] == [4]


def test_repair_version_must_be_out_of_sync(
    cluster, select_dir, monkeypatch, capsys, caplog
):
    cluster.migrate("pytest", select_dir, to_version=2)
    before = _stored(cluster)

    code, out = _cli(
        monkeypatch,
        capsys,
        "repair",
        "--write",
        "--version",
        "1",
        "--version",
        "3",
        "--migrations-dir",
        str(select_dir),
    )

    assert code == 1
    assert out == ""
    assert "1 (applied), 3 (pending)" in caplog.text
    assert _stored(cluster) == before


def test_repair_unknown_is_pruned_only_with_prune_and_write(
    cluster: ClickhouseCluster, select_dir, monkeypatch, capsys
):
    cluster.migrate("pytest", select_dir)
    (select_dir / "003_three.sql").unlink()
    args = ("--migrations-dir", str(select_dir))

    code, out = _cli(monkeypatch, capsys, "repair", *args)
    assert code == 1
    assert out.splitlines()[1].split()[:2] == ["3", STATUS_UNKNOWN]

    with pytest.raises(SystemExit):
        _cli(monkeypatch, capsys, "repair", "--prune", *args)

    code, out = _cli(monkeypatch, capsys, "repair", "--write", *args)
    assert code == 0
    assert out.splitlines()[1].split()[:2] == ["3", STATUS_UNKNOWN]
    assert _states(cluster, select_dir)[3] == STATUS_UNKNOWN

    code, out = _cli(monkeypatch, capsys, "repair", "--write", "--prune", *args)
    assert code == 0
    assert out.splitlines()[1].split()[:2] == ["3", STATUS_PRUNED]
    assert [row[0] for row in _stored(cluster)] == [1, 2]

    code, _ = _cli(monkeypatch, capsys, "status", "--strict", *args)
    assert code == 0


def test_repair_json(cluster, select_dir, monkeypatch, capsys):
    cluster.migrate("pytest", select_dir)
    _edit(select_dir, "001_one.sql", "SELECT 1; -- x\n")

    code, out = _cli(
        monkeypatch,
        capsys,
        "repair",
        "--format",
        "json",
        "--migrations-dir",
        str(select_dir),
    )

    assert code == 1
    doc = json.loads(out)
    assert [(m["version"], m["state"]) for m in doc["migrations"]] == [
        (1, STATUS_MD5_MISMATCH)
    ]


def test_repair_cleans_up_duplicate_rows(cluster, select_dir):
    # Two runs without the lock can leave two rows per version behind.
    cluster.migrate("pytest", select_dir)
    with cluster.connection("pytest") as conn:
        conn.insert(
            "schema_versions",
            [{"version": 1, "md5": "stale", "script": "SELECT 'stale';"}],
        )
    _edit(select_dir, "001_one.sql", "SELECT 1; -- x\n")
    local = _local(select_dir)

    cluster.repair("pytest", select_dir, write=True)

    assert [row for row in _stored(cluster) if row[0] == 1] == [
        (1, local[1].md5, local[1].script)
    ]


# --- custom table, drivers, ON CLUSTER, lock -----------------------------


def test_custom_migrations_table(cluster, schema_dir, monkeypatch, capsys):
    _existing_schema(cluster)
    table = ("--migrations-table", "my_versions", "--migrations-dir", str(schema_dir))

    code, _ = _cli(monkeypatch, capsys, "baseline", "--to", "2", *table)
    assert code == 0
    assert "schema_versions" not in cluster.show_tables("pytest")
    assert [row[0] for row in _stored(cluster, "my_versions")] == [1, 2]

    _edit(
        schema_dir,
        "001_events.sql",
        "-- adopted\n" + SCHEMA_MIGRATIONS["001_events.sql"],
    )
    code, out = _cli(monkeypatch, capsys, "repair", *table)
    assert code == 1
    assert STATUS_MD5_MISMATCH in out

    code, _ = _cli(monkeypatch, capsys, "repair", "--write", *table)
    assert code == 0
    code, _ = _cli(monkeypatch, capsys, "status", "--strict", *table)
    assert code == 0
    assert "schema_versions" not in cluster.show_tables("pytest")


@pytest.mark.parametrize(
    "driver, port", [("clickhouse-driver", "9000"), ("clickhouse-connect", "8123")]
)
def test_both_drivers(driver, port, select_dir):
    driver_cluster = ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        db_port=port,
        driver=driver,
    )

    rows = driver_cluster.baseline(None, select_dir, to_version=2)
    assert [r.state for r in rows] == [STATUS_APPLIED, STATUS_APPLIED, STATUS_PENDING]

    _edit(select_dir, "002_two.sql", "SELECT 2; -- x\n")
    assert [r.state for r in driver_cluster.repair(None, select_dir)] == [
        STATUS_MD5_MISMATCH
    ]
    rows = driver_cluster.repair(None, select_dir, write=True)
    assert [(r.version, r.state) for r in rows] == [(2, STATUS_APPLIED)]
    assert _states(driver_cluster, select_dir) == {
        1: STATUS_APPLIED,
        2: STATUS_APPLIED,
        3: STATUS_PENDING,
    }


def _replica_rows(cluster, server):
    with cluster.connection("pytest") as conn:
        return [
            (row["version"], row["md5"])
            for row in conn.query(
                f"SELECT version, md5 FROM remote('{server}', 'pytest.schema_versions') "
                "ORDER BY version, md5"
            )
        ]


def test_on_cluster(cluster, _clean_slate, select_dir, monkeypatch, capsys):
    args = ("--cluster-name", "company_cluster", "--migrations-dir", str(select_dir))

    code, _ = _cli(monkeypatch, capsys, "baseline", "--to", "3", *args)
    assert code == 0

    local = _local(select_dir)
    expected = [(v, local[v].md5) for v in (1, 2, 3)]
    for server in CLICKHOUSE_SERVERS:
        assert _replica_rows(cluster, server) == expected

    _edit(select_dir, "002_two.sql", "SELECT 2; -- x\n")
    local = _local(select_dir)
    code, _ = _cli(
        monkeypatch,
        capsys,
        "repair",
        "--write",
        "--migrations-dir",
        str(select_dir),
    )
    assert code == 0

    # mutations_sync = 2 waited for every replica: no stale row anywhere.
    expected = [(v, local[v].md5) for v in (1, 2, 3)]
    for server in CLICKHOUSE_SERVERS:
        assert _replica_rows(cluster, server) == expected
    assert set(_states(cluster, select_dir).values()) == {STATUS_APPLIED}


def _foreign_lock(cluster):
    conn = cluster.connection("pytest")
    held = KeeperMapLock(conn, "pytest", timeout=0, owner=OTHER_OWNER)
    assert held.acquire() is True
    return held


@NEEDS_LOCK
def test_lock(cluster, select_dir, monkeypatch, capsys, caplog):
    args = ("--lock", "--lock-timeout", "0", "--migrations-dir", str(select_dir))
    held = _foreign_lock(cluster)
    try:
        # Only real writes take the lock.
        code, _ = _cli(monkeypatch, capsys, "baseline", "--to", "2", "--dry-run", *args)
        assert code == 0
        code, _ = _cli(monkeypatch, capsys, "baseline", "--to", "2", *args)
        assert code == 1
        assert OTHER_OWNER in caplog.text
        assert "schema_versions" not in cluster.show_tables("pytest")
    finally:
        held.release()

    code, _ = _cli(monkeypatch, capsys, "baseline", "--to", "2", *args)
    assert code == 0
    _edit(select_dir, "001_one.sql", "SELECT 1; -- x\n")

    held = _foreign_lock(cluster)
    try:
        code, _ = _cli(monkeypatch, capsys, "repair", *args)
        assert code == 1  # the report still runs, and finds the mismatch
        with pytest.raises(MigrationException, match=OTHER_OWNER):
            cluster.repair("pytest", select_dir, write=True, lock=True, lock_timeout=0)
    finally:
        held.release()

    code, _ = _cli(monkeypatch, capsys, "repair", "--write", *args)
    assert code == 0
    assert set(_states(cluster, select_dir).values()) == {
        STATUS_APPLIED,
        STATUS_PENDING,
    }
    with cluster.connection("pytest") as conn:
        assert KeeperMapLock(conn, "pytest").holder() is None


def test_repair_prune_needs_write(cluster, select_dir):
    with pytest.raises(MigrationException, match="prune only works together"):
        cluster.repair("pytest", select_dir, prune=True)
