import sys

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.migrator import STATUS_APPLIED, STATUS_PENDING


def _scaffold(monkeypatch, storage_dir, name, *flags):
    monkeypatch.setattr(
        sys,
        "argv",
        ["clickhouse-migrations", "new", name, "--dir", str(storage_dir), *flags],
    )

    assert main() == 0


def _states(cluster: ClickhouseCluster, storage_dir):
    return {r.version: r.state for r in cluster.status("pytest", storage_dir)}


def test_scaffolded_migration_applies(
    cluster: ClickhouseCluster, monkeypatch, tmp_path
):
    storage_dir = tmp_path / "migrations"
    _scaffold(monkeypatch, storage_dir, "add events")

    migration_file = storage_dir / "001_add_events.sql"
    migration_file.write_text(
        migration_file.read_text(encoding="utf8")
        + "CREATE TABLE events (id UInt32) ENGINE = MergeTree() ORDER BY id;",
        encoding="utf8",
    )

    applied = cluster.migrate("pytest", storage_dir)

    # The generated file name is what MigrationStorage has to parse back.
    assert [m.version for m in applied] == [1]
    assert "events" in cluster.show_tables("pytest")
    assert _states(cluster, storage_dir) == {1: STATUS_APPLIED}


def test_scaffolded_down_pair_rolls_back(
    cluster: ClickhouseCluster, monkeypatch, tmp_path
):
    storage_dir = tmp_path / "migrations"
    _scaffold(monkeypatch, storage_dir, "add events", "--down")
    _scaffold(monkeypatch, storage_dir, "add users", "--down")

    for version, table in ((1, "events"), (2, "users")):
        up_file = storage_dir / f"00{version}_add_{table}.sql"
        up_file.write_text(
            up_file.read_text(encoding="utf8")
            + f"CREATE TABLE {table} (id UInt32) ENGINE = MergeTree() ORDER BY id;",
            encoding="utf8",
        )
        down_file = storage_dir / f"00{version}_add_{table}.down.sql"
        down_file.write_text(
            down_file.read_text(encoding="utf8") + f"DROP TABLE {table};",
            encoding="utf8",
        )

    cluster.migrate("pytest", storage_dir)
    assert _states(cluster, storage_dir) == {1: STATUS_APPLIED, 2: STATUS_APPLIED}

    rolled = cluster.rollback("pytest", storage_dir, steps=1)

    assert rolled == [2]
    tables = cluster.show_tables("pytest")
    assert "users" not in tables
    assert "events" in tables
    assert _states(cluster, storage_dir) == {1: STATUS_APPLIED, 2: STATUS_PENDING}
