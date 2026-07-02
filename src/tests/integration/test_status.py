import sys
from pathlib import Path

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.migrator import STATUS_APPLIED, STATUS_PENDING

TESTS_DIR = Path(__file__).parents[1]
MIGRATIONS = TESTS_DIR / "migrations"


def test_status_all_pending_when_not_initialized(cluster: ClickhouseCluster):
    rows = cluster.status("pytest", MIGRATIONS)

    assert len(rows) == 1
    assert all(r.state == STATUS_PENDING for r in rows)
    assert all(r.applied_at is None for r in rows)


def test_status_reports_applied_after_migrate(cluster: ClickhouseCluster):
    cluster.migrate("pytest", MIGRATIONS)

    rows = cluster.status("pytest", MIGRATIONS)

    assert len(rows) == 1
    assert all(r.state == STATUS_APPLIED for r in rows)
    assert all(r.applied_at is not None for r in rows)


def test_main_status_flag_prints_table(cluster: ClickhouseCluster, monkeypatch, capsys):
    cluster.migrate("pytest", MIGRATIONS)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "clickhouse-migrations",
            "--db-name",
            "pytest",
            "--migrations-dir",
            str(MIGRATIONS),
            "--status",
        ],
    )

    assert main() == 0

    out = capsys.readouterr().out
    assert "VERSION" in out
    assert STATUS_APPLIED in out
