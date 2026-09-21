import sys
from pathlib import Path

import pytest

from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migrator import STATUS_APPLIED, STATUS_PENDING

TESTS_DIR = Path(__file__).parents[1]
STAGED = TESTS_DIR / "staged_migrations"


def _states(cluster: ClickhouseCluster):
    return {r.version: r.state for r in cluster.status("pytest", STAGED)}


def _staged_tables(cluster: ClickhouseCluster):
    return sorted(t for t in cluster.show_tables("pytest") if t.startswith("staged_"))


def _run_main(monkeypatch, *extra):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "clickhouse-migrations",
            "migrate",
            "--db-name",
            "pytest",
            "--migrations-dir",
            str(STAGED),
            *extra,
        ],
    )
    return main()


def test_staged_apply(cluster: ClickhouseCluster):
    applied = cluster.migrate("pytest", STAGED, to_version=2)

    assert [m.version for m in applied] == [1, 2]
    assert _staged_tables(cluster) == ["staged_1", "staged_2"]
    assert _states(cluster) == {
        1: STATUS_APPLIED,
        2: STATUS_APPLIED,
        3: STATUS_PENDING,
        4: STATUS_PENDING,
    }

    applied = cluster.migrate("pytest", STAGED, to_version=3)
    assert [m.version for m in applied] == [3]
    assert _staged_tables(cluster) == ["staged_1", "staged_2", "staged_3"]

    applied = cluster.migrate("pytest", STAGED)
    assert [m.version for m in applied] == [4]
    assert len(_staged_tables(cluster)) == 4
    assert set(_states(cluster).values()) == {STATUS_APPLIED}


def test_to_equal_to_highest_applied_is_noop(cluster: ClickhouseCluster):
    cluster.migrate("pytest", STAGED, to_version=2)

    assert not cluster.migrate("pytest", STAGED, to_version=2)
    assert _staged_tables(cluster) == ["staged_1", "staged_2"]


def test_to_below_highest_applied_fails(cluster: ClickhouseCluster):
    cluster.migrate("pytest", STAGED, to_version=3)

    with pytest.raises(MigrationException, match="down"):
        cluster.migrate("pytest", STAGED, to_version=2)

    assert _staged_tables(cluster) == ["staged_1", "staged_2", "staged_3"]


def test_to_unknown_version_fails(cluster: ClickhouseCluster):
    with pytest.raises(MigrationException, match="not among the local"):
        cluster.migrate("pytest", STAGED, to_version=99)

    assert not _staged_tables(cluster)


def test_to_with_explicit_migrations_fails(cluster: ClickhouseCluster):
    with pytest.raises(MigrationException, match="mutually exclusive"):
        cluster.migrate("pytest", STAGED, explicit_migrations=["1"], to_version=2)

    assert not _staged_tables(cluster)


def test_dry_run_to_applies_nothing(cluster: ClickhouseCluster):
    planned = cluster.migrate("pytest", STAGED, dryrun=True, to_version=2)

    assert [m.version for m in planned] == [1, 2]
    assert not _staged_tables(cluster)
    assert set(_states(cluster).values()) == {STATUS_PENDING}


def test_fake_to_marks_only_up_to_target(cluster: ClickhouseCluster):
    cluster.migrate("pytest", STAGED, fake=True, to_version=2)

    assert not _staged_tables(cluster)
    assert _states(cluster) == {
        1: STATUS_APPLIED,
        2: STATUS_APPLIED,
        3: STATUS_PENDING,
        4: STATUS_PENDING,
    }


def test_main_migrate_to(cluster: ClickhouseCluster, monkeypatch):
    assert _run_main(monkeypatch, "--to", "2") == 0
    assert _staged_tables(cluster) == ["staged_1", "staged_2"]

    assert _run_main(monkeypatch, "--to", "1") == 1
    assert _staged_tables(cluster) == ["staged_1", "staged_2"]

    assert _run_main(monkeypatch) == 0
    assert len(_staged_tables(cluster)) == 4


def test_main_migrate_to_conflicts_with_migrations(monkeypatch):
    with pytest.raises(SystemExit) as exc_info:
        _run_main(monkeypatch, "--to", "2", "--migrations", "1")

    assert exc_info.value.code == 2
