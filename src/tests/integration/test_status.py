import json
import sys
from datetime import datetime
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
            "status",
            "--db-name",
            "pytest",
            "--migrations-dir",
            str(MIGRATIONS),
        ],
    )

    assert main() == 0

    out = capsys.readouterr().out
    assert "VERSION" in out
    assert STATUS_APPLIED in out


def _status(monkeypatch, capsys, mdir, *flags):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "clickhouse-migrations",
            "status",
            "--db-name",
            "pytest",
            "--migrations-dir",
            str(mdir),
            *flags,
        ],
    )
    code = main()
    return code, capsys.readouterr()


def _make_dir(tmp_path):
    (tmp_path / "001_a.sql").write_text("SELECT 1", encoding="utf8")
    (tmp_path / "001_a.down.sql").write_text("SELECT 1", encoding="utf8")
    (tmp_path / "002_b.sql").write_text("SELECT 2", encoding="utf8")
    return tmp_path


def test_status_json_applied_and_pending(
    cluster: ClickhouseCluster, tmp_path, monkeypatch, capsys
):
    mdir = _make_dir(tmp_path)
    cluster.migrate("pytest", mdir, explicit_migrations=["1"])

    code, captured = _status(
        monkeypatch, capsys, mdir, "--format", "json", "--log-level", "DEBUG"
    )

    assert code == 0
    # stdout is exactly one JSON document, so it can be piped to jq.
    doc = json.loads(captured.out)
    assert doc["database"] == "pytest"
    first, second = doc["migrations"]
    assert (first["version"], first["state"], first["has_down"]) == (
        1,
        "applied",
        True,
    )
    assert datetime.fromisoformat(first["applied_at"])
    assert len(first["md5"]) == 32
    assert second == {
        "version": 2,
        "state": "pending",
        "md5": second["md5"],
        "applied_at": None,
        "has_down": False,
    }


def test_status_table_shows_has_down(
    cluster: ClickhouseCluster, tmp_path, monkeypatch, capsys
):
    mdir = _make_dir(tmp_path)
    cluster.migrate("pytest", mdir, explicit_migrations=["1"])

    code, captured = _status(monkeypatch, capsys, mdir)

    assert code == 0
    lines = captured.out.splitlines()
    assert lines[0].split()[-2:] == ["HAS", "DOWN"]
    assert lines[1].split()[-1] == "yes"
    assert lines[2].split()[-1] == "no"


def test_status_exit_code_pending(
    cluster: ClickhouseCluster, tmp_path, monkeypatch, capsys
):
    mdir = _make_dir(tmp_path)
    cluster.migrate("pytest", mdir, explicit_migrations=["1"])

    assert _status(monkeypatch, capsys, mdir)[0] == 0
    assert _status(monkeypatch, capsys, mdir, "--strict")[0] == 0
    assert _status(monkeypatch, capsys, mdir, "--exit-code-pending")[0] == 1
    assert _status(monkeypatch, capsys, mdir, "--strict", "--exit-code-pending")[0] == 1

    cluster.migrate("pytest", mdir)
    assert _status(monkeypatch, capsys, mdir, "--strict", "--exit-code-pending")[0] == 0


def test_status_pending_before_schema_exists_has_down(tmp_path, monkeypatch, capsys):
    mdir = _make_dir(tmp_path)

    code, captured = _status(
        monkeypatch, capsys, mdir, "--format", "json", "--exit-code-pending"
    )

    assert code == 1
    doc = json.loads(captured.out)
    assert [m["has_down"] for m in doc["migrations"]] == [True, False]
    assert all(m["state"] == "pending" for m in doc["migrations"])


def test_status_strict_md5_mismatch(
    cluster: ClickhouseCluster, tmp_path, monkeypatch, capsys
):
    mdir = _make_dir(tmp_path)
    cluster.migrate("pytest", mdir)
    (mdir / "002_b.sql").write_text("SELECT 22", encoding="utf8")

    assert _status(monkeypatch, capsys, mdir)[0] == 0
    assert _status(monkeypatch, capsys, mdir, "--exit-code-pending")[0] == 0
    code, captured = _status(monkeypatch, capsys, mdir, "--strict", "--format", "json")

    assert code == 1
    states = [m["state"] for m in json.loads(captured.out)["migrations"]]
    assert states == ["applied", "md5-mismatch"]


def test_status_strict_unknown(
    cluster: ClickhouseCluster, tmp_path, monkeypatch, capsys
):
    mdir = _make_dir(tmp_path)
    cluster.migrate("pytest", mdir)
    (mdir / "002_b.sql").unlink()
    (mdir / "001_a.down.sql").unlink()

    assert _status(monkeypatch, capsys, mdir)[0] == 0
    code, captured = _status(monkeypatch, capsys, mdir, "--strict", "--format", "json")

    assert code == 1
    migrations = json.loads(captured.out)["migrations"]
    assert [m["state"] for m in migrations] == ["applied", "unknown"]
    assert migrations[1]["has_down"] is False
    assert migrations[1]["applied_at"] is not None
