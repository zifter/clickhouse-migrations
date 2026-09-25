import json
import logging
import os
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

import clickhouse_migrations
from clickhouse_migrations import lock as lock_module
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.command_line import main
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.lock import KeeperMapLock
from tests.clickhouse_version import NEEDS_LOCK

pytestmark = NEEDS_LOCK

TESTS_DIR = Path(__file__).parents[1]
MIGRATIONS = TESTS_DIR / "migrations"
DOWN_MIGRATIONS = TESTS_DIR / "down_migrations"
PACKAGE_ROOT = str(Path(clickhouse_migrations.__file__).parents[1])

LOCK_TABLE = "schema_versions_lock"
OTHER_OWNER = "dead-pod:4242:11111111-2222-3333-4444-555555555555"

# Each concurrent run sleeps inside the migration, so two unlocked runs would
# demonstrably overlap and both write their own schema_versions rows.
SLOW_MIGRATIONS = {
    "001_slow_one.sql": "SELECT sleep(1);\n"
    "CREATE TABLE IF NOT EXISTS conc_one (i UInt32) ENGINE = MergeTree ORDER BY i;\n",
    "002_slow_two.sql": "SELECT sleep(1);\n"
    "CREATE TABLE IF NOT EXISTS conc_two (i UInt32) ENGINE = MergeTree ORDER BY i;\n",
    "003_slow_three.sql": "SELECT sleep(1);\n"
    "CREATE TABLE IF NOT EXISTS conc_three (i UInt32) ENGINE = MergeTree ORDER BY i;\n",
}

# Runs one migration in its own process. The processes wait for each other on a
# file barrier, so they really do start migrating at the same time instead of
# hoping that a sleep lines them up.
CHILD = """
import json, logging, os, sys, time
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster

migrations_dir, barrier_dir, worker, lock = sys.argv[1:5]
total = int(sys.argv[5])
# The log (who took the lock when) goes to stderr: failures show it.
logging.basicConfig(
    level=logging.INFO, stream=sys.stderr, format=f"%(asctime)s {worker} %(message)s"
)

(open(os.path.join(barrier_dir, worker), "w")).close()
deadline = time.time() + 120
while len(os.listdir(barrier_dir)) < total:
    if time.time() > deadline:
        raise SystemExit("barrier timed out")
    time.sleep(0.01)

cluster = ClickhouseCluster(db_host="localhost", db_user="default", db_password="")
try:
    applied = cluster.migrate(
        "pytest",
        migrations_dir,
        lock=lock == "on",
        lock_timeout=120,
    )
    print(json.dumps({"worker": worker, "applied": [m.version for m in applied]}))
except Exception as exc:  # noqa: BLE001
    print(json.dumps({"worker": worker, "error": str(exc)}))
"""


# A run that takes ~8s with a lock TTL of 3s (heartbeat every second): only
# the heartbeat keeps the lock from looking stale to a second run.
LONG_RUN_TTL = 3
LONG_MIGRATIONS = {
    "001_long_one.sql": "SELECT sleep(2);\nSELECT sleep(2);\n"
    "CREATE TABLE IF NOT EXISTS long_one (i UInt32) ENGINE = MergeTree ORDER BY i;\n",
    "002_long_two.sql": "SELECT sleep(2);\nSELECT sleep(2);\n"
    "CREATE TABLE IF NOT EXISTS long_two (i UInt32) ENGINE = MergeTree ORDER BY i;\n",
}


@pytest.fixture(name="slow_migrations")
def slow_migrations_fixture(tmp_path) -> Path:
    directory = tmp_path / "slow_migrations"
    directory.mkdir()
    for name, script in SLOW_MIGRATIONS.items():
        (directory / name).write_text(script, encoding="utf8")

    return directory


def run_concurrently(migrations_dir, tmp_path, workers=4, lock="on"):
    """Start `workers` migration processes that all start at the same moment."""
    barrier = tmp_path / f"barrier_{lock}"
    barrier.mkdir()

    env = dict(os.environ, PYTHONPATH=PACKAGE_ROOT)
    processes = [
        subprocess.Popen(  # pylint: disable=consider-using-with
            [
                sys.executable,
                "-c",
                CHILD,
                str(migrations_dir),
                str(barrier),
                f"worker{index}",
                lock,
                str(workers),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True,
        )
        for index in range(workers)
    ]

    results = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=300)
        assert process.returncode == 0, stderr
        result = json.loads(stdout.strip().splitlines()[-1])
        result["log"] = stderr
        results.append(result)

    return results


def schema_version_rows(cluster, table="schema_versions"):
    with cluster.connection("pytest") as conn:
        return [
            (row["version"], row["n"])
            for row in conn.query(
                f"SELECT version, count() AS n FROM {table} "
                "GROUP BY version ORDER BY version"
            )
        ]


@contextmanager
def foreign_lock(cluster, owner=OTHER_OWNER, migrations_table="schema_versions"):
    """Hold the lock under somebody else's name, like another pod would."""
    with cluster.connection("pytest") as conn:
        held = KeeperMapLock(
            conn,
            "pytest",
            migrations_table=migrations_table,
            timeout=0,
            owner=owner,
        )
        assert held.acquire() is True
        try:
            yield held
        finally:
            held.release()


def current_holder(cluster, migrations_table="schema_versions"):
    with cluster.connection("pytest") as conn:
        return KeeperMapLock(conn, "pytest", migrations_table=migrations_table).holder()


def test_migrate_creates_the_lock_table_and_leaves_it_empty(cluster):
    cluster.migrate("pytest", MIGRATIONS, lock=True)

    assert LOCK_TABLE in cluster.show_tables("pytest")
    assert current_holder(cluster) is None


def test_concurrent_migrations_are_applied_exactly_once(
    cluster, slow_migrations, tmp_path
):
    results = run_concurrently(slow_migrations, tmp_path, workers=4)
    logs = "".join(sorted(r["log"] for r in results))

    assert not [r for r in results if "error" in r], logs
    # The lock serialises the runs: whoever gets in first applies everything,
    # the others find nothing left to do.
    applied = sorted(len(r["applied"]) for r in results)
    assert applied == [0, 0, 0, 3], logs
    # Without the lock every worker would insert its own bookkeeping rows.
    assert schema_version_rows(cluster) == [(1, 1), (2, 1), (3, 1)]
    assert {"conc_one", "conc_two", "conc_three"} <= set(cluster.show_tables("pytest"))
    assert current_holder(cluster) is None


def race_for_the_lock(racer, workers):
    """``workers`` threads try to take the lock at the same moment, each over
    its own connection; returns the owners that were told they took it."""
    start = threading.Barrier(workers)
    finished = threading.Barrier(workers)
    won = {}

    def attempt(index):
        with racer.connection("pytest") as conn:
            lock = KeeperMapLock(conn, "pytest", timeout=0, owner=f"racer-{index}")
            start.wait(timeout=60)
            try:
                won[lock.owner] = lock.acquire()
            except MigrationException:
                won[lock.owner] = False
            # Nobody releases before everybody has tried: a late attempt must
            # not win a lock that was already given back.
            finished.wait(timeout=60)
            if won[lock.owner]:
                lock.release()

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)

    assert len(won) == workers
    return sorted(owner for owner, took in won.items() if took)


@pytest.mark.parametrize(
    "driver, port", [("clickhouse-driver", "9000"), ("clickhouse-connect", "8123")]
)
def test_concurrent_acquire_has_one_winner_with_async_inserts_on(driver, port, cluster):
    # Asynchronous inserts (on by default on 26.3+, forced on here so every
    # server version runs this) batch concurrent inserts of the lock row into
    # one block, and every writer would be told it took the lock. The lock's
    # own insert overrides the setting with async_insert = 0.
    racer = ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_port=port,
        driver=driver,
        settings={"async_insert": "1", "wait_for_async_insert": "1"},
    )
    with cluster.connection("pytest") as conn:
        # Create the lock table up front, so the racers start level.
        assert KeeperMapLock(conn, "pytest").unsupported_reason() is None

    for _ in range(5):
        winners = race_for_the_lock(racer, workers=4)
        assert len(winners) == 1, winners
        assert current_holder(cluster) is None


def test_timeout_names_the_owner_and_how_long_it_held_the_lock(cluster):
    with foreign_lock(cluster):
        with pytest.raises(MigrationException) as exc_info:
            cluster.migrate("pytest", MIGRATIONS, lock=True, lock_timeout=0)

    message = str(exc_info.value)
    assert OTHER_OWNER in message
    assert "has held it for" in message
    assert "clickhouse-migrations unlock" in message
    # The failed acquire must not have touched anybody else's lock.
    assert "schema_versions" not in cluster.show_tables("pytest")


def test_stale_lock_is_taken_over_with_a_warning(cluster, caplog):
    with caplog.at_level(logging.WARNING):
        with foreign_lock(cluster):
            # lock_ttl = 0 makes the foreign lock stale right away.
            applied = cluster.migrate(
                "pytest", MIGRATIONS, lock=True, lock_ttl=0, lock_timeout=5
            )

    assert len(applied) == 1
    assert "stale migration lock" in caplog.text
    assert OTHER_OWNER in caplog.text
    assert current_holder(cluster) is None


def test_lock_is_released_when_a_migration_fails(cluster, tmp_path):
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "001_broken.sql").write_text("NOT VALID SQL;\n", encoding="utf8")

    with pytest.raises(Exception):  # noqa: B017 - a driver error, not ours
        cluster.migrate("pytest", broken, lock=True, lock_timeout=0)

    assert current_holder(cluster) is None
    # ... and the next run is not blocked by the failed one.
    assert len(cluster.migrate("pytest", MIGRATIONS, lock=True, lock_timeout=0)) == 1


def test_unlock_subcommand_force_releases_a_foreign_lock(cluster, monkeypatch, capsys):
    with foreign_lock(cluster):
        monkeypatch.setattr(
            sys,
            "argv",
            ["clickhouse-migrations", "unlock", "--db-name", "pytest"],
        )

        assert main() == 0

        out = capsys.readouterr().out
        assert OTHER_OWNER in out
        assert current_holder(cluster) is None

    # A second unlock has nothing to release.
    assert main() == 0
    assert "No migration lock is held." in capsys.readouterr().out


def test_locking_is_off_by_default_and_does_no_lock_work(cluster, monkeypatch):
    # Every lock code path starts with this probe (engine check + CREATE).
    probes = []
    monkeypatch.setattr(
        KeeperMapLock, "unsupported_reason", lambda self: probes.append(1)
    )

    assert len(cluster.migrate("pytest", MIGRATIONS)) == 1

    assert not probes

    tables = cluster.show_tables("pytest")
    assert "schema_versions" in tables
    assert LOCK_TABLE not in tables


def test_default_run_ignores_a_lock_held_by_somebody_else(cluster):
    # Opt-in means opt-in: an unlocked run is not blocked (and is unsafe).
    with foreign_lock(cluster):
        assert len(cluster.migrate("pytest", MIGRATIONS)) == 1


def test_dry_run_never_takes_the_lock(cluster):
    with foreign_lock(cluster):
        cluster.migrate("pytest", MIGRATIONS, dryrun=True, lock=True, lock_timeout=0)

    assert LOCK_TABLE in cluster.show_tables("pytest")
    assert "sample" not in cluster.show_tables("pytest")


def _break_keeper_map(monkeypatch):
    # An empty ZooKeeper path is rejected by the server, which is how a server
    # without <keeper_map_path_prefix> behaves: the CREATE is what fails.
    monkeypatch.setattr(
        KeeperMapLock, "_build_keeper_path", staticmethod(lambda *args: "")
    )


def test_lock_fails_when_keeper_map_is_unusable(cluster, monkeypatch):
    _break_keeper_map(monkeypatch)

    with pytest.raises(MigrationException) as exc_info:
        cluster.migrate("pytest", MIGRATIONS, lock=True)

    message = str(exc_info.value)
    assert "requested explicitly" in message
    assert "ZooKeeper path should not be empty" in message
    # Nothing was migrated: the run stops before it touches the schema.
    assert "schema_versions" not in cluster.show_tables("pytest")


@pytest.mark.parametrize(
    "driver, port", [("clickhouse-driver", "9000"), ("clickhouse-connect", "8123")]
)
def test_lock_works_with_both_drivers(driver, port, cluster):
    driver_cluster = ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        db_port=port,
        driver=driver,
    )

    driver_cluster.migrate("pytest", MIGRATIONS, lock=True, lock_timeout=0)
    assert LOCK_TABLE in driver_cluster.show_tables("pytest")

    # A lock taken over one driver is seen by the other: it lives in Keeper.
    with foreign_lock(cluster):
        with pytest.raises(MigrationException, match=OTHER_OWNER):
            driver_cluster.migrate("pytest", MIGRATIONS, lock=True, lock_timeout=0)

    assert current_holder(driver_cluster) is None


def test_lock_follows_a_custom_migrations_table(cluster):
    custom = ClickhouseCluster(
        db_host="localhost",
        db_user="default",
        db_password="",
        db_name="pytest",
        migrations_table="my_versions",
    )
    custom.migrate("pytest", MIGRATIONS, lock=True, lock_timeout=0)

    tables = custom.show_tables("pytest")
    assert "my_versions_lock" in tables
    assert LOCK_TABLE not in tables

    # The default lock is a different lock, so it does not block this one.
    with foreign_lock(cluster):
        assert custom.migrate("pytest", MIGRATIONS, lock=True, lock_timeout=0) == []

    with foreign_lock(cluster, migrations_table="my_versions"):
        with pytest.raises(MigrationException, match=OTHER_OWNER):
            custom.migrate("pytest", MIGRATIONS, lock=True, lock_timeout=0)


def test_rollback_takes_the_lock(cluster):
    cluster.migrate("pytest", DOWN_MIGRATIONS, lock=True, lock_timeout=0)

    with foreign_lock(cluster):
        with pytest.raises(MigrationException, match=OTHER_OWNER):
            cluster.rollback(
                "pytest", DOWN_MIGRATIONS, steps=1, lock=True, lock_timeout=0
            )

    assert cluster.rollback(
        "pytest", DOWN_MIGRATIONS, steps=1, lock=True, lock_timeout=0
    ) == [2]
    assert current_holder(cluster) is None


def test_rollback_dry_run_never_takes_the_lock(cluster):
    cluster.migrate("pytest", DOWN_MIGRATIONS, lock=True, lock_timeout=0)

    with foreign_lock(cluster):
        assert cluster.rollback(
            "pytest",
            DOWN_MIGRATIONS,
            steps=1,
            dryrun=True,
            lock=True,
            lock_timeout=0,
        ) == [2]


def test_lock_on_cluster_needs_no_on_cluster_ddl(cluster, _schema):
    # KeeperMap is Keeper-backed, so one plain CREATE on the connected node is
    # enough even in ON CLUSTER mode.
    cluster.migrate(
        "pytest",
        MIGRATIONS,
        cluster_name="company_cluster",
        lock=True,
        lock_timeout=0,
    )

    assert LOCK_TABLE in cluster.show_tables("pytest")
    assert current_holder(cluster) is None
    assert lock_module.LOCK_NAME == "migrate"


@pytest.fixture(name="long_migrations")
def long_migrations_fixture(tmp_path) -> Path:
    directory = tmp_path / "long_migrations"
    directory.mkdir()
    for name, script in LONG_MIGRATIONS.items():
        (directory / name).write_text(script, encoding="utf8")

    return directory


class LongRun(threading.Thread):
    """One locked migration run with the short TTL, in a thread of its own."""

    def __init__(self, migrations_dir):
        super().__init__(daemon=True)
        self.migrations_dir = migrations_dir
        self.applied = None
        self.waited = None

    def run(self):
        started = time.monotonic()
        cluster = ClickhouseCluster(
            db_host="localhost", db_user="default", db_password=""
        )
        applied = cluster.migrate(
            "pytest",
            self.migrations_dir,
            lock=True,
            lock_ttl=LONG_RUN_TTL,
            lock_timeout=60,
        )
        self.applied = [migration.version for migration in applied]
        self.waited = time.monotonic() - started


def race_a_long_run(cluster, migrations_dir):
    """Start a second run while a long one holds the lock; sample the lock.

    Returns both runs and the ages of the lock seen while the first one ran.
    """
    with cluster.connection("pytest") as conn:
        # Create the lock table up front so it can be sampled from the start.
        assert KeeperMapLock(conn, "pytest").unsupported_reason() is None

    first, second = LongRun(migrations_dir), LongRun(migrations_dir)
    first.start()
    ages = []
    while first.is_alive():
        holder = current_holder(cluster)
        if holder is not None:
            ages.append(holder.age)
            if second.ident is None:
                # The first run holds the lock: now the second one competes.
                second.start()
        time.sleep(0.25)

    first.join(60)
    second.join(60)
    return first, second, ages


def test_a_run_longer_than_the_ttl_keeps_its_lock(cluster, long_migrations, caplog):
    with caplog.at_level(logging.INFO):
        first, second, ages = race_a_long_run(cluster, long_migrations)

    assert first.applied == [1, 2]
    # The second run waited for the whole first run instead of taking over...
    assert second.applied == []
    assert second.waited > LONG_RUN_TTL + 1
    assert "stale migration lock" not in caplog.text
    assert "taken over" not in caplog.text
    # ... because the heartbeat never let the lock get anywhere near the TTL.
    assert "every 1s" in caplog.text
    assert ages and max(ages) < LONG_RUN_TTL
    assert schema_version_rows(cluster) == [(1, 1), (2, 1)]
    assert current_holder(cluster) is None


def test_without_the_heartbeat_the_same_run_loses_its_lock(
    cluster, long_migrations, caplog, monkeypatch
):
    # The control for the test above: a refresh that changes nothing is the
    # behaviour before the heartbeat existed.
    monkeypatch.setattr(KeeperMapLock, "refresh", KeeperMapLock.holder)

    with caplog.at_level(logging.INFO):
        first, second, ages = race_a_long_run(cluster, long_migrations)

    assert first.applied == [1, 2]
    # The second run took the "stale" lock over while the first still ran and
    # applied migration 2 a second time - what the lock exists to prevent...
    assert "stale migration lock" in caplog.text
    assert max(ages) >= LONG_RUN_TTL
    assert 2 in second.applied
    assert (2, 2) in schema_version_rows(cluster)
    # ... and the first run's heartbeat noticed and said so.
    assert "was taken over by" in caplog.text
    assert "concurrent runs are no longer excluded" in caplog.text
    assert current_holder(cluster) is None


def test_heartbeat_refreshes_the_lock_on_the_server(cluster):
    # Both drivers' clients accept the refresh statement, and it really moves
    # acquired_at of our own row only.
    for driver, port in (("clickhouse-driver", "9000"), ("clickhouse-connect", "8123")):
        driver_cluster = ClickhouseCluster(
            db_host="localhost",
            db_user="default",
            db_password="",
            db_port=port,
            driver=driver,
        )
        with driver_cluster.connection("pytest") as conn:
            held = KeeperMapLock(conn, "pytest", timeout=0, owner=f"me-{driver}")
            assert held.acquire() is True
            conn.command(
                f"ALTER TABLE {held.table} UPDATE acquired_at = now() - 100 "
                "WHERE name = 'migrate' SETTINGS keeper_map_strict_mode = 1"
            )
            assert current_holder(cluster).age >= 100

            refreshed = held.refresh()

            assert refreshed.owner == f"me-{driver}"
            assert refreshed.age <= 1
            held.release()
            # A released lock is never recreated by a late refresh.
            assert held.refresh() is None
            assert current_holder(cluster) is None

    with foreign_lock(cluster) as foreign:
        with cluster.connection("pytest") as conn:
            mine = KeeperMapLock(conn, "pytest", timeout=0, owner="me")
            assert mine.refresh().owner == OTHER_OWNER
        assert current_holder(cluster).acquired_ts == foreign.holder().acquired_ts
