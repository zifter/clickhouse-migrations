import logging
import re
import types
from typing import Dict, List, Optional, Tuple

import pytest

from clickhouse_migrations import lock as lock_module
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.connection import Connection
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.lock import (
    LOCK_NAME,
    KeeperMapLock,
    LockHolder,
    make_owner,
)

# Error text of a real ClickHouse 25.7 server when a second writer inserts an
# existing key with keeper_map_strict_mode = 1.
NODE_EXISTS = (
    "Code: 999. DB::Exception: Coordination::Exception: "
    "Transaction failed (Node exists): Op #0, path: /keeper_map_tables/x/data/y. "
    "(KEEPER_EXCEPTION)"
)

NOW = 1_700_000_000


class FakeKeeperMap(Connection):
    """In-memory stand-in for a KeeperMap-backed lock table.

    Only the statements the lock itself issues are understood; anything else
    raises, so a change of the SQL cannot silently pass the unit tests.
    """

    def __init__(self, has_engine: bool = True, now: int = NOW):
        self.has_engine = has_engine
        self.now = now
        self.row: Optional[Dict] = None
        self.table_created = False
        self.statements: List[str] = []
        # (substring, exception) - the next statement containing the substring
        # fails, the way a broken connection would.
        self.fail: Optional[Tuple[str, Exception]] = None
        self.on_insert = None

    # -- helpers -------------------------------------------------------
    @staticmethod
    def _literals(statement: str) -> List[str]:
        return re.findall(r"'([^']*)'", statement)

    def set_row(self, owner: str, acquired_at: Optional[int] = None) -> None:
        self.row = {
            "owner": owner,
            "acquired_at": self.now if acquired_at is None else acquired_at,
        }

    # -- Connection ----------------------------------------------------
    def command(self, statement: str) -> None:
        self.statements.append(statement)
        if self.fail is not None and self.fail[0] in statement:
            raise self.fail[1]

        if statement.startswith("CREATE TABLE"):
            self.table_created = True
            return

        if statement.startswith("INSERT"):
            if self.on_insert is not None:
                self.on_insert()
            if self.row is not None:
                raise RuntimeError(NODE_EXISTS)
            name, owner = self._literals(statement)
            assert name == LOCK_NAME
            self.set_row(owner)
            return

        if statement.startswith("DELETE"):
            self._delete(statement)
            return

        raise AssertionError(f"unexpected statement: {statement}")

    def _delete(self, statement: str) -> None:
        if self.row is None:
            return

        literals = self._literals(statement)
        owner = literals[1] if len(literals) > 1 else None
        if owner is not None and owner != self.row["owner"]:
            return

        acquired = re.search(r"toUnixTimestamp\(acquired_at\) = (\d+)", statement)
        if acquired is not None and int(acquired.group(1)) != self.row["acquired_at"]:
            return

        self.row = None

    def query(self, statement: str) -> List[Dict]:
        self.statements.append(statement)
        if "system.table_engines" in statement:
            return [{"n": 1 if self.has_engine else 0}]

        if "system.tables" in statement:
            return [{"n": 1 if self.table_created else 0}]

        if self.row is None:
            return []

        return [
            {
                "owner": self.row["owner"],
                "acquired_ts": self.row["acquired_at"],
                "age": self.now - self.row["acquired_at"],
            }
        ]

    def insert(self, table: str, rows: List[Dict]) -> None:
        raise AssertionError("the lock never uses the insert() API")

    def __enter__(self) -> "FakeKeeperMap":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None


class FakeClock:
    """Deterministic replacement for the lock's use of the time module."""

    def __init__(self):
        self.value = 0.0
        self.slept: List[float] = []
        self.on_sleep = None

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.value += seconds
        if self.on_sleep is not None:
            self.on_sleep()


@pytest.fixture(name="clock")
def clock_fixture(monkeypatch) -> FakeClock:
    clock = FakeClock()
    monkeypatch.setattr(
        lock_module,
        "time",
        types.SimpleNamespace(monotonic=clock.monotonic, sleep=clock.sleep),
    )
    return clock


def broken_create(message="keeper_map_path_prefix config is not defined"):
    """A connection whose CREATE TABLE fails, as on a server without Keeper."""
    conn = FakeKeeperMap()
    conn.fail = ("CREATE TABLE", RuntimeError(message))
    return conn


def build_lock(conn, owner="me", timeout=0, ttl=3600, db_name="pytest", **kwargs):
    return KeeperMapLock(
        conn,
        db_name,
        timeout=timeout,
        ttl=ttl,
        owner=owner,
        **kwargs,
    )


def test_make_owner_is_host_pid_uuid():
    owner = make_owner()

    host, pid, unique = owner.split(":")
    assert host and pid.isdigit() and len(unique) == 36
    assert make_owner() != owner


def test_lock_table_and_keeper_path_default():
    conn = FakeKeeperMap()
    migration_lock = build_lock(conn)

    assert migration_lock.table == '"pytest"."schema_versions_lock"'
    migration_lock.acquire()

    create = conn.statements[1]
    assert "ENGINE = KeeperMap('/clickhouse-migrations/pytest')" in create


def test_lock_table_follows_custom_migrations_table():
    conn = FakeKeeperMap()
    migration_lock = KeeperMapLock(conn, "pytest", migrations_table="meta.my_versions")

    assert migration_lock.table == '"meta"."my_versions_lock"'
    migration_lock.acquire()

    assert (
        "ENGINE = KeeperMap('/clickhouse-migrations/meta/my_versions')"
        in conn.statements[1]
    )


def test_keeper_path_escapes_odd_database_names():
    conn = FakeKeeperMap()
    KeeperMapLock(conn, "we/ird db").acquire()

    assert "KeeperMap('/clickhouse-migrations/we%2Fird%20db')" in conn.statements[1]


def test_acquire_and_release_round_trip():
    conn = FakeKeeperMap()
    migration_lock = build_lock(conn)

    assert migration_lock.acquire() is True
    assert migration_lock.acquired is True
    assert conn.row["owner"] == "me"
    assert migration_lock.holder() == LockHolder("me", NOW, 0)

    migration_lock.release()

    assert conn.row is None
    assert migration_lock.acquired is False


def test_release_without_acquire_is_a_no_op():
    conn = FakeKeeperMap()
    conn.set_row("somebody-else")
    statements = len(conn.statements)

    build_lock(conn).release()

    assert len(conn.statements) == statements
    assert conn.row["owner"] == "somebody-else"


def test_release_never_drops_another_owners_lock():
    conn = FakeKeeperMap()
    migration_lock = build_lock(conn)
    migration_lock.acquire()
    # The lock was taken over while we were migrating.
    conn.set_row("other")

    migration_lock.release()

    assert conn.row["owner"] == "other"


def test_release_failure_is_reported_and_swallowed(caplog):
    conn = FakeKeeperMap()
    migration_lock = build_lock(conn)
    migration_lock.acquire()
    conn.fail = ("DELETE", RuntimeError("connection lost"))

    with caplog.at_level(logging.WARNING):
        migration_lock.release()

    assert "Failed to release the migration lock" in caplog.text
    assert "unlock" in caplog.text


def test_timeout_names_the_owner_and_the_age(clock):
    conn = FakeKeeperMap()
    conn.set_row("pod-a:7:uuid", acquired_at=NOW - 42)
    migration_lock = build_lock(conn, timeout=0)

    with pytest.raises(MigrationException) as exc_info:
        migration_lock.acquire()

    message = str(exc_info.value)
    assert "pod-a:7:uuid" in message
    assert "42s" in message
    assert "within 0s" in message
    assert "clickhouse-migrations unlock" in message
    assert clock.slept == []


@pytest.mark.usefixtures("clock")
def test_timeout_without_a_visible_holder():
    conn = FakeKeeperMap()
    # The insert conflicts but the row is gone again by the time we look.
    conn.fail = ("INSERT", RuntimeError(NODE_EXISTS))
    migration_lock = build_lock(conn, timeout=0)

    with pytest.raises(MigrationException, match="another migration run"):
        migration_lock.acquire()


def test_waits_until_the_other_run_releases(clock):
    conn = FakeKeeperMap()
    conn.set_row("other")
    clock.on_sleep = lambda: setattr(conn, "row", None)
    migration_lock = build_lock(conn, timeout=60, poll_interval=1)

    assert migration_lock.acquire() is True
    assert clock.slept == [1]
    assert conn.row["owner"] == "me"


def test_last_poll_never_sleeps_past_the_deadline(clock):
    conn = FakeKeeperMap()
    conn.set_row("other")
    migration_lock = build_lock(conn, timeout=2, poll_interval=10)

    with pytest.raises(MigrationException):
        migration_lock.acquire()

    assert clock.slept == [2]


@pytest.mark.usefixtures("clock")
def test_stale_lock_is_taken_over_with_a_warning(caplog):
    conn = FakeKeeperMap()
    conn.set_row("dead-pod:1:uuid", acquired_at=NOW - 7200)
    migration_lock = build_lock(conn, timeout=0, ttl=3600)

    with caplog.at_level(logging.WARNING):
        assert migration_lock.acquire() is True

    assert "stale migration lock" in caplog.text
    assert "dead-pod:1:uuid" in caplog.text
    assert "7200s" in caplog.text
    assert conn.row["owner"] == "me"


@pytest.mark.usefixtures("clock")
def test_stale_takeover_lets_only_one_waiter_win(caplog):
    conn = FakeKeeperMap()
    conn.set_row("dead-pod:1:uuid", acquired_at=NOW - 7200)
    stale = LockHolder("dead-pod:1:uuid", NOW - 7200, 7200)

    first = build_lock(conn, owner="a", timeout=0, ttl=3600)
    second = build_lock(conn, owner="b", timeout=0, ttl=3600)

    # Both waiters observed the same stale lock. "a" deletes it first; "b"
    # slips in with its own (now redundant) delete and insert while "a" is
    # between its compare-and-delete and its own insert.
    attempts = []

    def race():
        attempts.append(1)
        if len(attempts) != 2:
            return

        second._steal(stale)  # pylint: disable=protected-access
        assert second.acquire() is True

    conn.on_insert = race

    with caplog.at_level(logging.WARNING):
        with pytest.raises(MigrationException) as exc_info:
            first.acquire()

    # The loser never overwrites the winner and is told who holds the lock.
    assert conn.row["owner"] == "b"
    assert "b" in str(exc_info.value)
    assert first.acquired is False
    assert second.acquired is True


def test_stale_takeover_does_not_delete_a_fresh_lock():
    conn = FakeKeeperMap()
    migration_lock = build_lock(conn)
    stale = LockHolder("dead-pod:1:uuid", NOW - 7200, 7200)
    conn.set_row("dead-pod:1:uuid", acquired_at=NOW)

    migration_lock._steal(stale)  # pylint: disable=protected-access

    # Same owner, but a newer acquired_at: the compare-and-delete misses.
    assert conn.row["owner"] == "dead-pod:1:uuid"


@pytest.mark.usefixtures("clock")
def test_non_conflict_insert_error_is_reported():
    conn = FakeKeeperMap()
    conn.fail = ("INSERT", RuntimeError("Code: 210. Connection refused"))
    migration_lock = build_lock(conn)

    with pytest.raises(MigrationException, match="Failed to take the migration lock"):
        migration_lock.acquire()


@pytest.mark.parametrize(
    "conn, reason",
    [
        (FakeKeeperMap(has_engine=False), "KeeperMap table engine"),
        (broken_create(), "keeper_map_path_prefix"),
    ],
)
def test_lock_fails_when_the_server_cannot_provide_it(conn, reason):
    # Locking is opt-in, so asking for it and not getting it is an error -
    # there is no "warn and continue" fallback.
    migration_lock = build_lock(conn)

    with pytest.raises(MigrationException) as exc_info:
        migration_lock.acquire()

    message = str(exc_info.value)
    assert "requested explicitly" in message
    assert reason in message
    assert "without --lock" in message
    assert migration_lock.acquired is False


def test_force_release_returns_none_without_a_lock_table():
    conn = FakeKeeperMap()

    assert build_lock(conn).force_release() is None


def test_force_release_returns_none_when_nothing_is_held():
    conn = FakeKeeperMap()
    conn.table_created = True

    assert build_lock(conn).force_release() is None


def test_force_release_drops_someone_elses_lock():
    conn = FakeKeeperMap()
    conn.table_created = True
    conn.set_row("dead-pod:1:uuid", acquired_at=NOW - 10)

    holder = build_lock(conn).force_release()

    assert holder == LockHolder("dead-pod:1:uuid", NOW - 10, 10)
    assert conn.row is None


def _cluster(conn, **kwargs):
    cluster = ClickhouseCluster(db_host="localhost", db_name="pytest", **kwargs)
    cluster.connection = lambda db_name=None: conn  # type: ignore[method-assign]
    return cluster


def test_cluster_lock_is_off_by_default():
    conn = FakeKeeperMap()

    with _cluster(conn).migration_lock() as migration_lock:
        assert migration_lock is None

    # Nothing lock-related is sent to the server unless the lock is asked for.
    assert not conn.statements


def test_cluster_lock_is_skipped_when_disabled():
    conn = FakeKeeperMap()

    with _cluster(conn).migration_lock(lock=True, enabled=False) as migration_lock:
        assert migration_lock is None

    assert not conn.statements


def test_cluster_lock_is_skipped_with_no_lock():
    conn = FakeKeeperMap()

    with _cluster(conn).migration_lock(lock=False) as migration_lock:
        assert migration_lock is None

    assert not conn.statements


def test_cluster_lock_is_taken_and_released():
    conn = FakeKeeperMap()

    with _cluster(conn).migration_lock(lock=True) as migration_lock:
        assert migration_lock.acquired is True
        assert conn.row["owner"] == migration_lock.owner

    assert conn.row is None


def test_cluster_lock_uses_the_migrations_table_name():
    conn = FakeKeeperMap()

    with _cluster(conn, migrations_table="my_versions").migration_lock(
        lock=True
    ) as held:
        assert held.table == '"pytest"."my_versions_lock"'


def test_cluster_lock_is_released_when_the_migration_fails():
    conn = FakeKeeperMap()

    with pytest.raises(RuntimeError, match="boom"):
        with _cluster(conn).migration_lock(lock=True):
            raise RuntimeError("boom")

    assert conn.row is None


def test_cluster_lock_is_released_on_keyboard_interrupt():
    conn = FakeKeeperMap()

    with pytest.raises(KeyboardInterrupt):
        with _cluster(conn).migration_lock(lock=True):
            raise KeyboardInterrupt

    assert conn.row is None


def test_cluster_lock_failure_does_not_release_someone_elses_lock():
    conn = FakeKeeperMap()
    conn.set_row("other", acquired_at=NOW - 5)

    # Enter directly: acquiring fails, so there is no body to run.
    with pytest.raises(MigrationException, match="other"):
        _cluster(conn).migration_lock(lock=True, lock_timeout=0).__enter__()

    assert conn.row["owner"] == "other"


def test_the_fake_rejects_statements_the_lock_never_issues():
    conn = FakeKeeperMap()

    with pytest.raises(AssertionError, match="unexpected statement"):
        conn.command("TRUNCATE TABLE x")

    with pytest.raises(AssertionError, match="insert"):
        conn.insert("x", [{"a": 1}])


def test_cluster_force_unlock():
    conn = FakeKeeperMap()
    conn.table_created = True
    conn.set_row("dead-pod:1:uuid", acquired_at=NOW - 10)
    cluster = _cluster(conn)

    assert cluster.force_unlock().owner == "dead-pod:1:uuid"
    assert cluster.force_unlock() is None
