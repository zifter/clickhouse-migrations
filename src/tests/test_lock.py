import logging
import re
import threading
import types
from typing import Dict, List, Optional, Tuple

import pytest
from clickhouse_driver.errors import ServerException

from clickhouse_migrations import lock as lock_module
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from clickhouse_migrations.connection import Connection
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.lock import (
    LOCK_NAME,
    KeeperMapLock,
    LockHeartbeat,
    LockHolder,
    heartbeat_interval,
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


class FakeKeeperMap(Connection):  # pylint: disable=too-many-instance-attributes
    """In-memory stand-in for a KeeperMap-backed lock table.

    Only the statements the lock itself issues are understood; anything else
    raises, so a change of the SQL cannot silently pass the unit tests.
    """

    def __init__(
        self, has_engine: bool = True, now: int = NOW, has_strict_mode: bool = True
    ):
        self.has_engine = has_engine
        self.has_strict_mode = has_strict_mode
        self.now = now
        self.row: Optional[Dict] = None
        self.table_created = False
        self.statements: List[str] = []
        # (substring, exception) - the next statement containing the substring
        # fails, the way a broken connection would.
        self.fail: Optional[Tuple[str, Exception]] = None
        self.on_insert = None
        # Called before every refresh (ALTER TABLE ... UPDATE); may raise.
        self.on_alter = None

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
            # A strict, synchronous insert: an asynchronous one (the server
            # default on 26.3+) is batched and every concurrent writer wins.
            assert "SETTINGS keeper_map_strict_mode = 1, async_insert = 0" in statement
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

        if statement.startswith("ALTER TABLE"):
            self._update(statement)
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

    def _update(self, statement: str) -> None:
        # The refresh must be a compare-and-set on our own row.
        assert "SETTINGS keeper_map_strict_mode = 1" in statement
        assert "UPDATE acquired_at = now()" in statement
        if self.on_alter is not None:
            self.on_alter()

        name, owner = self._literals(statement)
        assert name == LOCK_NAME
        if self.row is not None and self.row["owner"] == owner:
            self.row["acquired_at"] = self.now

    def query(self, statement: str) -> List[Dict]:
        self.statements.append(statement)
        if "system.table_engines" in statement:
            assert "system.settings WHERE name = 'keeper_map_strict_mode'" in statement
            return [
                {
                    "n": 1 if self.has_engine else 0,
                    "strict": 1 if self.has_strict_mode else 0,
                }
            ]

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
@pytest.mark.parametrize(
    "error",
    [
        RuntimeError(NODE_EXISTS),
        # clickhouse-driver against ClickHouse 23.8: no error name, no reason.
        ServerException(
            "Coordination::Exception. Coordination::Exception: Transaction "
            "failed: Op #0, path: /keeper_map_tables/x/data/y. Stack trace:",
            code=999,
        ),
    ],
    ids=["node-exists", "code-999-without-reason"],
)
def test_timeout_without_a_visible_holder(error):
    conn = FakeKeeperMap()
    # The insert conflicts but the row is gone again by the time we look.
    conn.fail = ("INSERT", error)
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
        (
            FakeKeeperMap(has_engine=False),
            "no KeeperMap table engine (ClickHouse 23.8+",
        ),
        # ClickHouse 23.3: the engine exists, the strict mode does not.
        (
            FakeKeeperMap(has_strict_mode=False),
            "no keeper_map_strict_mode setting (ClickHouse 23.8+",
        ),
        (broken_create(), "keeper_map_path_prefix"),
    ],
    ids=["no-engine", "no-strict-mode", "no-path-prefix"],
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


def test_heartbeat_interval_is_a_third_of_the_ttl_but_at_least_a_second():
    assert heartbeat_interval(3600) == 1200
    assert heartbeat_interval(3) == 1
    assert heartbeat_interval(1) == 1
    assert heartbeat_interval(0) == 1
    assert LockHeartbeat(build_lock(FakeKeeperMap(), ttl=30)).interval == 10
    assert LockHeartbeat(build_lock(FakeKeeperMap()), interval=0.5).interval == 0.5


def test_refresh_moves_acquired_at_of_our_own_row():
    conn = FakeKeeperMap()
    migration_lock = build_lock(conn)
    migration_lock.acquire()
    conn.now += 100

    assert migration_lock.refresh() == LockHolder("me", NOW + 100, 0)
    assert 'ALTER TABLE "pytest"."schema_versions_lock" UPDATE' in (conn.statements[-2])
    assert "AND owner = 'me'" in conn.statements[-2]


def test_refresh_never_touches_somebody_elses_row():
    conn = FakeKeeperMap()
    conn.set_row("other", acquired_at=NOW - 50)

    assert build_lock(conn).refresh() == LockHolder("other", NOW - 50, 50)
    assert conn.row == {"owner": "other", "acquired_at": NOW - 50}


def test_refresh_never_recreates_a_released_lock():
    conn = FakeKeeperMap()

    assert build_lock(conn).refresh() is None
    assert conn.row is None


def test_bound_to_is_the_same_lock_on_another_connection():
    conn, other_conn = FakeKeeperMap(), FakeKeeperMap()
    migration_lock = build_lock(conn, ttl=30)
    other_conn.set_row("me")

    bound = migration_lock.bound_to(other_conn)

    assert (bound.table, bound.owner, bound.ttl) == (migration_lock.table, "me", 30)
    bound.refresh()
    assert not conn.statements
    assert len(other_conn.statements) == 2


def held_heartbeat(conn=None, **kwargs):
    conn = conn if conn is not None else FakeKeeperMap()
    migration_lock = build_lock(conn)
    migration_lock.acquire()
    return conn, LockHeartbeat(migration_lock, **kwargs)


def test_beat_keeps_our_lock_fresh():
    conn, heartbeat = held_heartbeat()
    conn.now += 10

    assert heartbeat.beat() is True
    assert heartbeat.beat() is True

    assert conn.row["acquired_at"] == NOW + 10
    assert heartbeat.refreshed == 2
    assert heartbeat.lost is False


def test_beat_reports_a_lock_taken_over_and_stops(caplog):
    conn, heartbeat = held_heartbeat()
    conn.set_row("thief:9:uuid")

    with caplog.at_level(logging.ERROR):
        assert heartbeat.beat() is False

    assert heartbeat.lost is True
    assert "was taken over by thief:9:uuid" in caplog.text
    assert "held by me" in caplog.text
    assert "the migration continues" in caplog.text
    # Somebody else's lock is left alone.
    assert conn.row["owner"] == "thief:9:uuid"


def test_beat_reports_a_lock_that_is_gone_and_stops(caplog):
    conn, heartbeat = held_heartbeat()
    conn.row = None

    with caplog.at_level(logging.ERROR):
        assert heartbeat.beat() is False

    assert heartbeat.lost is True
    assert "is gone" in caplog.text
    assert "unlock" in caplog.text
    assert conn.row is None


def test_beat_retries_after_a_transient_error(caplog):
    conn, heartbeat = held_heartbeat(interval=7)
    conn.fail = ("ALTER TABLE", RuntimeError("Code: 210. Connection reset"))
    conn.now += 10

    with caplog.at_level(logging.WARNING):
        assert heartbeat.beat() is True

    assert "Failed to refresh the migration lock" in caplog.text
    assert "Connection reset" in caplog.text
    assert "Retrying in 7s" in caplog.text
    assert heartbeat.lost is False

    conn.fail = None
    assert heartbeat.beat() is True
    assert conn.row["acquired_at"] == NOW + 10
    assert heartbeat.refreshed == 1


def test_beat_after_stop_stays_quiet(caplog):
    conn, heartbeat = held_heartbeat()
    heartbeat.stop()

    # A refresh still in flight when the lock was released: neither the
    # vanished row nor an error is reported as a lost lock.
    conn.row = None
    with caplog.at_level(logging.WARNING):
        assert heartbeat.beat() is False
        conn.fail = ("ALTER TABLE", RuntimeError("connection closed"))
        assert heartbeat.beat() is True

    assert heartbeat.lost is False
    assert caplog.text == ""


class BeatCounter:
    """Lets a test wait for the n-th refresh instead of sleeping."""

    def __init__(self, conn: FakeKeeperMap, until: int):
        self.count = 0
        self.until = until
        self.done = threading.Event()
        conn.on_alter = self

    def __call__(self):
        self.count += 1
        if self.count >= self.until:
            self.done.set()

    def wait(self) -> bool:
        return self.done.wait(10)


def test_heartbeat_thread_refreshes_until_stopped():
    conn, heartbeat = held_heartbeat(interval=0.001)
    counter = BeatCounter(conn, until=3)

    heartbeat.start()
    assert counter.wait()
    heartbeat.stop()

    assert not heartbeat.alive
    assert heartbeat.refreshed >= 3
    # Nothing is refreshed once stop() has returned.
    statements = len(conn.statements)
    assert not threading.Event().wait(0.01)
    assert len(conn.statements) == statements


def test_heartbeat_thread_survives_errors_and_recovers(caplog):
    conn, heartbeat = held_heartbeat(interval=0.001)
    counter = BeatCounter(conn, until=4)

    def flaky():
        counter()
        if counter.count <= 2:
            raise RuntimeError("Code: 210. Connection reset")

    conn.on_alter = flaky
    with caplog.at_level(logging.WARNING):
        heartbeat.start()
        assert counter.wait()
        heartbeat.stop()

    assert caplog.text.count("Failed to refresh the migration lock") == 2
    assert heartbeat.lost is False
    assert heartbeat.refreshed >= 2


def test_heartbeat_thread_stops_by_itself_when_the_lock_is_lost(caplog):
    conn, heartbeat = held_heartbeat(interval=0.001)
    conn.set_row("thief:9:uuid")

    with caplog.at_level(logging.ERROR):
        heartbeat.start()
        heartbeat._thread.join(10)  # pylint: disable=protected-access

    assert not heartbeat.alive
    assert heartbeat.lost is True
    assert "taken over by thief:9:uuid" in caplog.text
    heartbeat.stop()


def test_heartbeat_thread_is_a_daemon():
    _, heartbeat = held_heartbeat()

    assert heartbeat._thread.daemon is True  # pylint: disable=protected-access
    assert heartbeat.alive is False


def test_stop_before_start_is_a_no_op():
    _, heartbeat = held_heartbeat()

    heartbeat.stop()

    assert heartbeat.alive is False


def test_stop_does_not_wait_forever_for_a_hanging_refresh(caplog):
    conn, heartbeat = held_heartbeat(interval=0.001, join_timeout=0.01)
    entered, release = threading.Event(), threading.Event()

    def hang():
        entered.set()
        release.wait(10)

    conn.on_alter = hang
    heartbeat.start()
    assert entered.wait(10)

    with caplog.at_level(logging.WARNING):
        heartbeat.stop()

    assert "did not stop within 0.01s" in caplog.text
    assert heartbeat.alive
    release.set()
    heartbeat._thread.join(10)  # pylint: disable=protected-access
    assert not heartbeat.alive
    assert heartbeat.lost is False


def _cluster(conn, **kwargs):
    cluster = ClickhouseCluster(db_host="localhost", db_name="pytest", **kwargs)
    cluster.connection = lambda db_name=None: conn  # type: ignore[method-assign]
    return cluster


def test_cluster_lock_is_off_by_default():
    conn = FakeKeeperMap()

    with _cluster(conn).migration_lock() as migration_lock:
        assert migration_lock is None
        assert not heartbeat_threads()

    # Nothing lock-related is sent to the server unless the lock is asked for.
    assert not conn.statements


def test_cluster_lock_off_opens_no_connection():
    cluster = _cluster(None)
    opened = []
    cluster.connection = lambda db_name=None: opened.append(db_name)

    with cluster.migration_lock(lock=False) as migration_lock:
        assert migration_lock is None

    assert not opened


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


def heartbeat_threads():
    return [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith("clickhouse-migrations-lock-heartbeat")
    ]


class SameServer(FakeKeeperMap):
    """A second client of the server behind ``other``: same lock table."""

    def __init__(self, other: FakeKeeperMap):
        self.other = other
        super().__init__()

    row = property(
        lambda self: self.other.row,
        lambda self, value: setattr(self.other, "row", value),
    )


def test_cluster_lock_runs_a_heartbeat_on_its_own_connection():
    lock_conn = FakeKeeperMap()
    heartbeat_conn = SameServer(lock_conn)
    connections = iter([lock_conn, heartbeat_conn])
    cluster = _cluster(None)
    cluster.connection = lambda db_name=None: next(connections)
    counter = BeatCounter(heartbeat_conn, until=2)

    with cluster.migration_lock(
        lock=True, lock_ttl=30, heartbeat_interval=0.001
    ) as migration_lock:
        assert counter.wait()
        assert [t.name for t in heartbeat_threads()] == [
            f"clickhouse-migrations-lock-heartbeat-{migration_lock.owner}"
        ]

    assert not heartbeat_threads()
    assert lock_conn.row is None
    # Refreshes never go over the lock connection.
    assert not [s for s in lock_conn.statements if s.startswith("ALTER")]
    assert [s for s in heartbeat_conn.statements if s.startswith("ALTER")]


def test_cluster_lock_heartbeat_interval_follows_the_ttl(caplog):
    conn = FakeKeeperMap()

    with caplog.at_level(logging.INFO):
        with _cluster(conn).migration_lock(lock=True, lock_ttl=30):
            pass

    assert "every 10s" in caplog.text


def test_cluster_lock_is_released_when_stopping_the_heartbeat_fails(
    monkeypatch, caplog
):
    conn = FakeKeeperMap()

    def broken_stop(self):
        raise RuntimeError("cannot stop")

    monkeypatch.setattr(LockHeartbeat, "stop", broken_stop)
    with caplog.at_level(logging.WARNING):
        with _cluster(conn).migration_lock(lock=True):
            pass

    assert "Failed to stop the migration lock heartbeat: cannot stop" in caplog.text
    assert conn.row is None


def test_cluster_lock_is_released_when_the_heartbeat_cannot_start(monkeypatch):
    conn = FakeKeeperMap()

    def broken_start(self):
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(LockHeartbeat, "start", broken_start)
    # Enter directly: starting the heartbeat fails, so there is no body.
    with pytest.raises(RuntimeError, match="new thread"):
        _cluster(conn).migration_lock(lock=True).__enter__()

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
