"""Migration lock backed by a ClickHouse ``KeeperMap`` table.

ClickHouse has no transactional DDL, so two concurrent migration runs (think a
Kubernetes ``Job`` with several replicas) can interleave their statements and
leave a half-applied schema or duplicate bookkeeping rows. A ``KeeperMap``
table is backed by Keeper/ZooKeeper and its inserts are linearizable, which
makes it a usable mutex:

* acquire: insert the single row ``name = 'migrate'`` with
  ``keeper_map_strict_mode = 1``. Without that setting an insert of an existing
  key silently **overwrites** it; with it the second writer fails with a Keeper
  "Node exists" transaction error, which is exactly the compare-and-set we need.
  The insert also sets ``async_insert = 0``: asynchronous inserts are on by
  default in recent servers (26.3 and newer; 25.7 still has them off), and an
  asynchronous insert is batched with the concurrent ones into one block, rows
  with the same key collapse into a single Keeper ``create`` and every writer
  is told it succeeded.
* release: delete only rows whose ``owner`` matches ours, so a run can never
  drop somebody else's lock.
* stale takeover: a lock older than the TTL is deleted with a
  compare-and-delete on ``(owner, acquired_at)`` and then re-acquired through
  the normal strict insert - two waiters may both try, but only one insert can
  win.
* heartbeat: while the lock is held, :class:`LockHeartbeat` refreshes
  ``acquired_at`` from a background thread, so only a run that is gone (or
  hung) ever looks stale - a long migration keeps its lock however long it
  takes.

Locking is opt-in (``--lock`` / ``lock=True``): when it is asked for and the
server cannot provide it, the run fails instead of silently continuing
unprotected.
"""

import copy
import logging
import os
import socket
import threading
import time
from collections import namedtuple
from typing import Optional
from urllib.parse import quote as urlquote
from uuid import uuid4

from clickhouse_migrations.connection import Connection
from clickhouse_migrations.defaults import (
    LOCK_TIMEOUT,
    LOCK_TTL,
    MIGRATIONS_TABLE,
    MIGRATIONS_TABLE_DEFAULT_NAME,
)
from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.util import (
    format_table_reference,
    quote_string,
    split_table_reference,
)

# Single row of the lock table. One lock per migrated database.
LOCK_NAME = "migrate"
# The lock table is derived from the bookkeeping table so two tools pointed at
# the same database with different bookkeeping tables never share a lock.
LOCK_TABLE_SUFFIX = "_lock"
# Root of the Keeper path used by the lock table.
KEEPER_PATH_PREFIX = "/clickhouse-migrations"
# How often a waiter retries while the lock is held.
LOCK_POLL_INTERVAL = 1.0
# The heartbeat refreshes a held lock every ttl / 3 seconds (at least every
# HEARTBEAT_MIN_INTERVAL), so two refreshes can fail before it goes stale.
HEARTBEAT_TTL_DIVISOR = 3
HEARTBEAT_MIN_INTERVAL = 1.0
# How long releasing the lock waits for a refresh that is still in flight.
HEARTBEAT_JOIN_TIMEOUT = 5.0
# The oldest server the lock is tested on: KeeperMap appeared in 22.9, but
# keeper_map_strict_mode is missing from 23.3 and present in 23.8.
LOCK_SERVER_REQUIREMENT = "ClickHouse 23.8+ backed by Keeper/ZooKeeper"
# ClickHouse error code of KEEPER_EXCEPTION, e.g. a strict insert of a key that
# already exists.
KEEPER_EXCEPTION_CODE = 999

# Current holder of the lock. ``age`` is computed by the server, so it does not
# depend on the clock of the machine running the migration.
LockHolder = namedtuple("LockHolder", ["owner", "acquired_ts", "age"])


def make_owner() -> str:
    """Identify this run in error messages: host, pid and a unique suffix."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid4()}"


def heartbeat_interval(ttl: float) -> float:
    """Seconds between two refreshes of a lock with the given TTL."""
    return max(HEARTBEAT_MIN_INTERVAL, ttl / HEARTBEAT_TTL_DIVISOR)


def _is_conflict(exc: Exception) -> bool:
    """Whether a failed insert means "somebody else holds the lock".

    The server reports the conflict as a ``KEEPER_EXCEPTION``: clickhouse-connect
    has the error name in the message, clickhouse-driver only has the numeric
    ``code``, and servers before 24.x leave "(Node exists)" out of the message
    (``Transaction failed: Op #0, path: ...``).
    """
    text = str(exc)
    return (
        "Node exists" in text
        or "KEEPER_EXCEPTION" in text
        or getattr(exc, "code", None) == KEEPER_EXCEPTION_CODE
    )


class KeeperMapLock:  # pylint: disable=too-many-instance-attributes
    """Mutual exclusion between migration runs of one database."""

    def __init__(
        self,
        conn: Connection,
        db_name: Optional[str],
        migrations_table: str = MIGRATIONS_TABLE,
        timeout: int = LOCK_TIMEOUT,
        ttl: int = LOCK_TTL,
        owner: Optional[str] = None,
        poll_interval: float = LOCK_POLL_INTERVAL,
    ):
        table_db, table_name = split_table_reference(migrations_table)
        lock_db = table_db if table_db is not None else db_name
        lock_table = table_name + LOCK_TABLE_SUFFIX

        self._conn = conn
        self._table = format_table_reference(lock_db, lock_table)
        self._keeper_path = self._build_keeper_path(lock_db, table_name)
        self._timeout = timeout
        self._ttl = ttl
        self._owner = owner if owner is not None else make_owner()
        self._poll_interval = poll_interval
        self._acquired = False

    @staticmethod
    def _build_keeper_path(lock_db: Optional[str], table_name: str) -> str:
        """Keeper path of the lock table, unique per database.

        Every component is URL-escaped so a database name containing a slash or
        a quote cannot reshape the path. A customised bookkeeping table adds one
        more component, so two tools migrating the same database with different
        bookkeeping tables take two different locks.
        """
        path = f"{KEEPER_PATH_PREFIX}/{urlquote(lock_db or '', safe='')}"
        if table_name != MIGRATIONS_TABLE_DEFAULT_NAME:
            path += f"/{urlquote(table_name, safe='')}"

        return path

    @property
    def table(self) -> str:
        return self._table

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def acquired(self) -> bool:
        return self._acquired

    @property
    def ttl(self) -> int:
        return self._ttl

    def bound_to(self, conn: Connection) -> "KeeperMapLock":
        """The same lock (table, owner, TTL) issuing its queries on ``conn``.

        The heartbeat refreshes the lock from another thread, and a client of
        either driver must not be shared between threads.
        """
        bound = copy.copy(self)
        bound._conn = conn  # pylint: disable=protected-access
        return bound

    def unsupported_reason(self) -> Optional[str]:
        """Why locking is unavailable here, or None when it works.

        The probe checks the engine and the ``keeper_map_strict_mode`` setting
        (the compare-and-set the lock is built on; 23.3 has the engine but not
        the setting) and creates the table: KeeperMap is also disabled when the
        server has no ``<keeper_map_path_prefix>``, which only shows up on the
        CREATE.
        """
        rows = self._conn.query(
            "SELECT (SELECT count() FROM system.table_engines "
            "WHERE name = 'KeeperMap') AS n, "
            "(SELECT count() FROM system.settings "
            "WHERE name = 'keeper_map_strict_mode') AS strict"
        )
        if not rows[0]["n"]:
            return (
                "this server has no KeeperMap table engine "
                f"({LOCK_SERVER_REQUIREMENT} is required)"
            )

        if not rows[0]["strict"]:
            return (
                "this server has no keeper_map_strict_mode setting "
                f"({LOCK_SERVER_REQUIREMENT} is required)"
            )

        try:
            self._conn.command(
                f"CREATE TABLE IF NOT EXISTS {self._table} "
                "(name String, owner String, acquired_at DateTime) "
                f"ENGINE = KeeperMap({quote_string(self._keeper_path)}) "
                "PRIMARY KEY name"
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return f"the lock table {self._table} could not be created: {exc}"

        return None

    def acquire(self) -> bool:
        """Take the lock, waiting up to the timeout.

        Raises when this server cannot provide the lock at all, and when the
        lock is held by somebody else for longer than the timeout.
        """
        reason = self.unsupported_reason()
        if reason is not None:
            raise MigrationException(
                f"The migration lock was requested explicitly, but {reason}. "
                "Configure <keeper_map_path_prefix> on the server, or run "
                "without --lock."
            )

        deadline = time.monotonic() + max(self._timeout, 0)
        while True:
            if self._try_acquire():
                return True

            holder = self.holder()
            if holder is not None and holder.age >= self._ttl:
                logging.warning(
                    "Taking over a stale migration lock on %s held by %s "
                    "for %ds (longer than the lock TTL of %ds).",
                    self._table,
                    holder.owner,
                    holder.age,
                    self._ttl,
                )
                self._steal(holder)
                if self._try_acquire():
                    return True

                holder = self.holder()

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise MigrationException(self._timeout_message(holder))

            time.sleep(min(self._poll_interval, remaining))

    def _try_acquire(self) -> bool:
        try:
            # async_insert = 0: see the module docstring, an asynchronous
            # insert lets every concurrent writer "win".
            self._conn.command(
                f"INSERT INTO {self._table} "
                "SETTINGS keeper_map_strict_mode = 1, async_insert = 0 "
                f"VALUES ({quote_string(LOCK_NAME)}, "
                f"{quote_string(self._owner)}, now())"
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if not _is_conflict(exc):
                raise MigrationException(
                    f"Failed to take the migration lock on {self._table}: {exc}"
                ) from exc

            return False

        self._acquired = True
        logging.info("Migration lock on %s taken by %s", self._table, self._owner)
        return True

    def holder(self) -> Optional[LockHolder]:
        rows = self._conn.query(
            "SELECT owner, toUnixTimestamp(acquired_at) AS acquired_ts, "
            "toUnixTimestamp(now()) - toUnixTimestamp(acquired_at) AS age "
            f"FROM {self._table} WHERE name = {quote_string(LOCK_NAME)}"
        )
        if not rows:
            return None

        return LockHolder(
            rows[0]["owner"], int(rows[0]["acquired_ts"]), int(rows[0]["age"])
        )

    def _steal(self, holder: LockHolder) -> None:
        """Compare-and-delete a stale lock.

        Only the exact row that was observed is deleted, so a lock taken in the
        meantime survives. Two waiters seeing the same stale lock may both run
        this; the mutual exclusion is decided by the strict insert that follows,
        which only one of them can win.
        """
        self._conn.command(
            f"DELETE FROM {self._table} WHERE name = {quote_string(LOCK_NAME)} "
            f"AND owner = {quote_string(holder.owner)} "
            f"AND toUnixTimestamp(acquired_at) = {int(holder.acquired_ts)}"
        )

    def refresh(self) -> Optional[LockHolder]:
        """Move ``acquired_at`` of our own row to now; return the holder.

        On a KeeperMap table ``ALTER TABLE ... UPDATE`` is executed right away
        (it is not a background mutation, so there is nothing to wait for) as
        a Keeper ``set`` of every matching row, and ``keeper_map_strict_mode``
        makes that ``set`` conditional on the row version read by the same
        query. So the refresh is a compare-and-set on our own row: a lock that
        was released or taken over is never matched by ``owner`` and never
        recreated (a ``set`` cannot create a row), and a takeover racing the
        refresh makes the ``set`` fail instead of overwriting the new row.
        (Keeper restarts versions when a row is recreated, so only a new row
        created *and* updated to our version inside that one query's
        read-then-set window could slip through - and a takeover needs our
        lock to have gone without a refresh for a whole TTL first.)

        The returned holder tells the caller whether the lock is still ours.
        """
        self._conn.command(
            f"ALTER TABLE {self._table} UPDATE acquired_at = now() "
            f"WHERE name = {quote_string(LOCK_NAME)} "
            f"AND owner = {quote_string(self._owner)} "
            "SETTINGS keeper_map_strict_mode = 1"
        )
        return self.holder()

    def _timeout_message(self, holder: Optional[LockHolder]) -> str:
        if holder is None:
            held_by = "it is held by another migration run"
        else:
            held_by = (
                f"it is held by {holder.owner}, which has held it for " f"{holder.age}s"
            )

        return (
            f"Could not take the migration lock on {self._table} within "
            f"{self._timeout}s: {held_by}. If that run is gone, wait for the "
            f"lock TTL ({self._ttl}s) or force-release it with "
            "'clickhouse-migrations unlock'."
        )

    def release(self) -> None:
        """Release our own lock; never touches a lock owned by somebody else."""
        if not self._acquired:
            return

        self._acquired = False
        try:
            self._conn.command(
                f"DELETE FROM {self._table} "
                f"WHERE name = {quote_string(LOCK_NAME)} "
                f"AND owner = {quote_string(self._owner)}"
            )
            logging.info(
                "Migration lock on %s released by %s", self._table, self._owner
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            # Never mask the error that is already propagating out of the
            # migration: report and let the TTL (or unlock) clean up.
            logging.warning(
                "Failed to release the migration lock on %s: %s. "
                "It expires after the lock TTL, or release it with "
                "'clickhouse-migrations unlock'.",
                self._table,
                exc,
            )

    def force_release(self) -> Optional[LockHolder]:
        """Drop the lock whoever owns it - for the "pod was OOM-killed" case."""
        if not self._table_exists():
            return None

        holder = self.holder()
        if holder is None:
            return None

        self._conn.command(
            f"DELETE FROM {self._table} WHERE name = {quote_string(LOCK_NAME)}"
        )
        return holder

    def _table_exists(self) -> bool:
        database, table = split_table_reference(self._table)
        rows = self._conn.query(
            "SELECT count() AS n FROM system.tables "
            f"WHERE database = {quote_string(database or '')} "
            f"AND name = {quote_string(table)}"
        )
        return bool(rows[0]["n"])


class LockHeartbeat:
    """Keep a held lock fresh from a daemon thread until :meth:`stop`.

    Every ``interval`` seconds the lock is refreshed (see
    :meth:`KeeperMapLock.refresh`). A failed refresh is logged and retried at
    the next tick. When the lock turns out to be gone or owned by somebody
    else, an error is logged and the heartbeat stops: the migration itself is
    not interrupted (a half-applied migration is worse than a loud log line),
    but concurrent runs are no longer excluded from that moment on.
    """

    def __init__(
        self,
        lock: KeeperMapLock,
        interval: Optional[float] = None,
        join_timeout: float = HEARTBEAT_JOIN_TIMEOUT,
    ):
        self._lock = lock
        self._interval = (
            interval if interval is not None else heartbeat_interval(lock.ttl)
        )
        self._join_timeout = join_timeout
        self._stopped = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"clickhouse-migrations-lock-heartbeat-{lock.owner}",
            # Never keeps the process alive, whatever happens to the run.
            daemon=True,
        )
        self.lost = False
        self.refreshed = 0

    @property
    def interval(self) -> float:
        return self._interval

    @property
    def alive(self) -> bool:
        return self._thread.is_alive()

    def start(self) -> "LockHeartbeat":
        self._thread.start()
        logging.info(
            "Refreshing the migration lock on %s every %gs",
            self._lock.table,
            self._interval,
        )
        return self

    def stop(self) -> None:
        """Stop refreshing; wait (bounded) for a refresh still in flight.

        A refresh that outlives the timeout cannot resurrect the lock after it
        is released: it only ever updates an existing row of our own.
        """
        self._stopped.set()
        if not self._thread.is_alive():
            return

        self._thread.join(self._join_timeout)
        if self._thread.is_alive():
            logging.warning(
                "The migration lock heartbeat on %s did not stop within %gs; "
                "releasing the lock anyway.",
                self._lock.table,
                self._join_timeout,
            )

    def _run(self) -> None:
        while not self._stopped.wait(self._interval):
            if not self.beat():
                return

    def beat(self) -> bool:
        """Refresh once; whether the heartbeat should keep going."""
        try:
            holder = self._lock.refresh()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if not self._stopped.is_set():
                logging.warning(
                    "Failed to refresh the migration lock on %s: %s. "
                    "Retrying in %gs.",
                    self._lock.table,
                    exc,
                    self._interval,
                )
            return True

        if holder is not None and holder.owner == self._lock.owner:
            self.refreshed += 1
            logging.debug("Migration lock on %s refreshed", self._lock.table)
            return True

        if self._stopped.is_set():
            # Released while this refresh was in flight: nothing was lost.
            return False

        self.lost = True
        if holder is None:
            what = "is gone (released or force-released with 'unlock')"
        else:
            what = f"was taken over by {holder.owner}"
        logging.error(
            "The migration lock on %s held by %s %s while this run is still "
            "migrating: concurrent runs are no longer excluded. The heartbeat "
            "stops; the migration continues.",
            self._lock.table,
            self._lock.owner,
            what,
        )
        return False
