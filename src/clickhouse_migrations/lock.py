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
* release: delete only rows whose ``owner`` matches ours, so a run can never
  drop somebody else's lock.
* stale takeover: a lock older than the TTL is deleted with a
  compare-and-delete on ``(owner, acquired_at)`` and then re-acquired through
  the normal strict insert - two waiters may both try, but only one insert can
  win.

Locking is opt-in (``--lock`` / ``lock=True``): when it is asked for and the
server cannot provide it, the run fails instead of silently continuing
unprotected.
"""

import logging
import os
import socket
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

# Current holder of the lock. ``age`` is computed by the server, so it does not
# depend on the clock of the machine running the migration.
LockHolder = namedtuple("LockHolder", ["owner", "acquired_ts", "age"])


def make_owner() -> str:
    """Identify this run in error messages: host, pid and a unique suffix."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid4()}"


def _is_conflict(exc: Exception) -> bool:
    """Whether a failed insert means "somebody else holds the lock"."""
    text = str(exc)
    return "Node exists" in text or "KEEPER_EXCEPTION" in text


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

    def unsupported_reason(self) -> Optional[str]:
        """Why locking is unavailable here, or None when it works.

        The probe both checks the engine and creates the table: KeeperMap is
        also disabled when the server has no ``<keeper_map_path_prefix>``, which
        only shows up on the CREATE.
        """
        rows = self._conn.query(
            "SELECT count() AS n FROM system.table_engines WHERE name = 'KeeperMap'"
        )
        if not rows[0]["n"]:
            return (
                "this server has no KeeperMap table engine "
                "(ClickHouse 22.9+ backed by Keeper/ZooKeeper is required)"
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
            self._conn.command(
                f"INSERT INTO {self._table} SETTINGS keeper_map_strict_mode = 1 "
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
