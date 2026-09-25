"""What ``--lock`` needs from the server, checked on every version CI runs.

Unlike ``test_lock.py`` (skipped on servers that cannot provide the lock) this
runs everywhere: on a new enough server the lock is available, on an older one
it is refused up front with the requirement, before any migration runs.
"""

from clickhouse_migrations.lock import KeeperMapLock
from tests.clickhouse_version import LOCK_MIN_VERSION, parse_version


def test_lock_support_matches_the_server_version(cluster, server_version):
    supported = parse_version(server_version) >= parse_version(LOCK_MIN_VERSION)

    with cluster.connection("pytest") as conn:
        reason = KeeperMapLock(conn, "pytest").unsupported_reason()

    assert (reason is None) == supported, reason
    assert supported or f"(ClickHouse {LOCK_MIN_VERSION}+" in reason
