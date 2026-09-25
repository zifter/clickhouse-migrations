"""Skip integration tests that need a newer ClickHouse server than the one under test.

CI runs the suite against several server versions (``CLICKHOUSE_VERSION``, see
``dev/docker-compose.yaml``). A test that depends on a feature of newer servers
is marked with ``@pytest.mark.clickhouse_min_version("23.3", reason="...")``;
the integration ``conftest.py`` compares the marker with ``SELECT version()``
and skips the test, with the reason, on older servers.
"""

import re
from typing import Tuple

import pytest

_VERSION = re.compile(r"\d+(?:\.\d+)*")

# The migration lock needs KeeperMap with keeper_map_strict_mode: 23.3 has the
# engine but not the setting ("Unknown setting keeper_map_strict_mode"), 23.8
# has both. Keep in sync with LOCK_SERVER_REQUIREMENT in lock.py.
LOCK_MIN_VERSION = "23.8"
NEEDS_LOCK = pytest.mark.clickhouse_min_version(
    LOCK_MIN_VERSION, reason="--lock needs KeeperMap with keeper_map_strict_mode"
)


def parse_version(text: str) -> Tuple[int, ...]:
    """``'25.7.4.11'`` -> ``(25, 7, 4, 11)``, ``'23.3'`` -> ``(23, 3)``."""
    match = _VERSION.match(text.strip())
    if match is None:
        raise ValueError(f"not a ClickHouse version: {text!r}")
    return tuple(int(part) for part in match.group(0).split("."))


def skip_unless_server_at_least(server_version: str, minimum: str, reason: str) -> None:
    """Skip the running test if ``server_version`` is older than ``minimum``."""
    if parse_version(server_version) < parse_version(minimum):
        pytest.skip(
            f"needs ClickHouse {minimum} or newer ({reason}), "
            f"the server is {server_version}"
        )
