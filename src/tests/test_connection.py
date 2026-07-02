import sys

import pytest

from clickhouse_migrations.connection import import_clickhouse_connect
from clickhouse_migrations.exceptions import MigrationException


def test_import_clickhouse_connect_missing_raises_clear_error(monkeypatch):
    # A None entry in sys.modules makes `import clickhouse_connect` fail.
    monkeypatch.setitem(sys.modules, "clickhouse_connect", None)

    with pytest.raises(MigrationException, match="clickhouse-connect"):
        import_clickhouse_connect()
