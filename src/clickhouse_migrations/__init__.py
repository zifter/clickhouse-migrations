"""
Simple file-based migrations for clickhouse
"""
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("clickhouse-migrations")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"
