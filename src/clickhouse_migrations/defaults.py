import os
from os.path import abspath
from pathlib import Path

DB_HOST = "localhost"
DB_PORT = 9000
DB_NAME = "default"
DB_USER = "default"
DB_PASSWORD = ""
MIGRATIONS_DIR = abspath(Path(os.getcwd()) / "migrations")

# Bookkeeping table holding the applied migrations. Accepts a "db.table"
# form; a bare name means the migrated database.
MIGRATIONS_TABLE_DEFAULT_NAME = "schema_versions"
MIGRATIONS_TABLE = MIGRATIONS_TABLE_DEFAULT_NAME
# Full engine clause for the bookkeeping table. None means the built-in
# default (MergeTree, or ReplicatedMergeTree when --cluster-name is set).
MIGRATIONS_TABLE_ENGINE = None

DB_URL = "clickhouse://default:@localhost:9000/db_placeholder"

# Migration lock (see clickhouse_migrations/lock.py). Opt-in: without it two
# concurrent runs can interleave, exactly as before the lock existed.
LOCK = False
# Seconds to wait for a lock held by another run before giving up. 0 fails
# immediately.
LOCK_TIMEOUT = 300
# A lock older than this many seconds may be taken over by another run.
LOCK_TTL = 3600
