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
MIGRATIONS_TABLE = "schema_versions"
# Full engine clause for the bookkeeping table. None means the built-in
# default (MergeTree, or ReplicatedMergeTree when --cluster-name is set).
MIGRATIONS_TABLE_ENGINE = None

DB_URL = "clickhouse://default:@localhost:9000/db_placeholder"
