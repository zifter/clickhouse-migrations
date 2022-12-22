import os
from os.path import abspath
from pathlib import Path

DB_HOST = "localhost"
DB_PORT = 9000
DB_NAME = "default"
DB_USER = "default"
DB_PASSWORD = ""
MIGRATIONS_DIR = abspath(Path(os.getcwd()) / "migrations")
