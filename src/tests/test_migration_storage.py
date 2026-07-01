import pytest

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import MigrationStorage


def test_valid_migrations_are_sorted_by_version(tmp_path):
    (tmp_path / "002_second.sql").write_text("SELECT 2;", encoding="utf8")
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf8")

    migrations = MigrationStorage(tmp_path).migrations()

    assert [m.version for m in migrations] == [1, 2]


def test_missing_directory_raises_clear_error(tmp_path):
    missing = tmp_path / "does_not_exist"

    with pytest.raises(MigrationException, match="does not exist"):
        MigrationStorage(missing).migrations()


def test_non_numeric_version_prefix_raises_clear_error(tmp_path):
    (tmp_path / "init.sql").write_text("SELECT 1;", encoding="utf8")

    with pytest.raises(MigrationException, match="numeric version"):
        MigrationStorage(tmp_path).migrations()


def test_duplicate_version_raises_clear_error(tmp_path):
    (tmp_path / "001_a.sql").write_text("SELECT 1;", encoding="utf8")
    (tmp_path / "001_b.sql").write_text("SELECT 2;", encoding="utf8")

    with pytest.raises(MigrationException, match="Duplicate migration version 1"):
        MigrationStorage(tmp_path).migrations()


def test_duplicate_version_detected_across_padding(tmp_path):
    (tmp_path / "1_a.sql").write_text("SELECT 1;", encoding="utf8")
    (tmp_path / "001_b.sql").write_text("SELECT 2;", encoding="utf8")

    with pytest.raises(MigrationException, match="Duplicate migration version 1"):
        MigrationStorage(tmp_path).migrations()
