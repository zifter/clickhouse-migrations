import hashlib

import pytest

from clickhouse_migrations.exceptions import MigrationException
from clickhouse_migrations.migration import (
    DEFAULT_VERSION_WIDTH,
    MigrationStorage,
    slugify,
)


def test_valid_migrations_are_sorted_by_version(tmp_path):
    (tmp_path / "002_second.sql").write_text("SELECT 2;", encoding="utf8")
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf8")

    migrations = MigrationStorage(tmp_path).migrations()

    assert [m.version for m in migrations] == [1, 2]


def test_migration_checksum_does_not_use_md5_for_security(tmp_path, monkeypatch):
    # FIPS-mode Python rejects md5() unless usedforsecurity=False is passed.
    migration_path = tmp_path / "001_first.sql"
    migration_path.write_text("SELECT 1;", encoding="utf8")
    original_md5 = hashlib.md5
    calls = []

    def fips_md5(data, **kwargs):
        calls.append(kwargs)
        return original_md5(data, **kwargs)

    monkeypatch.setattr(hashlib, "md5", fips_md5)

    migration = MigrationStorage(tmp_path).migrations()[0]

    assert calls == [{"usedforsecurity": False}]
    # Same digest as before, so checksums already stored stay valid.
    assert migration.md5 == original_md5(b"SELECT 1;").hexdigest()


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


def test_down_files_are_not_collected_as_migrations(tmp_path):
    (tmp_path / "001_init.sql").write_text("SELECT 1;", encoding="utf8")
    (tmp_path / "001_init.down.sql").write_text("SELECT 2;", encoding="utf8")

    migrations = MigrationStorage(tmp_path).migrations()

    # The paired .down.sql must not be picked up as a separate migration, and it
    # must not trip the duplicate-version guard against 001_init.sql.
    assert [m.version for m in migrations] == [1]
    assert migrations[0].script == "SELECT 1;"


def test_down_scripts_maps_version_to_script(tmp_path):
    (tmp_path / "001_init.sql").write_text("SELECT 1;", encoding="utf8")
    (tmp_path / "001_init.down.sql").write_text("DROP TABLE t;", encoding="utf8")
    (tmp_path / "002_more.sql").write_text("SELECT 2;", encoding="utf8")

    scripts = MigrationStorage(tmp_path).down_scripts()

    # Only versions that actually have a .down.sql show up.
    assert scripts == {1: "DROP TABLE t;"}


def test_down_scripts_empty_when_no_down_files(tmp_path):
    (tmp_path / "001_init.sql").write_text("SELECT 1;", encoding="utf8")

    assert not MigrationStorage(tmp_path).down_scripts()


def test_down_scripts_missing_directory_raises_clear_error(tmp_path):
    missing = tmp_path / "does_not_exist"

    with pytest.raises(MigrationException, match="does not exist"):
        MigrationStorage(missing).down_scripts()


def test_down_scripts_non_numeric_version_prefix_raises_clear_error(tmp_path):
    (tmp_path / "init.down.sql").write_text("DROP TABLE t;", encoding="utf8")

    with pytest.raises(MigrationException, match="numeric version"):
        MigrationStorage(tmp_path).down_scripts()


def test_down_scripts_duplicate_version_raises_clear_error(tmp_path):
    (tmp_path / "1_a.down.sql").write_text("DROP TABLE a;", encoding="utf8")
    (tmp_path / "001_b.down.sql").write_text("DROP TABLE b;", encoding="utf8")

    with pytest.raises(MigrationException, match="Duplicate down migration version 1"):
        MigrationStorage(tmp_path).down_scripts()


def test_slugify_normalizes_punctuation_and_case():
    assert slugify("Add  Events!!") == "add_events"
    assert slugify("--add/events--") == "add_events"
    assert slugify("add 2 events") == "add_2_events"


def test_slugify_folds_accents_to_ascii():
    assert slugify("Café naïve") == "cafe_naive"


def test_slugify_without_usable_characters_raises_clear_error():
    with pytest.raises(MigrationException, match="must contain letters or digits"):
        slugify("—!!—")


def test_next_version_on_empty_directory(tmp_path):
    assert MigrationStorage(tmp_path).next_version() == 1


def test_next_version_ignores_gaps(tmp_path):
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf8")
    (tmp_path / "007_seventh.sql").write_text("SELECT 7;", encoding="utf8")

    assert MigrationStorage(tmp_path).next_version() == 8


def test_next_version_counts_orphan_down_files(tmp_path):
    # A down file reserves its version even without the paired up file.
    (tmp_path / "004_only_down.down.sql").write_text("DROP TABLE t;", encoding="utf8")

    assert MigrationStorage(tmp_path).next_version() == 5


def test_next_version_missing_directory_raises_clear_error(tmp_path):
    with pytest.raises(MigrationException, match="does not exist"):
        MigrationStorage(tmp_path / "does_not_exist").next_version()


def test_version_width_defaults_when_empty(tmp_path):
    assert MigrationStorage(tmp_path).version_width() == DEFAULT_VERSION_WIDTH


def test_version_width_follows_widest_existing_file(tmp_path):
    (tmp_path / "01_first.sql").write_text("SELECT 1;", encoding="utf8")
    (tmp_path / "0002_second.sql").write_text("SELECT 2;", encoding="utf8")

    assert MigrationStorage(tmp_path).version_width() == 4


def test_version_width_missing_directory_raises_clear_error(tmp_path):
    with pytest.raises(MigrationException, match="does not exist"):
        MigrationStorage(tmp_path / "does_not_exist").version_width()


def test_create_in_missing_directory_creates_it(tmp_path):
    storage_dir = tmp_path / "nested" / "migrations"

    created = MigrationStorage(storage_dir).create("add events")

    assert created == [storage_dir / "001_add_events.sql"]
    assert (
        created[0].read_text(encoding="utf8").startswith("-- add events\n-- created:")
    )


def test_create_uses_next_version_and_existing_padding(tmp_path):
    (tmp_path / "0001_first.sql").write_text("SELECT 1;", encoding="utf8")

    created = MigrationStorage(tmp_path).create("add events")

    assert created == [tmp_path / "0002_add_events.sql"]


def test_create_with_down_writes_both_files(tmp_path):
    created = MigrationStorage(tmp_path).create("add events", with_down=True)

    assert created == [
        tmp_path / "001_add_events.sql",
        tmp_path / "001_add_events.down.sql",
    ]
    assert "-- add events (rollback)" in created[1].read_text(encoding="utf8")


def test_create_with_explicit_version(tmp_path):
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf8")

    created = MigrationStorage(tmp_path).create("add events", version=42)

    assert created == [tmp_path / "042_add_events.sql"]


def test_create_with_taken_version_raises_clear_error(tmp_path):
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf8")

    with pytest.raises(MigrationException, match="Duplicate migration version 1"):
        MigrationStorage(tmp_path).create("add events", version=1)


def test_create_with_version_taken_by_down_file_raises_clear_error(tmp_path):
    (tmp_path / "001_first.down.sql").write_text("DROP TABLE t;", encoding="utf8")

    with pytest.raises(MigrationException, match="Duplicate migration version 1"):
        MigrationStorage(tmp_path).create("add events", version=1)


def test_created_files_are_readable_as_migrations(tmp_path):
    storage = MigrationStorage(tmp_path)
    storage.create("add events", with_down=True)
    storage.create("add users")

    migrations = storage.migrations()

    assert [m.version for m in migrations] == [1, 2]
    assert set(storage.down_scripts()) == {1}
