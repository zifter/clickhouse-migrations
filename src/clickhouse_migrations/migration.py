import hashlib
import os
from collections import namedtuple
from pathlib import Path
from typing import Dict, List, Optional, Union

from clickhouse_migrations.exceptions import MigrationException

Migration = namedtuple("Migration", ["version", "md5", "script"])

# Suffix that marks an optional, hand-written rollback ("down") script paired
# with a migration by version, e.g. 001_init.sql <-> 001_init.down.sql.
DOWN_SUFFIX = ".down.sql"


def _parse_version(filename: str) -> int:
    version_string = filename.split("_")[0]
    try:
        return int(version_string)
    except ValueError as exc:
        raise MigrationException(
            "Migration file name must start with a numeric version "
            f"followed by '_', got: {filename}"
        ) from exc


class MigrationStorage:
    def __init__(self, storage_dir: Union[Path, str]):
        self.storage_dir: Path = Path(storage_dir)

    def _require_dir(self) -> None:
        if not self.storage_dir.is_dir():
            raise MigrationException(
                f"Migrations directory does not exist: {self.storage_dir}"
            )

    def filenames(self) -> List[Path]:
        self._require_dir()

        # ".down.sql" files are rollback scripts, not migrations to apply; they
        # are collected separately via down_scripts().
        return [
            self.storage_dir / f.name
            for f in os.scandir(self.storage_dir)
            if f.name.endswith(".sql") and not f.name.endswith(DOWN_SUFFIX)
        ]

    def down_scripts(self) -> Dict[int, str]:
        self._require_dir()

        scripts: Dict[int, str] = {}
        seen_versions: Dict[int, str] = {}
        for entry in os.scandir(self.storage_dir):
            if not entry.name.endswith(DOWN_SUFFIX):
                continue

            version_number = _parse_version(entry.name)
            if version_number in seen_versions:
                raise MigrationException(
                    f"Duplicate down migration version {version_number}: "
                    f"{seen_versions[version_number]} and {entry.name}"
                )
            seen_versions[version_number] = entry.name
            scripts[version_number] = (self.storage_dir / entry.name).read_text(
                encoding="utf8"
            )

        return scripts

    def migrations(
        self, explicit_migrations: Optional[List[str]] = None
    ) -> List[Migration]:
        migrations: List[Migration] = []
        seen_versions: Dict[int, str] = {}

        for full_path in self.filenames():
            version_string = full_path.name.split("_")[0]
            version_number = _parse_version(full_path.name)

            if version_number in seen_versions:
                raise MigrationException(
                    f"Duplicate migration version {version_number}: "
                    f"{seen_versions[version_number]} and {full_path.name}"
                )
            seen_versions[version_number] = full_path.name

            migration = Migration(
                version=version_number,
                script=str(full_path.read_text(encoding="utf8")),
                md5=hashlib.md5(full_path.read_bytes()).hexdigest(),
            )

            if (
                not explicit_migrations
                or full_path.name in explicit_migrations
                or full_path.stem in explicit_migrations
                or version_string in explicit_migrations
                or str(version_number) in explicit_migrations
            ):
                migrations.append(migration)

        migrations.sort(key=lambda m: m.version)

        return migrations
