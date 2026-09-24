import hashlib
import os
import re
import unicodedata
from collections import namedtuple
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from clickhouse_migrations.exceptions import MigrationException

Migration = namedtuple("Migration", ["version", "md5", "script"])

# Suffix that marks an optional, hand-written rollback ("down") script paired
# with a migration by version, e.g. 001_init.sql <-> 001_init.down.sql.
DOWN_SUFFIX = ".down.sql"

# Width used for the version prefix of a brand new migration when there is
# nothing on disk yet to copy the padding from.
DEFAULT_VERSION_WIDTH = 3


def slugify(name: str) -> str:
    # The name ends up in a file name, so keep it portable: fold accents instead
    # of dropping the letter ("café" -> "cafe") and squash everything else into
    # single underscores.
    folded = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^a-z0-9]+", "_", folded.lower()).strip("_")
    if not slug:
        raise MigrationException(
            f"Migration name must contain letters or digits, got: {name!r}"
        )

    return slug


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

    def down_filenames(self) -> Dict[int, str]:
        """Version -> file name of every ``.down.sql`` file."""
        self._require_dir()

        names: Dict[int, str] = {}
        for entry in os.scandir(self.storage_dir):
            if not entry.name.endswith(DOWN_SUFFIX):
                continue

            version_number = _parse_version(entry.name)
            if version_number in names:
                raise MigrationException(
                    f"Duplicate down migration version {version_number}: "
                    f"{names[version_number]} and {entry.name}"
                )
            names[version_number] = entry.name

        return names

    def down_scripts(self) -> Dict[int, str]:
        return {
            version: (self.storage_dir / name).read_text(encoding="utf8")
            for version, name in self.down_filenames().items()
        }

    def migration_filenames(self) -> Dict[int, str]:
        """Version -> file name of every migration (not ``.down.sql``) file."""
        return {_parse_version(path.name): path.name for path in self.filenames()}

    def _existing_files(self) -> List[Tuple[int, str]]:
        # Both up and down files take part: the version of a migration is owned
        # by the pair, so a stray 004_x.down.sql still reserves version 4.
        return [
            (_parse_version(entry.name), entry.name)
            for entry in os.scandir(self.storage_dir)
            if entry.name.endswith(".sql")
        ]

    def next_version(self) -> int:
        self._require_dir()

        return max((version for version, _ in self._existing_files()), default=0) + 1

    def version_width(self) -> int:
        """Zero-padding width for a new version, copied from the widest file on disk."""
        self._require_dir()

        return max(
            (len(name.split("_")[0]) for _, name in self._existing_files()),
            default=DEFAULT_VERSION_WIDTH,
        )

    def create(
        self,
        name: str,
        version: Optional[int] = None,
        with_down: bool = False,
        body: str = "",
    ) -> List[Path]:
        """Scaffold the next migration file (and its down pair) locally.

        ``body`` is written into the migration file after the header (the down
        file always starts empty).
        """
        # Scaffolding never touches ClickHouse, so a fresh checkout should not
        # need a manual mkdir before its very first migration.
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        slug = slugify(name)
        if version is None:
            version = self.next_version()

        taken = dict(self._existing_files())
        if version in taken:
            raise MigrationException(
                f"Duplicate migration version {version}: {taken[version]} already exists"
            )

        prefix = f"{version:0{self.version_width()}d}"
        created = [self.storage_dir / f"{prefix}_{slug}.sql"]
        if with_down:
            created.append(self.storage_dir / f"{prefix}_{slug}{DOWN_SUFFIX}")

        today = date.today().isoformat()
        for path in created:
            is_down = path.name.endswith(DOWN_SUFFIX)
            title = f"{name} (rollback)" if is_down else name
            text = f"-- {title}\n-- created: {today}\n"
            if body and not is_down:
                text += "\n" + body
            path.write_text(text, encoding="utf8")

        return created

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
                # Change detection, not security: FIPS-mode builds of Python
                # refuse plain md5() and would fail to read the directory.
                md5=hashlib.md5(
                    full_path.read_bytes(), usedforsecurity=False
                ).hexdigest(),
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
