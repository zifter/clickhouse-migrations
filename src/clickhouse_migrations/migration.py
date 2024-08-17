import hashlib
import os
from collections import namedtuple
from pathlib import Path
from typing import List, Optional, Union

Migration = namedtuple("Migration", ["version", "md5", "script"])


class MigrationStorage:
    def __init__(self, storage_dir: Union[Path, str]):
        self.storage_dir: Path = Path(storage_dir)

    def filenames(self) -> List[Path]:
        l: List[Path] = []
        for f in os.scandir(self.storage_dir):
            if f.name.endswith(".sql"):
                l.append(self.storage_dir / f.name)

        return l

    def migrations(
        self, explicit_migrations: Optional[List[str]] = None
    ) -> List[Migration]:
        migrations: List[Migration] = []

        for full_path in self.filenames():
            version_string = full_path.name.split("_")[0]
            version_number = int(version_string)
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
