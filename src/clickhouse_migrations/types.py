import hashlib
import os
from collections import namedtuple
from pathlib import Path
from typing import List

Migration = namedtuple("Migration", ["version", "md5", "script"])


class MigrationStorage:
    def __init__(self, storage_dir: Path):
        self.storage_dir: Path = storage_dir

    def filenames(self) -> List[Path]:
        l: List[Path] = []
        for f in os.scandir(self.storage_dir):
            if f.name.endswith(".sql"):
                l.append(self.storage_dir / f.name)

        return l

    def migrations(self) -> List[Migration]:
        migrations: List[Migration] = []

        for full_path in self.filenames():
            migration = Migration(
                version=int(full_path.name.split("_")[0]),
                script=str(full_path.read_text(encoding="utf8")),
                md5=hashlib.md5(full_path.read_bytes()).hexdigest(),
            )

            migrations.append(migration)

        migrations.sort(key=lambda m: m.version)

        return migrations
