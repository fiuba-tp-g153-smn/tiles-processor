"""Radar file repository — abstracts file listing and downloading."""

import shutil
from abc import ABC, abstractmethod
from pathlib import Path


class RadarFileRepository(ABC):
    """Interface for radar file storage backends."""

    @abstractmethod
    async def list_files(self) -> list[str]:
        """Return source URIs for all .H5 files."""

    @abstractmethod
    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Download/copy file to dest_path; return final path (with .H5 extension)."""


class LocalRadarFileRepository(RadarFileRepository):
    """Reads H5 files from a local directory.

    Supports two layouts:
      - Flat:   <input_dir>/*.H5
      - Nested: <input_dir>/RMA*/*.H5 (or any subdirectory)

    Both layouts are scanned and their files are merged.
    """

    def __init__(self, input_dir: Path) -> None:
        self._input_dir = input_dir

    async def list_files(self) -> list[str]:
        if not self._input_dir.exists():
            return []

        files: list[Path] = []

        # Flat files at root level
        files.extend(self._input_dir.glob("*.H5"))
        files.extend(self._input_dir.glob("*.h5"))

        # Nested per-radar subdirs
        for subdir in self._input_dir.iterdir():
            if subdir.is_dir():
                files.extend(subdir.glob("*.H5"))
                files.extend(subdir.glob("*.h5"))

        return [str(f.absolute()) for f in sorted(files)]

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        source_path = Path(source_uri)
        if not source_path.exists():
            raise FileNotFoundError(f"Radar file not found: {source_uri}")
        dest_with_ext = dest_path.with_suffix(".H5")
        dest_with_ext.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, dest_with_ext)
        return dest_with_ext
