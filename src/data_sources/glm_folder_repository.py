"""GLM folder file repository — abstracts file listing and copying.

Mirrors :mod:`data_sources.radar_repository`: the abstract base lets us swap
the local-filesystem implementation for a remote-bucket one later without
touching :class:`GlmFolderDataSource`.
"""

import asyncio
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.s3_repository_utils import filter_keys_by_glob, strip_s3_scheme

GLM_FOLDER_FILENAME_GLOB = "CG_GLM-L2-GLMF-*.nc"


class GlmFolderFileRepository(ABC):
    """Interface for CG_GLM-L2-GLMF storage backends."""

    @abstractmethod
    async def list_files(self) -> list[str]:
        """Return source URIs (or paths) for all candidate netCDF files."""

    @abstractmethod
    async def download_to_dir(self, source_uris: list[str], dest_dir: Path) -> Path:
        """Copy/download the given files into ``dest_dir``, returning the directory.

        ``dest_dir`` is created if missing. The original filenames are
        preserved so the processor can re-parse timestamps if needed.
        """


class LocalGlmFolderFileRepository(GlmFolderFileRepository):
    """Reads CG_GLM-L2-GLMF netCDF files from a local directory.

    Supports two layouts (same as the radar repository):

      * Flat:   ``<input_dir>/*.nc``
      * Nested: ``<input_dir>/<any-subdir>/*.nc``

    Files matching :data:`GLM_FOLDER_FILENAME_GLOB` from both layouts are
    merged and sorted by absolute path (which sorts chronologically because
    the timestamp segment is the dominant component of the filename).
    """

    def __init__(self, input_dir: Path) -> None:
        self._input_dir = input_dir

    async def list_files(self) -> list[str]:
        if not self._input_dir.exists():
            return []

        files: list[Path] = list(self._input_dir.glob(GLM_FOLDER_FILENAME_GLOB))
        for subdir in self._input_dir.iterdir():
            if subdir.is_dir():
                files.extend(subdir.glob(GLM_FOLDER_FILENAME_GLOB))

        return [str(f.absolute()) for f in sorted(files)]

    async def download_to_dir(self, source_uris: list[str], dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)

        def _copy_all() -> None:
            for uri in source_uris:
                source_path = Path(uri)
                if not source_path.exists():
                    raise FileNotFoundError(f"GLM file not found: {uri}")
                shutil.copy2(source_path, dest_dir / source_path.name)

        await asyncio.to_thread(_copy_all)
        return dest_dir


class S3GlmFolderFileRepository(GlmFolderFileRepository):
    """Reads CG_GLM-L2-GLMF netCDF files from an S3 bucket.

    Lists recursively under the configured prefix (a superset of the local
    flat + one-subdir-level rule) and filters by the same filename glob.
    URIs are plain S3 keys; sorting stays chronological because the timestamp
    dominates the basename.
    """

    def __init__(self, s3_client: S3Client, prefix: str = "") -> None:
        self._s3_client = s3_client
        self._prefix = prefix

    async def list_files(self) -> list[str]:
        keys = await self._s3_client.list_files(self._prefix, file_pattern="")
        return filter_keys_by_glob(keys, GLM_FOLDER_FILENAME_GLOB)

    async def download_to_dir(self, source_uris: list[str], dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        # The S3Client's internal semaphore bounds download concurrency.
        await asyncio.gather(
            *(
                self._s3_client.download_to_file(
                    strip_s3_scheme(uri), dest_dir / Path(uri).name
                )
                for uri in source_uris
            )
        )
        return dest_dir
