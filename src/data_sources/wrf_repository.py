"""WRF file repository — abstracts file listing and downloading.

Mirrors :mod:`data_sources.radar_repository`: the abstract base lets us swap
the local-filesystem implementation for an S3-bucket one without touching
:class:`WrfDataSource`.
"""

import asyncio
import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.s3_repository_utils import filter_keys_by_glob, strip_s3_scheme

logger = logging.getLogger(__name__)

WRF_FILENAME_GLOB = "WRF_ARG4K.FCST_L0_FIELD2D.*.nc"


class WrfFileRepository(ABC):
    """Interface for WRF file storage backends."""

    @abstractmethod
    async def list_files(self) -> list[str]:
        """Return source URIs for all FIELD2D .nc files."""

    @abstractmethod
    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Copy/download file to dest_path; return final path (with .nc extension)."""


class LocalWrfFileRepository(WrfFileRepository):
    """Reads WRF NetCDF files from a local directory.

    Supports the same two layouts as the radar and GLM repositories, which is
    also what the S3 implementation's recursive listing covers:

      * Flat:   ``<input_dir>/WRF_ARG4K.FCST_L0_FIELD2D.*.nc``
      * Nested: ``<input_dir>/<any-subdir>/WRF_ARG4K.FCST_L0_FIELD2D.*.nc``
    """

    def __init__(self, input_dir: Path) -> None:
        self._input_dir = input_dir

    async def list_files(self) -> list[str]:
        if not self._input_dir.exists():
            logger.warning(
                "WRF input dir does not exist, treating as empty: %s", self._input_dir
            )
            return []
        files: set[Path] = set(self._input_dir.glob(WRF_FILENAME_GLOB))
        for subdir in self._input_dir.iterdir():
            if subdir.is_dir():
                files.update(subdir.glob(WRF_FILENAME_GLOB))
        return [str(f.absolute()) for f in sorted(files)]

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        source_path = Path(source_uri)
        if not source_path.exists():
            raise FileNotFoundError(f"WRF file not found: {source_uri}")
        dest_with_ext = dest_path.with_suffix(".nc")
        dest_with_ext.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.copy2, source_path, dest_with_ext)
        return dest_with_ext


class S3WrfFileRepository(WrfFileRepository):
    """Reads WRF NetCDF files from an S3 bucket mirroring the local layout.

    Lists recursively under the configured prefix and filters by the FIELD2D
    filename glob. URIs are plain S3 keys so basename parsing keeps working.
    """

    def __init__(self, s3_client: S3Client, prefix: str = "") -> None:
        self._s3_client = s3_client
        self._prefix = prefix

    async def list_files(self) -> list[str]:
        keys = await self._s3_client.list_files(self._prefix, file_pattern="")
        return filter_keys_by_glob(keys, WRF_FILENAME_GLOB)

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        dest_with_ext = dest_path.with_suffix(".nc")
        dest_with_ext.parent.mkdir(parents=True, exist_ok=True)
        await self._s3_client.download_to_file(
            strip_s3_scheme(source_uri, self._s3_client.bucket_name), dest_with_ext
        )
        return dest_with_ext
