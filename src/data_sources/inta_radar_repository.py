"""INTA radar file repository — abstracts .vol listing, header reads and download."""

import asyncio
import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.s3_repository_utils import strip_s3_scheme
from models.rainbow_header import HEADER_WINDOW_BYTES

logger = logging.getLogger(__name__)


class IntaRadarFileRepository(ABC):
    """Interface for INTA (Rainbow5) radar file storage backends.

    Mirrors ``RadarFileRepository`` but adds ``read_header``: an INTA file's
    station cannot be derived from its name — the SMN's own samples carry no
    radar token — so discovery has to look inside the file.
    """

    @abstractmethod
    async def list_files(self) -> list[str]:
        """Return source URIs for all .vol files."""

    @abstractmethod
    async def read_header(self, source_uri: str) -> bytes:
        """Return the leading bytes of a file, enough to cover its XML header."""

    @abstractmethod
    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Download/copy file to dest_path; return final path (.vol extension)."""


class LocalIntaRadarFileRepository(IntaRadarFileRepository):
    """Reads .vol files from a local directory.

    Supports the same two layouts as the SINARAME repository:
      - Flat:   <input_dir>/*.vol
      - Nested: <input_dir>/PAR/*.vol (or any subdirectory)

    Only ``.vol`` is globbed: the feed also ships ``.azi`` products, which are
    not volumetric and must never reach the processor.
    """

    def __init__(self, input_dir: Path) -> None:
        self._input_dir = input_dir

    async def list_files(self) -> list[str]:
        if not self._input_dir.exists():
            logger.warning(
                "INTA radar input dir does not exist, treating as empty: %s",
                self._input_dir,
            )
            return []

        files: set[Path] = set()

        # Both cases are globbed because the layout is case-sensitive on Linux;
        # the set collapses the duplicate a case-insensitive filesystem would
        # otherwise return twice.
        files.update(self._input_dir.glob("*.vol"))
        files.update(self._input_dir.glob("*.VOL"))

        for subdir in self._input_dir.iterdir():
            if subdir.is_dir():
                files.update(subdir.glob("*.vol"))
                files.update(subdir.glob("*.VOL"))

        return [str(f.absolute()) for f in sorted(files)]

    async def read_header(self, source_uri: str) -> bytes:
        return await asyncio.to_thread(self._read_head, source_uri)

    @staticmethod
    def _read_head(source_uri: str) -> bytes:
        with open(source_uri, "rb") as handle:
            return handle.read(HEADER_WINDOW_BYTES)

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        source_path = Path(source_uri)
        if not source_path.exists():
            raise FileNotFoundError(f"INTA radar file not found: {source_uri}")
        dest_with_ext = dest_path.with_suffix(".vol")
        dest_with_ext.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.copy2, source_path, dest_with_ext)
        return dest_with_ext


class S3IntaRadarFileRepository(IntaRadarFileRepository):
    """Reads .vol files from an S3 bucket mirroring the local folder layout.

    Lists recursively under the configured prefix (a superset of the local
    flat + one-subdir-level rule); URIs are plain S3 keys so the basename
    parsing in discovery keeps working.
    """

    def __init__(self, s3_client: S3Client, prefix: str = "") -> None:
        self._s3_client = s3_client
        self._prefix = prefix

    async def list_files(self) -> list[str]:
        keys = await self._s3_client.list_files(self._prefix, file_pattern="")
        return sorted(k for k in keys if k.lower().endswith(".vol"))

    async def read_header(self, source_uri: str) -> bytes:
        return await self._s3_client.read_range(
            strip_s3_scheme(source_uri, self._s3_client.bucket_name),
            HEADER_WINDOW_BYTES,
        )

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        dest_with_ext = dest_path.with_suffix(".vol")
        dest_with_ext.parent.mkdir(parents=True, exist_ok=True)
        await self._s3_client.download_to_file(
            strip_s3_scheme(source_uri, self._s3_client.bucket_name), dest_with_ext
        )
        return dest_with_ext
