"""INTA radar file repository — abstracts .vol/.azi listing, header reads and download."""

import asyncio
import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.s3_repository_utils import strip_s3_scheme
from data_sources.s3_windowed_lister import S3WindowedKeyLister
from models.listing_window import ListingWindow
from models.radar_config import INTA_SCAN_SPECS
from models.rainbow_header import HEADER_WINDOW_BYTES

logger = logging.getLogger(__name__)

INTA_EXTENSIONS = tuple(sorted({spec.extension for spec in INTA_SCAN_SPECS}))


class IntaRadarFileRepository(ABC):
    """Interface for INTA (Rainbow5) radar file storage backends.

    Mirrors ``RadarFileRepository`` but adds ``read_header``: an INTA file's
    station cannot be derived from its name — the SMN's own samples carry no
    radar token — so discovery has to look inside the file.
    """

    @abstractmethod
    async def list_files(self, window: ListingWindow | None = None) -> list[str]:
        """Return source URIs for the .vol and .azi files.

        ``window`` lets a backend skip what discovery would drop for age; the
        result may still hold older files, so callers keep filtering by time.
        """

    @abstractmethod
    async def read_header(self, source_uri: str) -> bytes:
        """Return the leading bytes of a file, enough to cover its XML header."""

    @abstractmethod
    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Download/copy file to dest_path; return the final path.

        The source's extension is kept (lowercased), so a long-range ``.azi``
        stays recognizable in the worker's temp dir and its logs.
        """


class LocalIntaRadarFileRepository(IntaRadarFileRepository):
    """Reads .vol and .azi files from a local directory.

    Supports the same two layouts as the SINARAME repository:
      - Flat:   <input_dir>/*.vol
      - Nested: <input_dir>/PAR/*.vol (or any subdirectory)

    Which product a file feeds is decided by discovery, from its variable,
    extension and header range.
    """

    def __init__(self, input_dir: Path) -> None:
        self._input_dir = input_dir

    async def list_files(self, window: ListingWindow | None = None) -> list[str]:
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
        folders = [self._input_dir]
        folders.extend(d for d in self._input_dir.iterdir() if d.is_dir())
        for folder in folders:
            for extension in INTA_EXTENSIONS:
                files.update(folder.glob(f"*{extension}"))
                files.update(folder.glob(f"*{extension.upper()}"))

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
        dest_with_ext = dest_path.with_suffix(source_path.suffix.lower())
        dest_with_ext.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.copy2, source_path, dest_with_ext)
        return dest_with_ext


class S3IntaRadarFileRepository(IntaRadarFileRepository):
    """Reads .vol and .azi files from an S3 bucket mirroring the local layout.

    Lists recursively under the configured prefix (a superset of the local
    flat + one-subdir-level rule), or only the date partitions ``key_template``
    names; URIs are plain S3 keys so the basename parsing in discovery keeps
    working. Listings are shared per window (see ``S3WindowedKeyLister``).
    """

    def __init__(
        self, s3_client: S3Client, prefix: str = "", key_template: str | None = None
    ) -> None:
        self._s3_client = s3_client
        self._lister = S3WindowedKeyLister(
            s3_client, prefix, key_template, key_suffix=INTA_EXTENSIONS
        )

    async def list_files(self, window: ListingWindow | None = None) -> list[str]:
        return await self._lister.list_keys(window)

    async def read_header(self, source_uri: str) -> bytes:
        return await self._s3_client.read_range(
            strip_s3_scheme(source_uri, self._s3_client.bucket_name),
            HEADER_WINDOW_BYTES,
        )

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        dest_with_ext = dest_path.with_suffix(Path(source_uri).suffix.lower())
        dest_with_ext.parent.mkdir(parents=True, exist_ok=True)
        await self._s3_client.download_to_file(
            strip_s3_scheme(source_uri, self._s3_client.bucket_name), dest_with_ext
        )
        return dest_with_ext
