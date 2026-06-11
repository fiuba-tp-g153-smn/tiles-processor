"""GOES-19 file repository — abstracts hourly-path listing and downloading.

GOES-19 data is organized in hourly directories: {product_path}/YYYY/JJJ/HH/.
The S3 implementation reads that layout from a bucket (NOAA's public one by
default); the local implementation reads the same layout under a folder.
"""

import asyncio
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.s3_repository_utils import strip_s3_scheme

# NOAA public bucket for GOES-19 data
GOES19_BUCKET_NAME = "noaa-goes19"


class Goes19FileRepository(ABC):
    """Interface for GOES-19 hourly-directory storage backends."""

    @abstractmethod
    async def list_files(self, directory_path: str, file_pattern: str) -> list[str]:
        """List candidate URIs under an hourly dir whose names contain file_pattern.

        Matching is by substring (not glob), mirroring S3Client.list_files so
        band patterns like "C13_G19" behave identically in both backends.
        """

    @abstractmethod
    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Download/copy the file to dest_path and return it."""


class S3Goes19FileRepository(Goes19FileRepository):
    """Reads GOES-19 files from an S3 bucket (NOAA public or a private mirror)."""

    def __init__(self, s3_client: S3Client) -> None:
        self._s3_client = s3_client

    async def list_files(self, directory_path: str, file_pattern: str) -> list[str]:
        return await self._s3_client.list_files(
            directory_path, file_pattern=file_pattern
        )

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        await self._s3_client.download_to_file(strip_s3_scheme(source_uri), dest_path)
        return dest_path


class LocalGoes19FileRepository(Goes19FileRepository):
    """Reads GOES-19 files from a local folder mirroring the bucket layout:

    <input_dir>/{product_path}/YYYY/JJJ/HH/<files>
    """

    def __init__(self, input_dir: Path) -> None:
        self._input_dir = input_dir

    async def list_files(self, directory_path: str, file_pattern: str) -> list[str]:
        hour_dir = self._input_dir / directory_path
        if not hour_dir.exists():
            return []
        files = [
            f for f in hour_dir.iterdir() if f.is_file() and file_pattern in f.name
        ]
        return [str(f.absolute()) for f in sorted(files)]

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        source_path = Path(source_uri)
        if not source_path.exists():
            raise FileNotFoundError(f"GOES-19 file not found: {source_uri}")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.copy2, source_path, dest_path)
        return dest_path
