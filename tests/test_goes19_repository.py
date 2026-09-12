"""Tests for S3Goes19FileRepository and LocalGoes19FileRepository."""

from unittest.mock import AsyncMock

import pytest

from data_sources.goes19_repository import (
    LocalGoes19FileRepository,
    S3Goes19FileRepository,
)

HOURLY_DIR = "ABI-L1b-RadF/2026/162/14"
C13_NAME = (
    "OR_ABI-L1b-RadF-M6C13_G19_s20261621400208_e20261621409528_c20261621409582.nc"
)
C02_NAME = (
    "OR_ABI-L1b-RadF-M6C02_G19_s20261621400208_e20261621409516_c20261621409542.nc"
)


@pytest.mark.asyncio
async def test_local_list_files_filters_by_substring(tmp_path):
    hour_dir = tmp_path / HOURLY_DIR
    hour_dir.mkdir(parents=True)
    (hour_dir / C13_NAME).write_bytes(b"")
    (hour_dir / C02_NAME).write_bytes(b"")

    repo = LocalGoes19FileRepository(tmp_path)
    files = await repo.list_files(HOURLY_DIR, "C13_G19")

    assert len(files) == 1
    assert files[0].endswith(C13_NAME)


@pytest.mark.asyncio
async def test_local_list_files_missing_hour_dir_returns_empty(tmp_path):
    repo = LocalGoes19FileRepository(tmp_path)
    assert await repo.list_files("ABI-L1b-RadF/2026/001/00", "C13_G19") == []


@pytest.mark.asyncio
async def test_local_download_copies_file(tmp_path):
    hour_dir = tmp_path / HOURLY_DIR
    hour_dir.mkdir(parents=True)
    src = hour_dir / C13_NAME
    src.write_bytes(b"goes_data")

    repo = LocalGoes19FileRepository(tmp_path)
    dest = tmp_path / "work" / "image.nc"
    result = await repo.download(str(src), dest)

    assert result == dest
    assert result.read_bytes() == b"goes_data"


@pytest.mark.asyncio
async def test_local_download_raises_for_missing_file(tmp_path):
    repo = LocalGoes19FileRepository(tmp_path)
    with pytest.raises(FileNotFoundError):
        await repo.download(str(tmp_path / "missing.nc"), tmp_path / "out.nc")


@pytest.mark.asyncio
async def test_s3_list_files_filters_on_basename():
    """The band pattern must match the filename, never the enclosing prefix."""
    s3_client = AsyncMock()
    s3_client.list_files.return_value = [
        f"{HOURLY_DIR}/{C13_NAME}",
        f"{HOURLY_DIR}/OR_ABI-L1b-RadF-M6C09_G19_s1_e1_c1.nc",
    ]
    repo = S3Goes19FileRepository(s3_client)

    files = await repo.list_files(HOURLY_DIR, "C13_G19")

    s3_client.list_files.assert_awaited_once_with(HOURLY_DIR, file_pattern="")
    assert files == [f"{HOURLY_DIR}/{C13_NAME}"]


@pytest.mark.asyncio
async def test_s3_list_files_applies_configured_prefix():
    """A mirror may root NOAA's layout under a folder of its own."""
    s3_client = AsyncMock()
    s3_client.list_files.return_value = [f"mirror/goes/{HOURLY_DIR}/{C13_NAME}"]
    repo = S3Goes19FileRepository(s3_client, prefix="mirror/goes/")

    files = await repo.list_files(HOURLY_DIR, "C13_G19")

    s3_client.list_files.assert_awaited_once_with(
        f"mirror/goes/{HOURLY_DIR}", file_pattern=""
    )
    assert files == [f"mirror/goes/{HOURLY_DIR}/{C13_NAME}"]


@pytest.mark.asyncio
async def test_s3_list_files_prefix_does_not_leak_into_pattern_match():
    """A prefix containing the band token must not match every object."""
    s3_client = AsyncMock()
    s3_client.list_files.return_value = [
        f"C13_G19/{HOURLY_DIR}/OR_ABI-L1b-RadF-M6C09_G19_s1_e1_c1.nc",
    ]
    repo = S3Goes19FileRepository(s3_client, prefix="C13_G19/")

    assert await repo.list_files(HOURLY_DIR, "C13_G19") == []


@pytest.mark.asyncio
async def test_s3_download_strips_scheme_and_creates_parent(tmp_path):
    s3_client = AsyncMock()
    s3_client.bucket_name = "noaa-goes19"
    repo = S3Goes19FileRepository(s3_client)
    dest = tmp_path / "work" / "image.nc"

    result = await repo.download(f"s3://noaa-goes19/{HOURLY_DIR}/{C13_NAME}", dest)

    assert result == dest
    assert dest.parent.exists()
    s3_client.download_to_file.assert_awaited_once_with(
        f"{HOURLY_DIR}/{C13_NAME}", dest
    )
