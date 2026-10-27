"""Tests for the local and S3 INTA (Rainbow5) file repositories."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from data_sources.inta_radar_repository import (
    INTA_EXTENSIONS,
    LocalIntaRadarFileRepository,
    S3IntaRadarFileRepository,
)


def touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"<volume/>")
    return path


def test_both_published_extensions_are_listed():
    assert INTA_EXTENSIONS == (".azi", ".vol")


@pytest.mark.asyncio
async def test_lists_vol_and_azi_flat_and_nested(tmp_path):
    expected = [
        touch(tmp_path / "2026091501000300dBZ.vol"),
        touch(tmp_path / "ANG" / "2026091501072200dBZ.azi"),
        touch(tmp_path / "PER" / "2026091501074200dBZ.AZI"),
        touch(tmp_path / "PAR" / "2026091501000300KDP.VOL"),
    ]
    touch(tmp_path / "PAR" / "PAR_240_ZH_PPI01_20260521_140004Z.png")
    touch(tmp_path / "notes.txt")

    files = await LocalIntaRadarFileRepository(tmp_path).list_files()

    assert files == sorted(str(p.absolute()) for p in expected)


@pytest.mark.asyncio
async def test_missing_dir_lists_nothing(tmp_path):
    assert await LocalIntaRadarFileRepository(tmp_path / "nope").list_files() == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,suffix",
    [("2026091501072200dBZ.azi", ".azi"), ("2026091501000300dBZ.VOL", ".vol")],
)
async def test_local_download_keeps_the_source_extension(tmp_path, name, suffix):
    source = touch(tmp_path / "in" / name)
    dest = await LocalIntaRadarFileRepository(tmp_path / "in").download(
        str(source), tmp_path / "work" / "scan"
    )
    assert dest == tmp_path / "work" / f"scan{suffix}"
    assert dest.read_bytes() == b"<volume/>"


@pytest.mark.asyncio
async def test_s3_download_keeps_the_source_extension(tmp_path):
    s3_client = AsyncMock()
    s3_client.bucket_name = "radar"
    repo = S3IntaRadarFileRepository(s3_client, "inta/")
    dest = await repo.download(
        "inta/ANG/2026091501072200dBZ.azi", tmp_path / "work" / "scan"
    )
    assert dest == tmp_path / "work" / "scan.azi"
    s3_client.download_to_file.assert_awaited_once_with(
        "inta/ANG/2026091501072200dBZ.azi", dest
    )
