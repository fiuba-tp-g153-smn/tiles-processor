"""Tests for LocalWrfFileRepository and S3WrfFileRepository."""

from unittest.mock import AsyncMock

import pytest

from data_sources.wrf_repository import (
    LocalWrfFileRepository,
    S3WrfFileRepository,
)

FIELD2D_NAME = "WRF_ARG4K.FCST_L0_FIELD2D.01H.2026061100.F003.M000.nc"


@pytest.mark.asyncio
async def test_local_list_files_matches_field2d_only(tmp_path):
    (tmp_path / FIELD2D_NAME).write_bytes(b"")
    (tmp_path / "WRF_ARG4K.FCST_L0_FIELD3D.01H.2026061100.F003.M000.nc").write_bytes(
        b""
    )
    (tmp_path / "unrelated.txt").write_bytes(b"")

    repo = LocalWrfFileRepository(tmp_path)
    files = await repo.list_files()

    assert len(files) == 1
    assert files[0].endswith(FIELD2D_NAME)


@pytest.mark.asyncio
async def test_local_list_files_missing_dir(tmp_path):
    repo = LocalWrfFileRepository(tmp_path / "nonexistent")
    assert await repo.list_files() == []


@pytest.mark.asyncio
async def test_local_download_copies_file(tmp_path):
    src = tmp_path / FIELD2D_NAME
    src.write_bytes(b"wrf_data")
    repo = LocalWrfFileRepository(tmp_path)

    result = await repo.download(str(src), tmp_path / "dest" / "output")

    assert result.suffix == ".nc"
    assert result.read_bytes() == b"wrf_data"


@pytest.mark.asyncio
async def test_local_download_raises_for_missing_file(tmp_path):
    repo = LocalWrfFileRepository(tmp_path)
    with pytest.raises(FileNotFoundError):
        await repo.download(str(tmp_path / "missing.nc"), tmp_path / "out")


@pytest.mark.asyncio
async def test_s3_list_files_filters_by_glob_and_sorts():
    s3_client = AsyncMock()
    s3_client.list_files.return_value = [
        f"wrf_nc/{FIELD2D_NAME}",
        "wrf_nc/WRF_ARG4K.FCST_L0_FIELD3D.01H.2026061100.F003.M000.nc",
        "wrf_nc/2026061100/WRF_ARG4K.FCST_L0_FIELD2D.01H.2026061100.F001.M000.nc",
    ]
    repo = S3WrfFileRepository(s3_client, prefix="wrf_nc/")

    files = await repo.list_files()

    s3_client.list_files.assert_awaited_once_with("wrf_nc/", file_pattern="")
    assert files == [
        "wrf_nc/2026061100/WRF_ARG4K.FCST_L0_FIELD2D.01H.2026061100.F001.M000.nc",
        f"wrf_nc/{FIELD2D_NAME}",
    ]


@pytest.mark.asyncio
async def test_s3_download_forces_nc_suffix_and_strips_scheme(tmp_path):
    s3_client = AsyncMock()
    repo = S3WrfFileRepository(s3_client)
    dest = tmp_path / "work" / "output"

    result = await repo.download(f"s3://wrf-input/wrf_nc/{FIELD2D_NAME}", dest)

    assert result == dest.with_suffix(".nc")
    assert result.parent.exists()
    s3_client.download_to_file.assert_awaited_once_with(
        f"wrf_nc/{FIELD2D_NAME}", result
    )
