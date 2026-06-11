"""Tests for LocalRadarFileRepository and S3RadarFileRepository."""

from unittest.mock import AsyncMock

import pytest

from data_sources.radar_repository import (
    LocalRadarFileRepository,
    S3RadarFileRepository,
)


@pytest.fixture()
def h5_flat(tmp_path):
    """Flat layout: H5 files at root of input_dir."""
    for name in [
        "RMA1_0315_01_DBZH_20260114T170000Z.H5",
        "RMA2_0315_01_DBZH_20260114T170500Z.H5",
    ]:
        (tmp_path / name).write_bytes(b"")
    return tmp_path


@pytest.fixture()
def h5_nested(tmp_path):
    """Nested layout: H5 files inside RMA subdirs."""
    (tmp_path / "RMA5").mkdir()
    (tmp_path / "RMA6").mkdir()
    (tmp_path / "RMA5" / "RMA5_0315_01_DBZH_20260114T170000Z.H5").write_bytes(b"")
    (tmp_path / "RMA6" / "RMA6_0315_01_DBZH_20260114T170500Z.H5").write_bytes(b"")
    return tmp_path


@pytest.fixture()
def h5_mixed(tmp_path):
    """Mixed layout: some files at root, some nested."""
    (tmp_path / "RMA1_0315_01_DBZH_20260114T160000Z.H5").write_bytes(b"")
    (tmp_path / "RMA5").mkdir()
    (tmp_path / "RMA5" / "RMA5_0315_01_DBZH_20260114T170000Z.H5").write_bytes(b"")
    return tmp_path


@pytest.mark.asyncio
async def test_list_files_flat(h5_flat):
    repo = LocalRadarFileRepository(h5_flat)
    files = await repo.list_files()
    assert len(files) == 2
    assert all(f.endswith(".H5") for f in files)


@pytest.mark.asyncio
async def test_list_files_nested(h5_nested):
    repo = LocalRadarFileRepository(h5_nested)
    files = await repo.list_files()
    assert len(files) == 2
    assert all(f.endswith(".H5") for f in files)


@pytest.mark.asyncio
async def test_list_files_mixed(h5_mixed):
    repo = LocalRadarFileRepository(h5_mixed)
    files = await repo.list_files()
    assert len(files) == 2


@pytest.mark.asyncio
async def test_list_files_missing_dir(tmp_path):
    repo = LocalRadarFileRepository(tmp_path / "nonexistent")
    files = await repo.list_files()
    assert files == []


@pytest.mark.asyncio
async def test_download_copies_file(tmp_path):
    src = tmp_path / "source" / "RMA1_0315_01_DBZH_20260114T170000Z.H5"
    src.parent.mkdir()
    src.write_bytes(b"radar_data")

    dest_dir = tmp_path / "dest"
    repo = LocalRadarFileRepository(tmp_path / "source")

    result = await repo.download(str(src), dest_dir / "output")

    assert result.suffix == ".H5"
    assert result.read_bytes() == b"radar_data"


@pytest.mark.asyncio
async def test_download_raises_for_missing_file(tmp_path):
    repo = LocalRadarFileRepository(tmp_path)
    with pytest.raises(FileNotFoundError):
        await repo.download(str(tmp_path / "missing.H5"), tmp_path / "out")


@pytest.mark.asyncio
async def test_s3_list_files_filters_and_sorts():
    s3_client = AsyncMock()
    s3_client.list_files.return_value = [
        "radar_h5/RMA5/RMA5_0315_01_DBZH_20260114T170000Z.h5",
        "radar_h5/notes.txt",
        "radar_h5/RMA1_0315_01_DBZH_20260114T170000Z.H5",
    ]
    repo = S3RadarFileRepository(s3_client, prefix="radar_h5/")

    files = await repo.list_files()

    s3_client.list_files.assert_awaited_once_with("radar_h5/", file_pattern="")
    assert files == [
        "radar_h5/RMA1_0315_01_DBZH_20260114T170000Z.H5",
        "radar_h5/RMA5/RMA5_0315_01_DBZH_20260114T170000Z.h5",
    ]


@pytest.mark.asyncio
async def test_s3_download_forces_h5_suffix_and_creates_parent(tmp_path):
    s3_client = AsyncMock()
    repo = S3RadarFileRepository(s3_client)
    dest = tmp_path / "work" / "output"

    result = await repo.download("radar_h5/RMA1_file.H5", dest)

    assert result == dest.with_suffix(".H5")
    assert result.parent.exists()
    s3_client.download_to_file.assert_awaited_once_with("radar_h5/RMA1_file.H5", result)


@pytest.mark.asyncio
async def test_s3_download_strips_s3_scheme(tmp_path):
    s3_client = AsyncMock()
    repo = S3RadarFileRepository(s3_client)

    await repo.download("s3://radar-input/radar_h5/RMA1_file.H5", tmp_path / "out")

    (key, _), _ = s3_client.download_to_file.await_args
    assert key == "radar_h5/RMA1_file.H5"
