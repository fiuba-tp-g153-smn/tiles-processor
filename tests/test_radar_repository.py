"""Tests for LocalRadarFileRepository."""

import pytest

from data_sources.radar_repository import LocalRadarFileRepository


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
