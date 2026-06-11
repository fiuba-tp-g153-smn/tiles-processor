"""Tests for LocalGlmFolderFileRepository and S3GlmFolderFileRepository."""

from unittest.mock import AsyncMock, call

import pytest

from data_sources.glm_folder_repository import (
    LocalGlmFolderFileRepository,
    S3GlmFolderFileRepository,
)


def _make_file(parent, name, payload=b""):
    path = parent / name
    path.write_bytes(payload)
    return path


@pytest.fixture()
def glm_flat(tmp_path):
    """Flat layout: nc files at the root of input_dir."""
    _make_file(
        tmp_path,
        "CG_GLM-L2-GLMF-M3_G19_s20260611400000_e20260611401000_c20260611402030.nc",
    )
    _make_file(
        tmp_path,
        "CG_GLM-L2-GLMF-M3_G19_s20260611401000_e20260611402000_c20260611403020.nc",
    )
    _make_file(tmp_path, "unrelated.txt", b"ignore-me")
    return tmp_path


@pytest.fixture()
def glm_nested(tmp_path):
    """Nested layout: files inside per-day subdirs."""
    (tmp_path / "20260302").mkdir()
    _make_file(
        tmp_path / "20260302",
        "CG_GLM-L2-GLMF-M3_G19_s20260611400000_e20260611401000_c20260611402030.nc",
    )
    (tmp_path / "20260303").mkdir()
    _make_file(
        tmp_path / "20260303",
        "CG_GLM-L2-GLMF-M3_G19_s20260621400000_e20260621401000_c20260621402030.nc",
    )
    return tmp_path


@pytest.mark.asyncio
async def test_list_files_flat_returns_only_glm_nc(glm_flat):
    repo = LocalGlmFolderFileRepository(glm_flat)
    files = await repo.list_files()
    assert len(files) == 2
    assert all(f.endswith(".nc") for f in files)
    assert all("CG_GLM-L2-GLMF" in f for f in files)


@pytest.mark.asyncio
async def test_list_files_handles_nested_layout(glm_nested):
    repo = LocalGlmFolderFileRepository(glm_nested)
    files = await repo.list_files()
    assert len(files) == 2
    # Sorted by absolute path → date subdir order
    assert "20260302" in files[0]
    assert "20260303" in files[1]


@pytest.mark.asyncio
async def test_list_files_missing_dir_returns_empty(tmp_path):
    repo = LocalGlmFolderFileRepository(tmp_path / "nonexistent")
    assert await repo.list_files() == []


@pytest.mark.asyncio
async def test_download_to_dir_copies_files(tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    f1 = _make_file(
        src_dir,
        "CG_GLM-L2-GLMF-M3_G19_s20260611400000_e20260611401000_c20260611402030.nc",
        b"file1",
    )
    f2 = _make_file(
        src_dir,
        "CG_GLM-L2-GLMF-M3_G19_s20260611401000_e20260611402000_c20260611403020.nc",
        b"file2",
    )

    repo = LocalGlmFolderFileRepository(src_dir)
    dest = tmp_path / "dest"
    result = await repo.download_to_dir([str(f1), str(f2)], dest)

    assert result == dest
    assert (dest / f1.name).read_bytes() == b"file1"
    assert (dest / f2.name).read_bytes() == b"file2"


@pytest.mark.asyncio
async def test_download_to_dir_raises_for_missing_source(tmp_path):
    repo = LocalGlmFolderFileRepository(tmp_path)
    with pytest.raises(FileNotFoundError):
        await repo.download_to_dir([str(tmp_path / "missing.nc")], tmp_path / "out")


@pytest.mark.asyncio
async def test_s3_list_files_filters_by_glob_and_sorts():
    s3_client = AsyncMock()
    s3_client.list_files.return_value = [
        "glm_h5/20260303/CG_GLM-L2-GLMF-M3_G19_s20260621400000_e1_c1.nc",
        "glm_h5/notes.txt",
        "glm_h5/20260302/CG_GLM-L2-GLMF-M3_G19_s20260611400000_e1_c1.nc",
    ]
    repo = S3GlmFolderFileRepository(s3_client, prefix="glm_h5/")

    files = await repo.list_files()

    s3_client.list_files.assert_awaited_once_with("glm_h5/", file_pattern="")
    assert files == [
        "glm_h5/20260302/CG_GLM-L2-GLMF-M3_G19_s20260611400000_e1_c1.nc",
        "glm_h5/20260303/CG_GLM-L2-GLMF-M3_G19_s20260621400000_e1_c1.nc",
    ]


@pytest.mark.asyncio
async def test_s3_download_to_dir_preserves_basenames(tmp_path):
    s3_client = AsyncMock()
    repo = S3GlmFolderFileRepository(s3_client)
    dest = tmp_path / "window"
    uris = [
        "glm_h5/CG_GLM-L2-GLMF-M3_G19_s20260611400000_e1_c1.nc",
        "s3://glm-input/glm_h5/CG_GLM-L2-GLMF-M3_G19_s20260611401000_e1_c1.nc",
    ]

    result = await repo.download_to_dir(uris, dest)

    assert result == dest
    assert dest.exists()
    s3_client.download_to_file.assert_has_awaits(
        [
            call(
                "glm_h5/CG_GLM-L2-GLMF-M3_G19_s20260611400000_e1_c1.nc",
                dest / "CG_GLM-L2-GLMF-M3_G19_s20260611400000_e1_c1.nc",
            ),
            call(
                "glm_h5/CG_GLM-L2-GLMF-M3_G19_s20260611401000_e1_c1.nc",
                dest / "CG_GLM-L2-GLMF-M3_G19_s20260611401000_e1_c1.nc",
            ),
        ],
        any_order=True,
    )
