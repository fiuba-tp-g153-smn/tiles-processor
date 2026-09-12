"""Tests for the folder and bucket backends behind GfsGribRepository."""

import os
import sys
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest

from data_sources.gfs_repository import (
    LocalGfsGribRepository,
    S3GfsGribRepository,
    describe_step,
    step_relative_path,
)
from exceptions import (
    ForecastNotAvailableError,
    InvalidGribResponseError,
    TransientDownloadError,
)
from models.gfs_config import GFS_EXPECTED_MESSAGE_COUNT

CYCLE = datetime(2026, 8, 8, 0, 0, tzinfo=UTC)
STEP = 3
RELATIVE = "20260808T0000Z/20260808T0000Z_f003.grib2"


def _grib_message(payload_len: int = 32) -> bytes:
    total = 16 + payload_len
    return b"GRIB" + b"\x00" * 4 + total.to_bytes(8, "big") + b"\x00" * payload_len


def _valid_grib(messages: int = GFS_EXPECTED_MESSAGE_COUNT) -> bytes:
    return b"".join(_grib_message() for _ in range(messages))


def test_step_relative_path_mirrors_the_cache_key():
    """The input layout matches the cache so one can be synced into the other."""
    assert step_relative_path(CYCLE, STEP) == RELATIVE


def test_describe_step_labels_cycle_and_step():
    assert describe_step(CYCLE, STEP) == "20260808T0000Z f003"


# ---------------------------------------------------------------------------
# Local folder
# ---------------------------------------------------------------------------


@pytest.fixture(name="grib_dir")
def _grib_dir(tmp_path):
    step_file = tmp_path / RELATIVE
    step_file.parent.mkdir(parents=True)
    step_file.write_bytes(_valid_grib())
    return tmp_path


@pytest.mark.asyncio
async def test_local_fetch_copies_and_forces_the_suffix(grib_dir, tmp_path):
    repo = LocalGfsGribRepository(grib_dir)
    dest = tmp_path / "work" / "step"

    result = await repo.fetch(CYCLE, STEP, dest)

    assert result == dest.with_suffix(".grib2")
    assert result.read_bytes() == _valid_grib()


@pytest.mark.asyncio
async def test_local_fetch_of_a_missing_step_is_skippable(grib_dir, tmp_path):
    repo = LocalGfsGribRepository(grib_dir)

    with pytest.raises(ForecastNotAvailableError):
        await repo.fetch(CYCLE, 999, tmp_path / "step")


@pytest.mark.asyncio
async def test_local_fetch_rejects_a_non_grib_file(tmp_path):
    """A folder file gets the same scrutiny as an HTTP body."""
    step_file = tmp_path / "input" / RELATIVE
    step_file.parent.mkdir(parents=True)
    step_file.write_bytes(b"<html>not a grib</html>")
    repo = LocalGfsGribRepository(tmp_path / "input")

    with pytest.raises(InvalidGribResponseError, match="did not return a GRIB2"):
        await repo.fetch(CYCLE, STEP, tmp_path / "step")


@pytest.mark.asyncio
async def test_local_fetch_rejects_a_short_subset(tmp_path):
    """Fewer messages than the products read is permanent, not transient."""
    step_file = tmp_path / "input" / RELATIVE
    step_file.parent.mkdir(parents=True)
    step_file.write_bytes(_valid_grib(messages=9))
    repo = LocalGfsGribRepository(tmp_path / "input")

    with pytest.raises(InvalidGribResponseError, match="returned 9 GRIB messages"):
        await repo.fetch(CYCLE, STEP, tmp_path / "step")


@pytest.mark.asyncio
async def test_local_fetch_treats_a_truncated_file_as_transient(tmp_path):
    step_file = tmp_path / "input" / RELATIVE
    step_file.parent.mkdir(parents=True)
    whole = _valid_grib()
    step_file.write_bytes(whole[: len(whole) - 10])
    repo = LocalGfsGribRepository(tmp_path / "input")

    with pytest.raises(TransientDownloadError, match="Truncated GRIB"):
        await repo.fetch(CYCLE, STEP, tmp_path / "step")


# ---------------------------------------------------------------------------
# S3 bucket
# ---------------------------------------------------------------------------


def _s3_client(payload: bytes = b"") -> MagicMock:
    client = MagicMock()
    client.bucket_name = "gfs-input"
    client.head_exists = AsyncMock(return_value=True)

    async def _download(_key, dest):
        dest.write_bytes(payload)

    client.download_to_file = AsyncMock(side_effect=_download)
    return client


@pytest.mark.asyncio
async def test_s3_fetch_downloads_the_step_key(tmp_path):
    client = _s3_client(_valid_grib())
    repo = S3GfsGribRepository(client, prefix="grib/models/gfs/")

    result = await repo.fetch(CYCLE, STEP, tmp_path / "step")

    client.download_to_file.assert_awaited_once_with(
        f"grib/models/gfs/{RELATIVE}", result
    )
    assert result == (tmp_path / "step").with_suffix(".grib2")


@pytest.mark.asyncio
async def test_s3_fetch_of_an_absent_key_is_skippable(tmp_path):
    client = _s3_client()
    client.head_exists = AsyncMock(return_value=False)
    repo = S3GfsGribRepository(client)

    with pytest.raises(ForecastNotAvailableError):
        await repo.fetch(CYCLE, STEP, tmp_path / "step")

    client.download_to_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_s3_fetch_validates_the_payload(tmp_path):
    client = _s3_client(b"<html>nope</html>")
    repo = S3GfsGribRepository(client)

    with pytest.raises(InvalidGribResponseError):
        await repo.fetch(CYCLE, STEP, tmp_path / "step")


def test_s3_source_label_names_bucket_and_prefix():
    repo = S3GfsGribRepository(_s3_client(), prefix="grib/models/gfs/")

    assert repo.source_label == "s3://gfs-input/grib/models/gfs/"
