"""Tests for the three ECMWF GRIB backends behind EcmwfGribRepository."""

import os
import sys
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
import requests

from data_sources.ecmwf_repository import (
    LocalEcmwfGribRepository,
    OpenDataEcmwfGribRepository,
    S3EcmwfGribRepository,
    newest_run,
    parse_run_timestamp,
    product_directory,
)
from exceptions import ForecastNotAvailableError, TransientDownloadError
from models.ecmwf_config import ECMWF_MSLP_CONFIG, ECMWF_TP_CONFIG

RUN = datetime(2026, 2, 17, 0, 0, tzinfo=UTC)
STEPS = [3, 6, 144]


# ---------------------------------------------------------------------------
# Layout helpers — the contract the folder and bucket layouts share
# ---------------------------------------------------------------------------


def test_product_directory_matches_the_cache_key_tail():
    """The input layout mirrors the cache so one can be synced into the other."""
    assert product_directory(ECMWF_TP_CONFIG) == "total-precipitation"
    assert product_directory(ECMWF_MSLP_CONFIG) == "mean-sea-level-pressure"


def test_parse_run_timestamp_round_trips_and_rejects_junk():
    assert parse_run_timestamp("20260217T0000Z.grib") == RUN
    assert parse_run_timestamp("not-a-run.grib") is None


def test_newest_run_ignores_unparseable_names():
    names = ["20260216T1200Z.grib", "README.txt", "20260217T0000Z.grib"]
    assert newest_run(names) == RUN


def test_newest_run_of_nothing_is_none():
    assert newest_run([]) is None


# ---------------------------------------------------------------------------
# Local folder
# ---------------------------------------------------------------------------


@pytest.fixture(name="grib_dir")
def _grib_dir(tmp_path):
    product = tmp_path / product_directory(ECMWF_TP_CONFIG)
    product.mkdir()
    (product / "20260216T1200Z.grib").write_bytes(b"GRIB-old")
    (product / "20260217T0000Z.grib").write_bytes(b"GRIB-new")
    (product / "notes.txt").write_text("ignored")
    return tmp_path


@pytest.mark.asyncio
async def test_local_latest_available_run_is_the_newest_file(grib_dir):
    repo = LocalEcmwfGribRepository(grib_dir, ECMWF_TP_CONFIG)

    assert await repo.latest_available_run() == RUN


@pytest.mark.asyncio
async def test_local_latest_available_run_is_none_when_dir_missing(tmp_path):
    repo = LocalEcmwfGribRepository(tmp_path / "nonexistent", ECMWF_TP_CONFIG)

    assert await repo.latest_available_run() is None


@pytest.mark.asyncio
async def test_local_reads_only_its_own_product_folder(grib_dir):
    """Both products share an input root; each sees only its own runs."""
    repo = LocalEcmwfGribRepository(grib_dir, ECMWF_MSLP_CONFIG)

    assert await repo.latest_available_run() is None


@pytest.mark.asyncio
async def test_local_fetch_copies_the_run(grib_dir, tmp_path):
    repo = LocalEcmwfGribRepository(grib_dir, ECMWF_TP_CONFIG)
    target = tmp_path / "out" / "run.grib"
    target.parent.mkdir()

    result = await repo.fetch(RUN, target)

    assert result == target
    assert result.read_bytes() == b"GRIB-new"


@pytest.mark.asyncio
async def test_local_fetch_of_a_missing_run_is_skippable(grib_dir, tmp_path):
    """A missing file must be "not available" (skip), never a hard crash."""
    repo = LocalEcmwfGribRepository(grib_dir, ECMWF_TP_CONFIG)

    with pytest.raises(ForecastNotAvailableError):
        await repo.fetch(datetime(2020, 1, 1, tzinfo=UTC), tmp_path / "run.grib")


# ---------------------------------------------------------------------------
# S3 bucket
# ---------------------------------------------------------------------------


def _s3_client() -> MagicMock:
    client = MagicMock()
    client.bucket_name = "ecmwf-input"
    client.list_files = AsyncMock(return_value=[])
    client.head_exists = AsyncMock(return_value=True)
    client.download_to_file = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_s3_latest_available_run_lists_the_product_prefix():
    client = _s3_client()
    client.list_files.return_value = [
        "grib/ecmwf-ifs/total-precipitation/20260216T1200Z.grib",
        "grib/ecmwf-ifs/total-precipitation/20260217T0000Z.grib",
    ]
    repo = S3EcmwfGribRepository(client, ECMWF_TP_CONFIG, prefix="grib/ecmwf-ifs/")

    assert await repo.latest_available_run() == RUN
    client.list_files.assert_awaited_once_with(
        "grib/ecmwf-ifs/total-precipitation/", file_pattern=".grib"
    )


@pytest.mark.asyncio
async def test_s3_fetch_downloads_the_run_key(tmp_path):
    client = _s3_client()
    repo = S3EcmwfGribRepository(client, ECMWF_TP_CONFIG, prefix="grib/ecmwf-ifs")
    target = tmp_path / "run.grib"

    await repo.fetch(RUN, target)

    client.download_to_file.assert_awaited_once_with(
        "grib/ecmwf-ifs/total-precipitation/20260217T0000Z.grib", target
    )


@pytest.mark.asyncio
async def test_s3_fetch_of_an_absent_key_is_skippable(tmp_path):
    client = _s3_client()
    client.head_exists = AsyncMock(return_value=False)
    repo = S3EcmwfGribRepository(client, ECMWF_TP_CONFIG)

    with pytest.raises(ForecastNotAvailableError):
        await repo.fetch(RUN, tmp_path / "run.grib")

    client.download_to_file.assert_not_awaited()


# ---------------------------------------------------------------------------
# ECMWF Open Data mirrors
# ---------------------------------------------------------------------------


def _opendata(sources: tuple[str, ...]) -> OpenDataEcmwfGribRepository:
    return OpenDataEcmwfGribRepository(ECMWF_TP_CONFIG, sources, STEPS)


def test_latest_available_run_uses_last_step_and_normalizes_to_utc():
    """latest() is queried for the final step and its naive result is made UTC-aware."""
    repo = _opendata(("ecmwf",))
    fake_client = MagicMock()
    fake_client.latest.return_value = datetime(2026, 2, 17, 0, 0)  # naive

    with patch(
        "data_sources.ecmwf_repository.Client", return_value=fake_client
    ) as client_cls:
        result = repo._latest_available_run()

    assert result == RUN
    client_cls.assert_called_once_with(source="ecmwf")
    kwargs = fake_client.latest.call_args.kwargs
    assert kwargs["type"] == "fc"
    assert kwargs["step"] == STEPS[-1]
    assert kwargs["param"] == [ECMWF_TP_CONFIG.parameter]


def test_latest_available_run_falls_back_across_mirrors_and_returns_none():
    """Every mirror failing (network error) returns None, after trying each in order."""
    repo = _opendata(("ecmwf", "azure", "aws"))
    fake_client = MagicMock()
    fake_client.latest.side_effect = ValueError("Cannot establish latest date")

    with patch(
        "data_sources.ecmwf_repository.Client", return_value=fake_client
    ) as client_cls:
        assert repo._latest_available_run() is None

    # One client per mirror was tried.
    assert client_cls.call_count == 3


@pytest.mark.asyncio
async def test_fetch_falls_back_to_next_mirror_on_transient(tmp_path):
    """A 503 on the first mirror falls through to the next, which succeeds."""
    repo = _opendata(("ecmwf", "azure"))
    tried = []

    def fake_retrieve(mirror, _forecast_time, target):
        tried.append(mirror)
        if mirror == "ecmwf":
            raise TransientDownloadError("503")
        target.write_bytes(b"GRIB")

    repo._retrieve_from_mirror = fake_retrieve
    target = tmp_path / "run.grib"

    result = await repo.fetch(RUN, target)

    assert tried == ["ecmwf", "azure"]
    assert result == target
    assert result.read_bytes() == b"GRIB"


@pytest.mark.asyncio
async def test_fetch_requeues_when_all_mirrors_transiently_fail(tmp_path):
    """Every mirror 503 → TransientDownloadError (requeue, not skip)."""
    repo = _opendata(("ecmwf", "azure", "aws"))

    def fake_retrieve(_mirror, _forecast_time, _target):
        raise TransientDownloadError("503")

    repo._retrieve_from_mirror = fake_retrieve

    with pytest.raises(TransientDownloadError):
        await repo.fetch(RUN, tmp_path / "run.grib")


@pytest.mark.asyncio
async def test_fetch_skips_when_no_mirror_has_data_yet(tmp_path):
    """Every mirror 404 (and none transient) → ForecastNotAvailableError (skip)."""
    repo = _opendata(("ecmwf", "azure"))

    def fake_retrieve(_mirror, _forecast_time, _target):
        raise ForecastNotAvailableError("404")

    repo._retrieve_from_mirror = fake_retrieve

    with pytest.raises(ForecastNotAvailableError):
        await repo.fetch(RUN, tmp_path / "run.grib")


@pytest.mark.asyncio
async def test_fetch_without_mirrors_is_transient_not_a_silent_success(tmp_path):
    """An empty mirror list is a misconfiguration, not "nothing to do"."""
    with pytest.raises(TransientDownloadError, match="No ECMWF mirrors"):
        await _opendata(()).fetch(RUN, tmp_path / "run.grib")


# --- fail-fast opendata client -------------------------------------------------
#
# multiurl.robust retries connection errors and 408/429/5xx 500 times, 120 s
# apart, inside the call. These tests pin the interception that keeps one flaky
# mirror from owning the whole discovery tick.


class _FakeSession:
    """Stands in for requests.Session; `outcome` is raised or returned."""

    def __init__(self, outcome):
        self._outcome = outcome
        self.calls = []

    def request(self, method, url, *args, **kwargs):
        self.calls.append((method, url, kwargs))
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


def _patched_client(outcome):
    from data_sources.ecmwf_repository import opendata_client

    fake = MagicMock()
    fake.session = _FakeSession(outcome)
    with patch("data_sources.ecmwf_repository.Client", return_value=fake):
        return opendata_client("ecmwf"), fake.session


@pytest.mark.parametrize(
    "exc",
    [
        requests.exceptions.ConnectionError("Name or service not known"),
        requests.exceptions.ReadTimeout("read timed out"),
        requests.exceptions.ChunkedEncodingError("truncated"),
    ],
)
def test_connection_errors_multiurl_would_retry_become_transient(exc):
    """A DNS blip must escape multiurl's loop, not restart it 500 times."""
    client, _ = _patched_client(exc)

    with pytest.raises(TransientDownloadError):
        client.session.request("HEAD", "https://data.ecmwf.int/x.grib2")


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_retriable_statuses_become_transient(status):
    client, _ = _patched_client(MagicMock(status_code=status))

    with pytest.raises(TransientDownloadError):
        client.session.request("HEAD", "https://data.ecmwf.int/x.grib2")


@pytest.mark.parametrize("status", [200, 404])
def test_non_retriable_responses_pass_through(status):
    """404 must still reach the caller as a 404 — it means "run not published"."""
    response = MagicMock(status_code=status)
    client, _ = _patched_client(response)

    assert client.session.request("HEAD", "https://data.ecmwf.int/x.grib2") is response


def test_requests_get_a_default_timeout():
    """Client.latest() HEADs without a timeout; a hung mirror would block forever."""
    client, session = _patched_client(MagicMock(status_code=200))

    client.session.request("HEAD", "https://data.ecmwf.int/x.grib2")

    assert session.calls[0][2]["timeout"] > 0


def test_an_explicit_timeout_is_respected():
    client, session = _patched_client(MagicMock(status_code=200))

    client.session.request("GET", "https://data.ecmwf.int/x.grib2", timeout=5)

    assert session.calls[0][2]["timeout"] == 5
