"""Tests for the GFS GRIB fetchers."""

from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from data_sources.gfs_fetcher import (
    GfsGribFetcher,
    InvalidGribResponseError,
    _scan_grib,
)
from exceptions import (
    ForecastNotAvailableError,
    TransientDownloadError,
    UnprocessableInputError,
)
from models.gfs_config import GFS_EXPECTED_MESSAGE_COUNT, GfsAccessConfig

PUBLIC_ENDPOINT = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
# Stands in for the SMN's internal mirror. The real hostname is deliberately not
# committed; what these tests check is that swapping the base URL leaves the
# query byte-identical, which any second host exercises just as well.
INTERNAL_ENDPOINT = "http://gfs-mirror.internal/cgi-enabled/g2subset.pl"

BOUNDS = {"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0}
CYCLE = datetime(2026, 8, 8, 0, tzinfo=UTC)


def _access(endpoint: str = PUBLIC_ENDPOINT, **kwargs) -> GfsAccessConfig:
    return GfsAccessConfig(subset_endpoint=endpoint, **kwargs)


def _fetcher(endpoint: str = PUBLIC_ENDPOINT) -> GfsGribFetcher:
    return GfsGribFetcher(_access(endpoint), BOUNDS)


def _grib_message(payload_len: int = 32) -> bytes:
    """Build a minimal GRIB2 section-0 header plus filler of the declared length."""
    total = 16 + payload_len
    return b"GRIB" + b"\x00" * 4 + total.to_bytes(8, "big") + b"\x00" * payload_len


def _valid_grib(messages: int = GFS_EXPECTED_MESSAGE_COUNT) -> bytes:
    return b"".join(_grib_message() for _ in range(messages))


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


class TestBuildUrl:
    """The query must be byte-compatible with both endpoints."""

    def test_file_and_dir_encode_cycle_and_step(self):
        params = parse_qs(urlparse(_fetcher().build_url(CYCLE, 3)).query)
        assert params["file"] == ["gfs.t00z.pgrb2.0p25.f003"]
        assert params["dir"] == ["/gfs.20260808/00/atmos"]

    def test_step_is_zero_padded_to_three_digits(self):
        params = parse_qs(urlparse(_fetcher().build_url(CYCLE, 144)).query)
        assert params["file"] == ["gfs.t00z.pgrb2.0p25.f144"]

    def test_requests_every_variable_and_level(self):
        params = parse_qs(urlparse(_fetcher().build_url(CYCLE, 0)).query)
        for var in ("HGT", "MSLET", "TMP", "UGRD", "VGRD"):
            assert params[f"var_{var}"] == ["on"]
        for level in ("mean_sea_level", "1000_mb", "500_mb", "250_mb"):
            assert params[f"lev_{level}"] == ["on"]

    def test_western_longitudes_shift_into_0_360(self):
        """NOMADS expects 0-360; the project works in -180..180 (-110 → 250)."""
        params = parse_qs(urlparse(_fetcher().build_url(CYCLE, 0)).query)
        assert params["leftlon"] == ["250"]
        assert params["rightlon"] == ["330"]

    def test_latitudes_pass_through_signed(self):
        params = parse_qs(urlparse(_fetcher().build_url(CYCLE, 0)).query)
        assert params["toplat"] == ["-15"]
        assert params["bottomlat"] == ["-60"]

    def test_eastern_longitudes_are_left_alone(self):
        fetcher = GfsGribFetcher(
            _access(), {"minx": 10.0, "miny": -60.0, "maxx": 40.0, "maxy": -15.0}
        )
        params = parse_qs(urlparse(fetcher.build_url(CYCLE, 0)).query)
        assert params["leftlon"] == ["10"]
        assert params["rightlon"] == ["40"]

    def test_only_the_base_url_differs_between_environments(self):
        """The internal/external switch must never change the query itself."""
        public = urlparse(_fetcher(PUBLIC_ENDPOINT).build_url(CYCLE, 6))
        internal = urlparse(_fetcher(INTERNAL_ENDPOINT).build_url(CYCLE, 6))
        assert public.query == internal.query
        assert (public.scheme, public.netloc, public.path) != (
            internal.scheme,
            internal.netloc,
            internal.path,
        )


# ---------------------------------------------------------------------------
# HTTP status mapping
# ---------------------------------------------------------------------------


class TestStatusMapping:
    """Measured against the public endpoint: 403 pending, 404 purged, 500 bad param."""

    @pytest.mark.parametrize("status", [403, 404])
    def test_missing_run_is_skippable(self, status):
        with pytest.raises(ForecastNotAvailableError):
            _fetcher()._raise_for_status(status, CYCLE, 0)

    @pytest.mark.parametrize("status", [500, 502, 503])
    def test_server_errors_are_retryable(self, status):
        with pytest.raises(TransientDownloadError):
            _fetcher()._raise_for_status(status, CYCLE, 0)

    def test_other_4xx_is_a_configuration_bug(self):
        with pytest.raises(InvalidGribResponseError):
            _fetcher()._raise_for_status(400, CYCLE, 0)

    def test_200_passes(self):
        _fetcher()._raise_for_status(200, CYCLE, 0)


# ---------------------------------------------------------------------------
# Response validation — the defense for the unverifiable internal mirror
# ---------------------------------------------------------------------------


class TestValidation:
    def test_accepts_a_well_formed_subset(self, tmp_path: Path):
        target = tmp_path / "ok.grib2"
        target.write_bytes(_valid_grib())
        _fetcher()._validate(target, CYCLE, 0)
        assert target.exists()

    def test_rejects_an_html_error_page_served_with_200(self, tmp_path: Path):
        """The failure mode an old grib_filter build produces."""
        target = tmp_path / "error.grib2"
        target.write_bytes(
            b"<!DOCTYPE html><html><head><title>Error</title></head>"
            b"<body>invalid parameter: var_NOEXISTE</body></html>"
        )
        with pytest.raises(InvalidGribResponseError, match="did not return a GRIB2"):
            _fetcher()._validate(target, CYCLE, 0)

    def test_rejects_an_empty_body(self, tmp_path: Path):
        target = tmp_path / "empty.grib2"
        target.write_bytes(b"")
        with pytest.raises(InvalidGribResponseError):
            _fetcher()._validate(target, CYCLE, 0)

    def test_rejects_a_subset_with_missing_messages(self, tmp_path: Path):
        """A mirror with reduced coverage is caught on the first download."""
        target = tmp_path / "short.grib2"
        target.write_bytes(_valid_grib(messages=9))
        with pytest.raises(InvalidGribResponseError, match="returned 9 GRIB messages"):
            _fetcher()._validate(target, CYCLE, 0)

    def test_a_truncated_body_is_transient_not_permanent(self, tmp_path: Path):
        """A network hiccup must be retried, not skipped for good.

        Both faults yield fewer than 13 messages, but only one of them can
        succeed on a retry — so they must not share an exception type.
        """
        whole = _valid_grib()
        target = tmp_path / "cut.grib2"
        target.write_bytes(whole[: len(whole) - 10])
        with pytest.raises(TransientDownloadError, match="Truncated GRIB"):
            _fetcher()._validate(target, CYCLE, 0)

    def test_a_truncated_body_is_not_skipped_by_the_worker(self, tmp_path: Path):
        """`UnprocessableInputError` would ack it as SKIPPED and drop the step."""
        whole = _valid_grib()
        target = tmp_path / "cut.grib2"
        target.write_bytes(whole[: len(whole) - 10])
        with pytest.raises(TransientDownloadError) as caught:
            _fetcher()._validate(target, CYCLE, 0)
        assert not isinstance(caught.value, UnprocessableInputError)

    def test_reduced_coverage_is_permanent_not_transient(self, tmp_path: Path):
        """The mirror image of the case above: retrying gets the same answer."""
        target = tmp_path / "short.grib2"
        target.write_bytes(_valid_grib(messages=9))
        with pytest.raises(InvalidGribResponseError) as caught:
            _fetcher()._validate(target, CYCLE, 0)
        assert not isinstance(caught.value, TransientDownloadError)

    def test_a_truncated_body_is_deleted(self, tmp_path: Path):
        target = tmp_path / "cut.grib2"
        whole = _valid_grib()
        target.write_bytes(whole[: len(whole) - 10])
        with pytest.raises(TransientDownloadError):
            _fetcher()._validate(target, CYCLE, 0)
        assert not target.exists()

    def test_invalid_response_deletes_the_file(self, tmp_path: Path):
        """Never leave a bogus payload where a later stage could read it."""
        target = tmp_path / "bad.grib2"
        target.write_bytes(b"<html>nope</html>")
        with pytest.raises(InvalidGribResponseError):
            _fetcher()._validate(target, CYCLE, 0)
        assert not target.exists()

    def test_error_message_names_the_endpoint(self, tmp_path: Path):
        """The endpoint is the thing being diagnosed, so it belongs in the message."""
        target = tmp_path / "bad.grib2"
        target.write_bytes(b"<html>nope</html>")
        with pytest.raises(InvalidGribResponseError, match=INTERNAL_ENDPOINT):
            _fetcher(INTERNAL_ENDPOINT)._validate(target, CYCLE, 0)

    def test_is_not_retried_by_the_worker(self):
        """Subclassing UnprocessableInputError makes the worker ack, not retry."""
        assert issubclass(InvalidGribResponseError, UnprocessableInputError)


class TestScanGrib:
    def test_counts_concatenated_messages(self, tmp_path: Path):
        target = tmp_path / "multi.grib2"
        target.write_bytes(_valid_grib(messages=13))
        scan = _scan_grib(target)
        assert scan.message_count == 13
        assert scan.ends_at_eof

    def test_a_whole_file_tiles_its_bytes_exactly(self, tmp_path: Path):
        """The property that separates truncation from reduced coverage."""
        target = tmp_path / "whole.grib2"
        target.write_bytes(_valid_grib(messages=4))
        assert _scan_grib(target).ends_at_eof

    def test_returns_zero_for_a_non_grib_file(self, tmp_path: Path):
        target = tmp_path / "text.grib2"
        target.write_bytes(b"not a grib at all")
        assert _scan_grib(target).message_count == 0

    def test_flags_a_message_cut_short(self, tmp_path: Path):
        """Half a message left over: the walk cannot land on EOF."""
        whole = _valid_grib(messages=4)
        target = tmp_path / "truncated.grib2"
        target.write_bytes(whole[: len(whole) - 10])
        scan = _scan_grib(target)
        assert scan.message_count == 3
        assert not scan.ends_at_eof

    def test_stops_on_a_truncated_trailing_header(self, tmp_path: Path):
        target = tmp_path / "truncated.grib2"
        target.write_bytes(_valid_grib(messages=2) + b"GRIB\x00\x00")
        scan = _scan_grib(target)
        assert scan.message_count == 2
        assert not scan.ends_at_eof

    def test_does_not_loop_on_a_zero_length_message(self, tmp_path: Path):
        """A malformed length field must terminate the walk, not spin forever."""
        target = tmp_path / "zero.grib2"
        target.write_bytes(b"GRIB" + b"\x00" * 4 + (0).to_bytes(8, "big"))
        assert _scan_grib(target).message_count == 0


# ---------------------------------------------------------------------------
# Access config
# ---------------------------------------------------------------------------


class TestAccessConfig:
    def test_rejects_non_positive_concurrency(self):
        with pytest.raises(ValueError, match="max_concurrent_downloads"):
            GfsAccessConfig(subset_endpoint=PUBLIC_ENDPOINT, max_concurrent_downloads=0)

    def test_an_absent_endpoint_is_tolerated_until_used(self):
        """Config is built in many contexts that never touch GFS."""
        access = GfsAccessConfig(subset_endpoint="")
        assert not access.is_configured

    def test_building_a_fetcher_without_an_endpoint_fails_loudly(self):
        with pytest.raises(ValueError, match="GFS_SUBSET_ENDPOINT"):
            GfsGribFetcher(GfsAccessConfig(subset_endpoint=""), BOUNDS)
