"""Tests for Rainbow5 (.vol) header and filename parsing."""

import pytest

from models.rainbow_header import (
    HEADER_WINDOW_BYTES,
    RainbowHeaderError,
    parse_inta_filename,
    parse_inta_timestamp,
    parse_rainbow_header,
)


def test_parses_identity_and_geometry(rainbow_header):
    header = parse_rainbow_header(rainbow_header())
    assert header.radar_id == "PAR"
    assert header.radar_name == "INTA_Parana"
    assert header.latitude == pytest.approx(-31.848438)
    assert header.longitude == pytest.approx(-60.537289)
    assert header.altitude_m == pytest.approx(100.0)
    assert header.stop_range_km == pytest.approx(240.0)
    assert header.elevation_count == 12
    assert header.scan_name == "VOL_240_ALL.vol"


def test_radarinfo_past_16kb_is_still_found(rainbow_header):
    # The tag sits ~18.7 KB in on real files; the read window must cover it.
    window = rainbow_header()
    assert window.index(b"<radarinfo") > 16 * 1024
    assert len(window) < HEADER_WINDOW_BYTES
    assert parse_rainbow_header(window).radar_id == "PAR"


def test_truncated_window_fails_loudly(rainbow_header):
    # A window that stops short must raise, never silently mis-identify.
    with pytest.raises(RainbowHeaderError, match="no <radarinfo>"):
        parse_rainbow_header(rainbow_header()[:16384])


def test_rejects_non_rainbow_content():
    with pytest.raises(RainbowHeaderError):
        parse_rainbow_header(b"\x89HDF\r\n\x1a\n" + b"\x00" * 1000)


def test_radar_id_is_upper_cased(rainbow_header):
    assert parse_rainbow_header(rainbow_header(radar_id="par")).radar_id == "PAR"


def test_missing_id_attribute_rejected(rainbow_header):
    window = rainbow_header().replace(b'id="PAR"', b"")
    with pytest.raises(RainbowHeaderError, match="no id attribute"):
        parse_rainbow_header(window)


def test_short_range_scan_is_reported_as_such(rainbow_header):
    # Anguil/Pergamino interleave these; the caller filters on stop_range_km.
    header = parse_rainbow_header(
        rainbow_header(stop_range="120", numele="8", scan_name="SMN_120.vol")
    )
    assert header.stop_range_km == pytest.approx(120.0)
    assert header.scan_name == "SMN_120.vol"


@pytest.mark.parametrize(
    "filename,radar_id,timestamp,variable",
    [
        # SMN sample naming: no radar token.
        ("2026052115100400dBZ.vol", "", "20260521T151004Z", "dBZ"),
        # Production naming: radar token prefixed.
        ("PAR2026061920000300dBZ.vol", "PAR", "20260619T200003Z", "dBZ"),
        ("ANG2026091501442900RhoHV.vol", "ANG", "20260915T014429Z", "RhoHV"),
        ("2026091501442900KDP.vol", "", "20260915T014429Z", "KDP"),
    ],
)
def test_parse_filename_both_conventions(filename, radar_id, timestamp, variable):
    parsed = parse_inta_filename(filename)
    assert parsed["radar_id"] == radar_id
    assert parsed["timestamp"] == timestamp
    assert parsed["variable"] == variable


def test_timestamp_matches_the_radar_tileset_format():
    # The visualizer's parseRadarTimestamp reads YYYYMMDD'T'HHMMSS, the same
    # shape the SINARAME filenames already carry.
    parsed = parse_inta_filename("2026052115100400dBZ.vol")
    assert parse_inta_timestamp(parsed["timestamp"]).isoformat() == (
        "2026-05-21T15:10:04+00:00"
    )


@pytest.mark.parametrize(
    "filename",
    [
        # .azi products are not volumetric and must never be ingested.
        "2026091501472600dBZ.azi",
        "OneDrive_1_9-18-2026.zip",
        "RMA1_0315_01_DBZH_20260114T170328Z.H5",
        "garbage.vol",
        "2026dBZ.vol",
    ],
)
def test_parse_filename_rejects_foreign_names(filename):
    with pytest.raises(ValueError, match="Invalid INTA radar filename"):
        parse_inta_filename(filename)
