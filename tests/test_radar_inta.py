"""Tests for INTA Rainbow5 .vol filename parsing and dispatcher."""

import pytest

from models.radar_config import (
    parse_inta_radar_filename,
    parse_radar_file,
    INTA_RADAR_ID,
)


class TestParseIntaRadarFilename:
    def test_basic_dbz(self):
        result = parse_inta_radar_filename("2026052115400400dBZ.vol")
        assert result["radar_id"] == INTA_RADAR_ID
        assert result["variable"] == "DBZH"
        assert result["timestamp"] == "20260521T154004Z"
        assert result["subvolume"] == "01"
        assert result["format"] == "rainbow5"

    def test_timestamp_normalized(self):
        result = parse_inta_radar_filename("2025010312300000dBZ.vol")
        assert result["timestamp"] == "20250103T123000Z"

    def test_uppercase_vol_extension(self):
        result = parse_inta_radar_filename("2026052115400400dBZ.VOL")
        assert result["variable"] == "DBZH"

    def test_unknown_variable_raises(self):
        with pytest.raises(ValueError, match="Unknown INTA variable"):
            parse_inta_radar_filename("2026052115400400UNKN.vol")

    def test_missing_vol_extension_raises(self):
        with pytest.raises(ValueError, match="Expected .vol extension"):
            parse_inta_radar_filename("2026052115400400dBZ.h5")

    def test_malformed_filename_raises(self):
        with pytest.raises(ValueError, match="Invalid INTA radar filename format"):
            parse_inta_radar_filename("notaradarfile.vol")


class TestParseRadarFileDispatch:
    def test_h5_dispatches_to_sinarame(self):
        result = parse_radar_file("RMA1_0315_01_DBZH_20260114T170328Z.H5")
        assert result["format"] == "sinarame"
        assert result["radar_id"] == "RMA1"
        assert result["variable"] == "DBZH"

    def test_lowercase_h5_dispatches_to_sinarame(self):
        result = parse_radar_file("RMA1_0315_01_DBZH_20260114T170328Z.h5")
        assert result["format"] == "sinarame"

    def test_vol_dispatches_to_rainbow5(self):
        result = parse_radar_file("2026052115400400dBZ.vol")
        assert result["format"] == "rainbow5"
        assert result["radar_id"] == INTA_RADAR_ID
        assert result["variable"] == "DBZH"

    def test_unsupported_extension_raises(self):
        with pytest.raises(ValueError, match="Unsupported radar file extension"):
            parse_radar_file("somefile.nc")

    def test_sinarame_has_subvolume(self):
        result = parse_radar_file("RMA1_0315_01_DBZH_20260114T170328Z.H5")
        assert result["subvolume"] == "01"

    def test_rainbow5_subvolume_always_01(self):
        result = parse_radar_file("2026052115400400dBZ.vol")
        assert result["subvolume"] == "01"

    def test_image_id_format_compatible(self):
        """Verify both parsers produce image_id-compatible output."""
        rma = parse_radar_file("RMA1_0315_01_DBZH_20260114T170328Z.H5")
        inta = parse_radar_file("2026052115400400dBZ.vol")

        rma_id = f"{rma['radar_id']}_{rma['variable']}_{rma['timestamp']}"
        inta_id = f"{inta['radar_id']}_{inta['variable']}_{inta['timestamp']}"

        assert rma_id == "RMA1_DBZH_20260114T170328Z"
        assert inta_id == f"{INTA_RADAR_ID}_DBZH_20260521T154004Z"
