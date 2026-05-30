"""Tests for the CG_GLM-L2-GLMF filename parser."""

from datetime import datetime, timezone

import pytest

from models.glm_folder_config import parse_glm_folder_filename


def test_parses_sample_filename():
    parts = parse_glm_folder_filename(
        "CG_GLM-L2-GLMF-M3_G19_s20260611400000_e20260611401000_c20260611402030.nc"
    )

    assert parts.platform == "G19"
    assert parts.mode == "M3"
    # day-of-year 061 in 2026 = March 2; 14:00:00.0 UTC
    assert parts.start_dt == datetime(2026, 3, 2, 14, 0, 0, tzinfo=timezone.utc)
    assert parts.end_dt == datetime(2026, 3, 2, 14, 1, 0, tzinfo=timezone.utc)


def test_decisecond_is_preserved_as_microseconds():
    # Last digit "5" → 500_000 µs (0.5 s)
    parts = parse_glm_folder_filename(
        "CG_GLM-L2-GLMF-M3_G19_s20260010000005_e20260010000015_c20260010000105.nc"
    )
    assert parts.start_dt.microsecond == 500_000
    assert parts.end_dt.microsecond == 500_000


def test_handles_different_mode_suffix():
    parts = parse_glm_folder_filename(
        "CG_GLM-L2-GLMF-M6_G19_s20260611400000_e20260611401000_c20260611402030.nc"
    )
    assert parts.mode == "M6"


def test_rejects_non_glm_filename():
    with pytest.raises(ValueError, match="Not a CG_GLM-L2-GLMF"):
        parse_glm_folder_filename(
            "OR_GLM-L2-LCFA_G19_s20260611400000_e20260611401000_c20260611402030.nc"
        )


def test_rejects_malformed_timestamp_token():
    with pytest.raises(ValueError, match="Invalid GOES timestamp token"):
        parse_glm_folder_filename(
            "CG_GLM-L2-GLMF-M3_G19_sBADTIME000000_e20260611401000_c20260611402030.nc"
        )


def test_rejects_filename_with_too_few_parts():
    with pytest.raises(ValueError, match="Not a CG_GLM-L2-GLMF"):
        parse_glm_folder_filename("CG_GLM-L2-GLMF-M3_G19_s20260611400000.nc")
