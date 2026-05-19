"""Tests for the folder-pipeline GLM palettes and BandConfig entries."""

import re

import pytest

from models.band_config import BAND_CONFIGS, get_band_config
from services.generate_geotiff_files import GenerateGeoTIFFFilesService


_HEX_RE = re.compile(r"^#[0-9a-f]{6}$")


@pytest.fixture(
    params=[
        "GLM_FOLDER_FED_PALETTE",
        "GLM_FOLDER_TOE_PALETTE",
        "GLM_FOLDER_MFA_PALETTE",
    ]
)
def palette_name(request):
    return request.param


def test_palette_is_256_hex_entries(palette_name):
    palette = GenerateGeoTIFFFilesService.get_palette(palette_name)
    assert len(palette) == 256
    assert all(_HEX_RE.match(entry) for entry in palette), palette[:3]


def test_get_palette_rejects_unknown_name():
    with pytest.raises(ValueError, match="Unknown palette"):
        GenerateGeoTIFFFilesService.get_palette("DOES_NOT_EXIST")


def test_fed_palette_starts_at_reference_navy_and_ends_off_white():
    """The first/last entries should match the FED reference list endpoints."""
    palette = GenerateGeoTIFFFilesService.get_palette("GLM_FOLDER_FED_PALETTE")
    # First stop in grafico_glmtools_viejo.py:102-105 is "#0000b8"; sampling
    # at fraction 0 of a LinearSegmentedColormap returns the first stop exactly.
    assert palette[0] == "#0000b8"
    # Last stop "#f9e5e7" — last sample may differ by 1 LSB due to int() floor
    # in the (r*255) conversion, so be lenient about each channel.
    last_r, last_g, last_b = (
        int(palette[-1][1:3], 16),
        int(palette[-1][3:5], 16),
        int(palette[-1][5:7], 16),
    )
    ref_r, ref_g, ref_b = 0xF9, 0xE5, 0xE7
    assert abs(last_r - ref_r) <= 1
    assert abs(last_g - ref_g) <= 1
    assert abs(last_b - ref_b) <= 1


def test_magma_palette_goes_dark_to_light():
    palette = GenerateGeoTIFFFilesService.get_palette("GLM_FOLDER_TOE_PALETTE")
    start_brightness = sum(int(palette[0][i : i + 2], 16) for i in (1, 3, 5))
    end_brightness = sum(int(palette[-1][i : i + 2], 16) for i in (1, 3, 5))
    assert start_brightness < 30  # near-black at low end
    assert end_brightness > 600  # bright cream at high end


def test_viridis_r_palette_starts_yellow_ends_purple():
    palette = GenerateGeoTIFFFilesService.get_palette("GLM_FOLDER_MFA_PALETTE")
    # viridis_r starts at viridis's high end (yellow) and ends at the low end (purple).
    r0, g0, b0 = (int(palette[0][i : i + 2], 16) for i in (1, 3, 5))
    r1, g1, b1 = (int(palette[-1][i : i + 2], 16) for i in (1, 3, 5))
    # Yellow: high R+G, low B
    assert r0 > 200 and g0 > 200 and b0 < 80
    # Purple: low R+G, mid B
    assert r1 < 80 and g1 < 30 and 60 < b1 < 130


@pytest.mark.parametrize(
    "band_id, expected_vmin, expected_vmax, expected_palette, expected_s3",
    [
        ("glm_folder_fed", 1.0, 128.0, "GLM_FOLDER_FED_PALETTE", "tiles/glm_fed"),
        ("glm_folder_toe", 0.01, 1500.0, "GLM_FOLDER_TOE_PALETTE", "tiles/glm_toe"),
        ("glm_folder_mfa", 64.0, 2500.0, "GLM_FOLDER_MFA_PALETTE", "tiles/glm_mfa"),
    ],
)
def test_glm_folder_band_configs_registered(
    band_id, expected_vmin, expected_vmax, expected_palette, expected_s3
):
    assert band_id in BAND_CONFIGS
    cfg = get_band_config(band_id)
    assert cfg.vmin == expected_vmin
    assert cfg.vmax == expected_vmax
    assert cfg.palette_name == expected_palette
    assert cfg.s3_tiles_prefix == expected_s3
    # All folder-based GLM configs share the CG_GLM-L2-GLMF input pattern.
    assert cfg.file_pattern == "CG_GLM-L2-GLMF"


def test_event_based_glm_configs_unchanged():
    """Phase 3 must NOT alter the existing event-based GLM entries — Phase 4/5 removes them."""
    assert get_band_config("glm_fed").vmax == 256.0
    assert get_band_config("glm_fed").palette_name == "FED_PALETTE"
    assert get_band_config("glm_toe").palette_name == "TOE_PALETTE"
    assert get_band_config("glm_mfa").palette_name == "MFA_PALETTE"
