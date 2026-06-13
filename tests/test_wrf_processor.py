import os
import shutil
import sys
import warnings

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import numpy as np
import pytest
import rasterio
from rasterio.errors import NotGeoreferencedWarning

from processors.wrf_processor import WrfProcessor


def _gdal_cli_available() -> bool:
    """gdalwarp + gdal_translate run inside the GDAL container, not the venv."""
    return bool(shutil.which("gdalwarp") and shutil.which("gdal_translate"))


requires_gdal_cli = pytest.mark.skipif(
    not _gdal_cli_available(),
    reason="gdalwarp/gdal_translate not on PATH (runs inside the GDAL container)",
)


def _curvilinear_grid(n: int = 8):
    """Small 2D lat/lon mesh standing in for the WRF Lambert grid."""
    lon, lat = np.meshgrid(
        np.linspace(-65.0, -60.0, n), np.linspace(-35.0, -30.0, n)
    )
    return lat, lon


class TestGcpWritersSuppressWarning:
    """GCP-tagged writers must not leak the expected NotGeoreferencedWarning.

    Without suppression, opening the rasterio writer before attaching GCPs
    emits NotGeoreferencedWarning to stderr, which the worker mislabels ERROR.
    """

    def test_save_float_geotiff_gcp_emits_no_warning(self, tmp_path):
        lat, lon = _curvilinear_grid()
        data = np.arange(lat.size, dtype="float32").reshape(lat.shape)
        out = tmp_path / "float.tif"

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            WrfProcessor._save_float_geotiff_gcp([data], lat, lon, out)

        assert out.exists()
        assert not any(
            isinstance(w.message, NotGeoreferencedWarning) for w in caught
        )

    def test_save_rgba_geotiff_emits_no_warning(self, tmp_path):
        lat, lon = _curvilinear_grid()
        rgba = np.zeros((*lat.shape, 4), dtype=np.uint8)
        out = tmp_path / "rgba.tif"

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            WrfProcessor._save_rgba_geotiff(rgba, lat, lon, out)

        assert out.exists()
        assert not any(
            isinstance(w.message, NotGeoreferencedWarning) for w in caught
        )

    def test_suppressor_swallows_the_warning(self):
        """The helper itself silences exactly NotGeoreferencedWarning."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with WrfProcessor._suppress_not_georeferenced_warning():
                warnings.warn("expected", NotGeoreferencedWarning)

        assert caught == []


class TestMultibandFloatWriter:
    """_save_float_geotiff_gcp stacks N float fields into one GCP-tagged raster."""

    def test_writes_all_bands_with_nan_nodata(self, tmp_path):
        lat, lon = _curvilinear_grid()
        b1 = np.arange(lat.size, dtype="float32").reshape(lat.shape)
        b2 = (b1 * 10.0).astype("float32")
        b2[0, 0] = np.nan  # distinct per-band NaN mask
        out = tmp_path / "stack.tif"

        WrfProcessor._save_float_geotiff_gcp([b1, b2], lat, lon, out)

        with rasterio.open(out) as ds:
            assert ds.count == 2
            assert ds.gcps[0]  # GCPs attached
            assert np.isnan(ds.nodata)
            assert np.array_equal(ds.read(1), b1, equal_nan=True)
            assert np.array_equal(ds.read(2), b2, equal_nan=True)

    def test_empty_bands_raises(self, tmp_path):
        lat, lon = _curvilinear_grid()
        with pytest.raises(ValueError, match="at least one band"):
            WrfProcessor._save_float_geotiff_gcp([], lat, lon, tmp_path / "x.tif")


@requires_gdal_cli
class TestMultibandWarpEquivalence:
    """One multiband tps warp + per-band split == N separate per-field warps.

    The whole point of the A4 change is that warping the primary + secondary
    float fields together (they share GCPs, dtype, nodata and warp params) and
    splitting the result yields bytes identical to warping each field on its
    own — just far cheaper (one tps solve instead of N+1).
    """

    @staticmethod
    def _read_band(path):
        with rasterio.open(path) as ds:
            return ds.read(1), ds.crs, ds.transform

    def test_stacked_split_matches_separate_warps(self, tmp_path):
        lat, lon = _curvilinear_grid(16)
        ramp = np.arange(lat.size, dtype="float32").reshape(lat.shape)
        field_a = ramp.copy()
        field_b = (ramp * -2.0 + 5.0).astype("float32")
        field_a[0, 0] = np.nan  # distinct per-band NaN masks
        field_b[-1, -1] = np.nan

        cog_co = ("COMPRESS=DEFLATE", "PREDICTOR=3", "BLOCKSIZE=512")

        # Path A — old pipeline: warp each field separately straight to COG.
        separate = []
        for i, field in enumerate((field_a, field_b)):
            gcp = tmp_path / f"sep_{i}_gcp.tif"
            WrfProcessor._save_float_geotiff_gcp([field], lat, lon, gcp)
            cog = tmp_path / f"sep_{i}_cog.tif"
            WrfProcessor._warp_to_epsg4326(
                gcp,
                cog,
                of="COG",
                resampling="bilinear",
                extra_creation_options=cog_co,
            )
            separate.append(cog)

        # Path B — new pipeline: stack, warp once, split each band to COG.
        stack_gcp = tmp_path / "stack_gcp.tif"
        WrfProcessor._save_float_geotiff_gcp([field_a, field_b], lat, lon, stack_gcp)
        stack_warped = tmp_path / "stack_warped.tif"
        WrfProcessor._warp_to_epsg4326(
            stack_gcp,
            stack_warped,
            resampling="bilinear",
            extra_creation_options=("COMPRESS=DEFLATE", "PREDICTOR=3"),
        )
        split = []
        for band in (1, 2):
            cog = tmp_path / f"split_{band}_cog.tif"
            WrfProcessor._split_band_to_cog(stack_warped, band, cog)
            split.append(cog)

        for sep_path, split_path in zip(separate, split):
            sep_arr, sep_crs, sep_t = self._read_band(sep_path)
            split_arr, split_crs, split_t = self._read_band(split_path)
            assert sep_arr.shape == split_arr.shape
            assert np.array_equal(sep_arr, split_arr, equal_nan=True)
            assert sep_crs == split_crs
            assert sep_t.almost_equals(split_t)
