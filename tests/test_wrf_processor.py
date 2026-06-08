import os
import sys
import warnings

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import numpy as np
import pytest
from rasterio.errors import NotGeoreferencedWarning

from processors.wrf_processor import WrfProcessor


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
            WrfProcessor._save_float_geotiff_gcp(data, lat, lon, out)

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
