"""Band-2 memory-invariant tests.

Band 2 (21696x21696, ~470M px) is the pipeline's top OOM path. Two CLAUDE.md
"critical gotcha" invariants keep it under control and are asserted here without
touching GDAL:

  1. The raw Rad is opened with ``mask_and_scale=False`` (int16, ~940 MB) rather
     than CF-decoded to float64 (~3.76 GB).
  2. The array is coarsened BEFORE ``scale_factor``/``add_offset`` are applied, so
     the decode runs on the small (downsampled) array.
  3. The Band-2 template override (not the module-level georeferencing) runs.

``xr.open_dataset`` and ``pyproj.CRS.from_cf`` are mocked; the real xarray coarsen
runs on tiny fixtures.
"""

import sys
import os
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
import xarray as xr

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from processors.band2_processor import Band2Processor
from processors.goes_processor import GoesProcessor

_SAT_H = 35786023.0
_SCALE = np.float32(0.5)
_OFFSET = np.float32(1.0)
_KAPPA0 = 2.0


def _cf_dataset() -> xr.Dataset:
    """CF-decoded first open: metadata + coordinates only (Rad data unused)."""
    proj = xr.DataArray(
        0,
        attrs={
            "grid_mapping_name": "geostationary",
            "perspective_point_height": _SAT_H,
        },
    )
    # Rad values here are deliberately zeros: if the processor (wrongly) read this
    # CF array instead of the raw int16, the output would collapse to the offset.
    rad = xr.DataArray(np.zeros((8, 8), dtype=np.float64), dims=["y", "x"])
    rad.encoding = {"scale_factor": _SCALE, "add_offset": _OFFSET}
    return xr.Dataset(
        {
            "Rad": rad,
            "kappa0": xr.DataArray(_KAPPA0),
            "goes_imager_projection": proj,
        },
        coords={
            "x": np.arange(8, dtype=float),
            "y": np.arange(8, dtype=float),
        },
    )


def _raw_dataset() -> xr.Dataset:
    """Raw (mask_and_scale=False) second open: int16 Rad the processor coarsens."""
    raw = xr.DataArray(np.arange(64, dtype=np.int16).reshape(8, 8), dims=["y", "x"])
    return xr.Dataset({"Rad": raw})


@pytest.fixture
def open_dataset_spy():
    """Patch xr.open_dataset to return CF vs raw datasets and record the calls."""
    cf_ds, raw_ds = _cf_dataset(), _raw_dataset()

    def fake_open(_path, **kwargs):
        return raw_ds if kwargs.get("mask_and_scale") is False else cf_ds

    with mock.patch(
        "processors.band2_processor.xr.open_dataset", side_effect=fake_open
    ) as spy, mock.patch(
        "pyproj.CRS.from_cf",
        return_value=SimpleNamespace(to_string=lambda: "EPSG:4326"),
    ):
        yield spy


def _run_georeferencing() -> xr.Dataset:
    """Invoke the Band-2 override with a minimal ``self`` (only DOWNSAMPLE_FACTOR)."""
    fake_self = SimpleNamespace(DOWNSAMPLE_FACTOR=Band2Processor.DOWNSAMPLE_FACTOR)
    return Band2Processor._apply_georeferencing(fake_self, "dummy.nc")


def test_raw_open_uses_mask_and_scale_false(open_dataset_spy):
    """The raw Rad load must skip CF decoding (int16, not float64)."""
    _run_georeferencing()

    calls = open_dataset_spy.call_args_list
    assert len(calls) == 2, "expected a metadata open then a raw int16 open"
    # First (metadata) open is CF-decoded: it must NOT force mask_and_scale=False.
    assert calls[0].kwargs.get("mask_and_scale") is not False
    # Second open loads raw int16 — the memory-critical invariant.
    assert calls[1].kwargs.get("mask_and_scale") is False


def test_coarsen_before_scale_from_raw_int16(open_dataset_spy):
    """Output = coarsen(raw int16) THEN scale/offset — proving decode-after-coarsen."""
    result = _run_georeferencing()

    raw2d = np.arange(64, dtype=np.float64).reshape(8, 8)
    coarsened = raw2d.reshape(2, 4, 2, 4).mean(axis=(1, 3))  # 4x coarsen (trim)
    expected = (coarsened.astype(np.float32) * _SCALE + _OFFSET).astype(np.float32)

    rad = result["Rad"]
    assert rad.shape == (2, 2), "must be downsampled 4x before decode"
    assert rad.dtype == np.float32
    np.testing.assert_allclose(rad.values, expected, rtol=1e-6)


def test_result_carries_kappa0_and_crs(open_dataset_spy):
    """Reflectance metadata (kappa0) and the CRS survive into the output dataset."""
    result = _run_georeferencing()
    assert float(result["kappa0"].values) == _KAPPA0
    assert result.rio.crs is not None


def test_georeferencing_is_overridden():
    """Band 2 must override the base georeferencing (never the module-level fn)."""
    assert (
        Band2Processor._apply_georeferencing is not GoesProcessor._apply_georeferencing
    )
