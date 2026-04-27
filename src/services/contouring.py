"""Pure functions to smooth a 2-D field and extract simplified isolines as GeoJSON."""

import json
from pathlib import Path

import numpy as np
import xarray as xr
from contourpy import contour_generator
from scipy.ndimage import gaussian_filter
from shapely.geometry import LineString, mapping


def smooth_field(da: xr.DataArray, sigma: float) -> xr.DataArray:
    """Apply Gaussian smoothing to a DataArray, preserving NaN locations.

    `scipy.ndimage.gaussian_filter` propagates NaN to neighboring cells, so we
    fill NaN with the array mean before filtering and re-mask afterwards.
    """
    arr = np.asarray(da.values, dtype=np.float32)
    nan_mask = np.isnan(arr)
    if nan_mask.all():
        return da.copy()

    fill_value = float(np.nanmean(arr))
    arr_filled = np.where(nan_mask, fill_value, arr)
    smoothed = gaussian_filter(arr_filled, sigma=sigma)
    smoothed = np.where(nan_mask, np.nan, smoothed)
    return da.copy(data=smoothed)


def extract_isolines(
    da: xr.DataArray,
    step: float,
    simplify_tolerance: float,
    value_property: str = "value",
) -> list[dict]:
    """Extract isolines at every multiple of `step` within the data range.

    Args:
        da: 2-D georeferenced DataArray with `x` and `y` coords.
        step: Spacing between successive contour levels (data units).
        simplify_tolerance: Tolerance passed to `shapely.simplify`. Same units
            as the spatial coordinates (degrees for EPSG:4326).
        value_property: Name of the GeoJSON property holding the contour value.

    Returns:
        List of GeoJSON Feature dicts (LineString geometries).
    """
    arr = np.asarray(da.values, dtype=np.float64)
    if np.all(np.isnan(arr)):
        return []

    vmin = float(np.nanmin(arr))
    vmax = float(np.nanmax(arr))
    if vmin == vmax:
        return []

    lo = float(np.ceil(vmin / step) * step)
    hi = float(np.floor(vmax / step) * step)
    if lo > hi:
        return []
    levels = np.arange(lo, hi + step / 2, step)

    x_coords = np.asarray(da["x"].values, dtype=np.float64)
    y_coords = np.asarray(da["y"].values, dtype=np.float64)

    gen = contour_generator(
        x=x_coords,
        y=y_coords,
        z=arr,
        name="serial",
        corner_mask=True,
        line_type="SeparateCode",
    )

    features: list[dict] = []
    for level in levels:
        lines, _codes = gen.lines(float(level))
        for seg in lines:
            if len(seg) < 2:
                continue
            line = LineString(seg)
            simplified = line.simplify(simplify_tolerance, preserve_topology=False)
            if simplified.is_empty or simplified.length == 0:
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(simplified),
                    "properties": {value_property: float(level)},
                }
            )
    return features


def write_geojson(features: list[dict], output_path: Path) -> Path:
    """Serialize features as a GeoJSON FeatureCollection at output_path.

    Per RFC 7946, GeoJSON coordinates are always WGS84 (lon, lat) and the
    deprecated `crs` member is intentionally omitted.
    """
    fc = {
        "type": "FeatureCollection",
        "features": features,
    }
    output_path.write_text(json.dumps(fc, separators=(",", ":")), encoding="utf-8")
    return output_path
