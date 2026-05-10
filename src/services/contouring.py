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


def smooth_array(arr: np.ndarray, sigma: float) -> np.ndarray:
    """Gaussian-smooth a 2-D array, preserving NaN locations.

    Uses 0-fill (matching `WRF/generar_wrf.py` for BRN / SLP / shear): NaN
    pixels are replaced with 0 before the Gaussian filter, then re-masked
    to NaN afterwards. Filling with `nanmean` instead biases the smoothed
    field around the NaN edges and produces dense, fragmented contours
    (hundreds of small loops instead of a few smooth lines).
    """
    nan_mask = np.isnan(arr)
    if nan_mask.all() or sigma <= 0:
        return arr
    arr_filled = np.where(nan_mask, 0.0, arr)
    smoothed = gaussian_filter(arr_filled, sigma=sigma)
    return np.where(nan_mask, np.nan, smoothed)


def extract_isolines_2d(
    z: np.ndarray,
    x_2d: np.ndarray,
    y_2d: np.ndarray,
    levels: list[float] | tuple[float, ...],
    simplify_tolerance: float,
    value_property: str = "value",
) -> list[dict]:
    """Extract isolines on an irregular 2-D grid (e.g. Lambert WRF lat/lon)."""
    arr = np.asarray(z, dtype=np.float64)
    if np.all(np.isnan(arr)):
        return []

    gen = contour_generator(
        x=np.asarray(x_2d, dtype=np.float64),
        y=np.asarray(y_2d, dtype=np.float64),
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


def extract_barbs(
    u_ms: np.ndarray,
    v_ms: np.ndarray,
    lon_2d: np.ndarray,
    lat_2d: np.ndarray,
    stride: int,
    ms_to_kt: float = 1.94384,
) -> list[dict]:
    """Subsample a wind field and emit one Point Feature per barb position.

    u/v are expected in m/s; speed and components are emitted in knots so the
    frontend can render with a consistent unit.

    Properties:
        speed_kt: Wind speed in knots.
        dir_deg:  Meteorological wind direction (degrees from which the wind
                  blows, 0=N, 90=E).
        u_kt, v_kt: Component values in knots.
    """
    u = np.asarray(u_ms, dtype=np.float64)
    v = np.asarray(v_ms, dtype=np.float64)
    lon = np.asarray(lon_2d, dtype=np.float64)
    lat = np.asarray(lat_2d, dtype=np.float64)

    sl = (slice(None, None, stride), slice(None, None, stride))
    u_s = u[sl] * ms_to_kt
    v_s = v[sl] * ms_to_kt
    lon_s = lon[sl]
    lat_s = lat[sl]

    valid = np.isfinite(u_s) & np.isfinite(v_s) & np.isfinite(lon_s) & np.isfinite(lat_s)
    speed = np.hypot(u_s, v_s)
    # Meteorological direction: 0° = wind from north, increases clockwise
    dir_deg = (np.degrees(np.arctan2(-u_s, -v_s)) + 360.0) % 360.0

    features: list[dict] = []
    for j, i in zip(*np.where(valid)):
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon_s[j, i]), float(lat_s[j, i])],
                },
                "properties": {
                    "speed_kt": round(float(speed[j, i]), 2),
                    "dir_deg": round(float(dir_deg[j, i]), 1),
                    "u_kt": round(float(u_s[j, i]), 2),
                    "v_kt": round(float(v_s[j, i]), 2),
                },
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
