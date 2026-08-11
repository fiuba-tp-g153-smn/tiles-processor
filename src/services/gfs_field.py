"""Load a georeferenced field out of a GFS GRIB subset.

Shared by both GFS processors. Everything specific to how NOMADS hands us the
data lives here, so the processors deal in ready-to-use DataArrays.
"""

import logging
from pathlib import Path

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)

_ISOBARIC = "isobaricInhPa"


def load_field(
    grib_path: Path,
    short_name: str,
    bounds: dict[str, float],
    level_hpa: int | None = None,
) -> xr.DataArray:
    """Read one variable (optionally at one isobaric level) ready for rendering.

    Args:
        grib_path: Path to the cached GRIB2 subset.
        short_name: cfgrib short name — `mslet`, `gh`, `t`, `u`, `v`.
        bounds: Clip box in EPSG:4326, -180..180 longitudes.
        level_hpa: Isobaric level to select; None for single-level fields.

    Returns:
        A 2-D DataArray with `x`/`y` dims, EPSG:4326 CRS and the project's
        longitude convention.
    """
    dataset = _open_variable(grib_path, short_name, isobaric=level_hpa is not None)
    try:
        field = dataset[short_name]
        if level_hpa is not None:
            field = field.sel({_ISOBARIC: level_hpa})
        field = field.load()
    finally:
        dataset.close()

    return clip(to_geographic(field), bounds)


def load_levels(
    grib_path: Path,
    short_name: str,
    bounds: dict[str, float],
    levels: tuple[int, ...],
) -> dict[int, xr.DataArray]:
    """Read one isobaric variable at several levels from a single open.

    cfgrib rebuilds its message index on every `open_dataset`, so calling
    `load_field` once per level pays that cost once per level for no reason.
    A product that needs the same variable at more than one level — the
    1000/500 thickness — should open once and slice.

    Args:
        grib_path: Path to the cached GRIB2 subset.
        short_name: cfgrib short name of an isobaric variable, e.g. `gh`.
        bounds: Clip box in EPSG:4326, -180..180 longitudes.
        levels: Isobaric levels to select, in hPa.

    Returns:
        One ready-to-render 2-D DataArray per requested level, keyed by level.
    """
    dataset = _open_variable(grib_path, short_name, isobaric=True)
    try:
        raw = {
            level: dataset[short_name].sel({_ISOBARIC: level}).load()
            for level in levels
        }
    finally:
        dataset.close()

    return {level: clip(to_geographic(field), bounds) for level, field in raw.items()}


def _open_variable(grib_path: Path, short_name: str, isobaric: bool) -> xr.Dataset:
    """Open the GRIB filtered down to one variable (optionally isobaric only)."""
    filter_keys: dict[str, object] = {"shortName": short_name}
    if isobaric:
        filter_keys["typeOfLevel"] = _ISOBARIC
    return xr.open_dataset(
        grib_path, engine="cfgrib", backend_kwargs={"filter_by_keys": filter_keys}
    )


def to_geographic(field: xr.DataArray) -> xr.DataArray:
    """Put a GRIB field into the conventions the rest of the pipeline expects.

    Three fixes, all of them mandatory:

    * **Longitudes.** NOMADS answers in 0-360 (the project's -110..-30 comes
      back as 250..330). Clipping a 250..330 array to minx=-110 yields an empty
      selection — a silent, empty-output failure — so the axis is shifted and
      re-sorted first.
    * **Latitude order.** Rasters are written north-up, so a south-to-north axis
      is flipped. Which way the subset arrives is not assumed: NOMADS answers
      north-to-south, but a mirror need not, so the order is checked and only
      flipped when it actually needs flipping.
    * **Dimension names.** rioxarray wants `x`/`y`, not `longitude`/`latitude`.
    """
    import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

    field = field.drop_vars(
        [c for c in ("step", "time", "valid_time", _ISOBARIC) if c in field.coords],
        errors="ignore",
    )
    field.attrs.pop("grid_mapping", None)

    rename = {}
    if "latitude" in field.dims:
        rename["latitude"] = "y"
    if "longitude" in field.dims:
        rename["longitude"] = "x"
    if rename:
        field = field.rename(rename)

    lon = field["x"].values
    if float(np.max(lon)) > 180.0:
        field = field.assign_coords(x=((lon + 180.0) % 360.0) - 180.0).sortby("x")

    if field["y"].values[0] < field["y"].values[-1]:
        field = field.sortby("y", ascending=False)

    field.rio.write_crs("EPSG:4326", inplace=True)
    field.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    field.rio.write_nodata(float("nan"), inplace=True)
    return field


def clip(field: xr.DataArray, bounds: dict[str, float]) -> xr.DataArray:
    """Clip to the project bounds.

    NOMADS already subsets server-side from the same bounds, so this is normally
    a no-op — it exists so a mirror serving a wider domain still produces tiles
    aligned with every other layer.
    """
    return field.rio.clip_box(
        minx=bounds["minx"],
        miny=bounds["miny"],
        maxx=bounds["maxx"],
        maxy=bounds["maxy"],
    )


def coordinate_mesh(field: xr.DataArray) -> tuple[np.ndarray, np.ndarray]:
    """2-D lon/lat meshes matching `field`, as the barb helpers expect."""
    return np.meshgrid(field["x"].values, field["y"].values)
