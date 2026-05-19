"""Aggregate pre-gridded GLM 1-minute imagery into a single N-minute window.

This module is the in-codebase port of `data/glm_codigos/sumo_archivos_glm.py`
adapted for the project's dependency stack:

  * Opens CG_GLM-L2-GLMF-* netCDFs with the ``h5netcdf`` engine (the default
    ``netCDF4`` engine fails on these files with an HDF error).
  * Reuses :func:`glmtools.io.imagery.aggregate` for the actual temporal
    reduction (sum for extensive vars, min for ``minimum_flash_area``).
  * Reprojects the aggregated grid from the GOES GEOS satellite projection
    to EPSG:4326 using the projection metadata carried in
    ``goes_imager_projection``.

The output of :func:`aggregate_glm_window` is still in the GOES GEOS
projection; call :func:`reproject_to_latlon` to obtain a per-variable
:class:`xarray.DataArray` ready for the existing colorize / tile pipeline.
"""

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import rioxarray  # noqa: F401  # pylint: disable=unused-import  # registers .rio
import xarray as xr
from glmtools.io.imagery import aggregate
from pyproj import CRS


def _load_time_series(files: list[Path]) -> xr.Dataset:
    """Stack 1-minute GLM grids along a new ``time`` dimension.

    Replicates :func:`glmtools.io.imagery.open_glm_time_series` but pins the
    xarray engine to ``h5netcdf`` so the CG_GLM-L2-GLMF files actually load.
    """
    if not files:
        raise ValueError("aggregate_glm_window requires at least one file")

    starts: list[pd.Timestamp] = []
    ends: list[pd.Timestamp] = []
    for path in files:
        with xr.open_dataset(path, engine="h5netcdf") as ds:
            starts.append(
                pd.Timestamp(ds.attrs["time_coverage_start"]).tz_localize(None)
            )
            ends.append(pd.Timestamp(ds.attrs["time_coverage_end"]).tz_localize(None))

    series = xr.open_mfdataset(
        [str(p) for p in files],
        concat_dim="time",
        combine="nested",
        engine="h5netcdf",
    )
    series = series.assign_coords(time=("time", starts))
    series = series.set_index({"time": "time"}).set_coords("time")
    series.attrs["time_coverage_start"] = min(starts).isoformat()
    series.attrs["time_coverage_end"] = max(ends).isoformat()
    return series


def aggregate_glm_window(
    files: list[Path],
    window_start: datetime,
    window_end: datetime,
    accum_minutes: int,
) -> xr.Dataset:
    """Aggregate N 1-minute GLM grids into a single bin of ``accum_minutes`` length.

    Args:
        files: 1-minute CG_GLM-L2-GLMF netCDFs covering the window.
        window_start: Inclusive UTC start of the aggregation window.
        window_end: Exclusive UTC end of the aggregation window.
        accum_minutes: Size of each aggregation bin in minutes.

    Returns:
        Aggregated :class:`xarray.Dataset` (still in GOES GEOS coords) with the
        ``time_bins`` dimension renamed to ``time`` and reduced to scalar when a
        single bin covers the window. ``goes_imager_projection`` is preserved.

    Raises:
        ValueError: If ``files`` is empty or the resulting aggregation produces
            no time bins (e.g. the window does not overlap the provided files).
    """
    # glmtools.aggregate uses tz-naive datetimes internally; strip tzinfo to match.
    win_start_naive = window_start.replace(tzinfo=None)
    win_end_naive = window_end.replace(tzinfo=None)

    series = _load_time_series(files)
    aggregated = aggregate(series, accum_minutes, [win_start_naive, win_end_naive])

    if aggregated.sizes.get("time_bins", 0) == 0:
        raise ValueError(
            f"Aggregation produced 0 time bins for window "
            f"{window_start.isoformat()}..{window_end.isoformat()}"
        )

    aggregated = aggregated.assign_coords(
        time_bins=[interval.left for interval in aggregated.time_bins.values]
    )
    aggregated = aggregated.rename({"time_bins": "time"})

    # glmtools.aggregate uses sum(skipna=True) for the extensive variables, which
    # returns 0 (not NaN) for cells that were NaN across every minute of the
    # window. Treat those 0s as "no lightning" so downstream colorization sees a
    # missing-value sentinel and renders the cell transparent — same convention
    # as minimum_flash_area, which glmtools already returns as NaN.
    for var in ("flash_extent_density", "total_energy"):
        if var in aggregated.data_vars:
            aggregated[var] = aggregated[var].where(aggregated[var] > 0)

    # CG_GLM-L2-GLMF ships ``total_energy`` in nanojoules. The SMN reference
    # (data/glm_codigos/grafico_glmtools_viejo.py:122) and our BandConfig
    # vmin/vmax for TOE are in femtojoules (1 nJ = 1e6 fJ). Convert here so
    # every downstream consumer — COG, tiles, BandConfig — operates in fJ. This
    # also keeps the BandConfig literal range readable (0.01, 1500), at the
    # same magnitude as FED's (1, 128) and MFA's (64, 2500).
    if "total_energy" in aggregated.data_vars:
        aggregated["total_energy"] = aggregated["total_energy"] * 1e6
        aggregated["total_energy"].attrs["units"] = "fJ"

    return aggregated


def reproject_to_latlon(
    dataset: xr.Dataset,
    var_name: str,
    bounds: dict[str, float],
    resolution_deg: float,
) -> xr.DataArray:
    """Reproject one variable of an aggregated GLM dataset to EPSG:4326.

    The input is expected to come from :func:`aggregate_glm_window` and carry
    the ``goes_imager_projection`` variable with the standard CF attributes
    (``perspective_point_height``, ``semi_major_axis``, ``semi_minor_axis``,
    ``longitude_of_projection_origin``).

    Args:
        dataset: Aggregated GLM dataset in GOES GEOS coordinates.
        var_name: Variable to extract (e.g. ``"flash_extent_density"``).
        bounds: Geographic clip box in degrees with keys ``minx``/``maxx``/
            ``miny``/``maxy``.
        resolution_deg: Target pixel size for the reprojected grid.

    Returns:
        A georeferenced :class:`xarray.DataArray` in EPSG:4326 clipped to
        ``bounds``, with the ``time`` singleton dimension squeezed out.
    """
    if "goes_imager_projection" not in dataset.variables:
        raise ValueError(
            "Aggregated dataset is missing 'goes_imager_projection'; "
            "cannot determine source CRS."
        )

    sat_h = float(dataset["goes_imager_projection"].attrs["perspective_point_height"])
    src_crs = CRS.from_cf(dataset["goes_imager_projection"].attrs)

    da = dataset[var_name]
    if "time" in da.dims:
        da = da.squeeze("time", drop=True)

    da = da.assign_coords(x=da["x"].values * sat_h, y=da["y"].values * sat_h)
    da.rio.write_crs(src_crs.to_string(), inplace=True)
    da.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    # Pin NaN as the nodata sentinel both on the source and on the warp output
    # so destination cells outside the source extent (or filled by GDAL) come
    # back as NaN instead of 0 — without this, a future rioxarray/GDAL default
    # change could silently turn empty cells into opaque palette-floor colors.
    da.rio.write_nodata(np.nan, inplace=True)

    reprojected = da.rio.reproject(
        "EPSG:4326",
        resolution=resolution_deg,
        nodata=float("nan"),
    )
    return reprojected.rio.clip_box(
        minx=bounds["minx"],
        miny=bounds["miny"],
        maxx=bounds["maxx"],
        maxy=bounds["maxy"],
    )
