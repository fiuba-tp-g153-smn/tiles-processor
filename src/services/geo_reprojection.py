"""Windowed reprojection onto a destination grid restricted to the bounds.

The naive path — ``rio.reproject("EPSG:4326")`` over the whole source, then
``rio.clip_box`` to the project bounds — warps the full GOES disk and throws
away ~85% of the pixels it just computed. These helpers derive the *same*
destination grid from the source metadata alone (no pixels touched), cut the
window that ``clip_box`` would have kept, and warp only that window.

Two properties make the result bit-identical to the naive path rather than
merely equivalent:

  * The destination grid is derived exactly as rioxarray derives it
    (:func:`rasterio.warp.calculate_default_transform` over the source bounds),
    and the window replicates ``clip_box``/``isel_window`` rounding — cells
    that *touch* the box are kept, offsets floored, ends ceiled.
  * ``tolerance=0`` disables GDAL's per-chunk polynomial approximation of the
    coordinate transformer. Its default error threshold of 0.125 destination
    pixels is enough, under nearest resampling, to pick the neighbouring source
    pixel — and the fit depends on the warped extent, so a window and a full
    disk disagree. Measured on real GOES C13 and GLM data: without it 95-99% of
    cells match; with it, 100.0000%.
"""

import math
from typing import Any

import xarray as xr

from exceptions import UnprocessableInputError

DEFAULT_CRS = "EPSG:4326"

# Disables GDAL's approximate transformer. See the module docstring: the
# approximation is extent-dependent, so warping a window with it on does not
# reproduce the full-disk result.
_EXACT_TRANSFORMER_TOLERANCE = 0


def compute_target_window(
    source: xr.DataArray,
    bounds: dict[str, float],
    dst_crs: str = DEFAULT_CRS,
) -> tuple[Any, tuple[int, int]]:
    """Return the ``(transform, shape)`` of the destination grid cut to ``bounds``.

    Reads only the source's CRS, shape and bounds — never its pixels.

    Args:
        source: Georeferenced source array (``.rio`` accessor required).
        bounds: Clip box in ``dst_crs`` degrees, keys ``minx``/``miny``/
            ``maxx``/``maxy``.
        dst_crs: Target CRS.

    Returns:
        The window's affine transform and its ``(height, width)``.

    Raises:
        UnprocessableInputError: The bounds do not intersect the source extent.
    """
    import rasterio.warp  # pylint: disable=import-outside-toplevel
    import rasterio.windows  # pylint: disable=import-outside-toplevel

    src_height, src_width = source.rio.shape
    dst_transform, dst_width, dst_height = rasterio.warp.calculate_default_transform(
        source.rio.crs, dst_crs, src_width, src_height, *source.rio.bounds()
    )
    window = rasterio.windows.from_bounds(
        left=bounds["minx"],
        bottom=bounds["miny"],
        right=bounds["maxx"],
        top=bounds["maxy"],
        transform=dst_transform,
    )
    rows, cols = _clamp_to_grid(window, dst_height, dst_width)
    shape = (rows.stop - rows.start, cols.stop - cols.start)
    if shape[0] < 1 or shape[1] < 1:
        raise UnprocessableInputError(
            f"Bounds {bounds} do not intersect the source extent "
            f"{source.rio.bounds()} in {dst_crs}"
        )

    window_transform = rasterio.windows.transform(
        rasterio.windows.Window.from_slices(
            rows=rows, cols=cols, width=dst_width, height=dst_height
        ),
        dst_transform,
    )
    return window_transform, shape


def reproject_to_bounds(
    source: xr.DataArray,
    bounds: dict[str, float],
    dst_crs: str = DEFAULT_CRS,
    **reproject_kwargs: Any,
) -> xr.DataArray:
    """Reproject ``source`` straight onto the grid window covering ``bounds``.

    Equivalent to ``source.rio.reproject(dst_crs)`` followed by
    ``rio.clip_box(**bounds)``, without materializing the discarded pixels.

    Args:
        source: Georeferenced source array.
        bounds: Clip box in ``dst_crs`` degrees.
        dst_crs: Target CRS.
        **reproject_kwargs: Forwarded to ``rio.reproject`` (e.g. ``nodata``).

    Returns:
        The reprojected array, already restricted to ``bounds``.
    """
    transform, shape = compute_target_window(source, bounds, dst_crs)
    return source.rio.reproject(
        dst_crs,
        transform=transform,
        shape=shape,
        tolerance=_EXACT_TRANSFORMER_TOLERANCE,
        **reproject_kwargs,
    )


def _clamp_to_grid(window: Any, height: int, width: int) -> tuple[slice, slice]:
    """Round a float window outwards and clamp it to the grid, as ``isel_window`` does.

    Keeps every destination cell the box touches: offsets floored, ends ceiled,
    then clipped to ``[0, height]`` / ``[0, width]``.
    """
    (row_start, row_stop), (col_start, col_stop) = window.toranges()
    return (
        slice(_floor(row_start, height), _ceil(row_stop, height)),
        slice(_floor(col_start, width), _ceil(col_stop, width)),
    )


def _floor(offset: float, limit: int) -> int:
    return min(max(math.floor(offset), 0), limit)


def _ceil(offset: float, limit: int) -> int:
    return min(max(math.ceil(offset), 0), limit)
