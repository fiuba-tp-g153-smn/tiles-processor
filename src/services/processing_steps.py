"""Shared pure-computation functions for GOES satellite image processing.

These functions contain the core algorithms used by both the single-file
processor pipeline (GoesProcessor) and the batch service classes.
"""

import gc
import io
import logging
import math
import shutil
import subprocess
import uuid
from pathlib import Path

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


def apply_goes_georeferencing(source: Path | bytes) -> xr.Dataset:
    """Apply GOES geostationary projection to a NetCDF dataset.

    Scales x/y coordinates from radians to meters using the satellite
    perspective height, then writes the CRS extracted from CF conventions.

    Args:
        source: Path to a NetCDF file, or raw NetCDF bytes.

    Returns:
        In-memory xarray Dataset with georeferenced coordinates and CRS.
    """
    from pyproj import CRS  # pylint: disable=import-outside-toplevel

    import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

    file_input = io.BytesIO(source) if isinstance(source, bytes) else source

    with xr.open_dataset(file_input, engine="h5netcdf") as dataset:
        sat_h = dataset["goes_imager_projection"].perspective_point_height
        dataset = dataset.assign_coords(
            x=dataset["x"].values * sat_h, y=dataset["y"].values * sat_h
        )
        crs = CRS.from_cf(dataset["goes_imager_projection"].attrs)
        dataset.rio.write_crs(crs.to_string(), inplace=True)
        return dataset.load()


def compute_brightness_temperature(dataset: xr.Dataset) -> xr.DataArray:
    """Convert radiance to brightness temperature using the inverse Planck function.

    Uses band-specific Planck constants stored in the dataset:
        T = (fk2 / ln((fk1 / radiance) + 1) - bc1) / bc2

    Values outside 150K–350K are set to NaN (non-physical).
    """
    import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

    radiance = dataset["Rad"]

    fk1 = float(dataset["planck_fk1"].values)
    fk2 = float(dataset["planck_fk2"].values)
    bc1 = float(dataset["planck_bc1"].values)
    bc2 = float(dataset["planck_bc2"].values)

    radiance_safe = xr.where(radiance <= 0, 1e-10, radiance)
    del radiance
    gc.collect()

    brightness_temperature = (fk2 / np.log((fk1 / radiance_safe) + 1.0) - bc1) / bc2
    del radiance_safe
    gc.collect()

    brightness_temperature = xr.where(
        (brightness_temperature >= 150) & (brightness_temperature <= 350),
        brightness_temperature,
        np.nan,
    )

    brightness_temperature.rio.write_crs(dataset.rio.crs, inplace=True)
    brightness_temperature.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    return brightness_temperature


def normalize_and_colorize(
    array: xr.DataArray,
    vmin: float,
    vmax: float,
    color_palette: list[str],
    nan_alpha: int = 0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Normalize a DataArray to [0, 255] and map through a hex color palette.

    Args:
        array: Input data (e.g. brightness temperature).
        vmin: Value that maps to palette index 0.
        vmax: Value that maps to palette index 255.
        color_palette: List of 256 hex color strings (e.g. "#ff0000").
        nan_alpha: Alpha value for NaN/masked pixels (default 0 = transparent).

    Returns:
        (red, green, blue, alpha) as uint8 ndarrays.
        Alpha is 255 for valid pixels, nan_alpha for NaN.
    """
    arr = np.asarray(
        array.values if hasattr(array, "values") else array, dtype=np.float32
    )
    nan_mask = np.isnan(arr)
    alpha = np.where(nan_mask, nan_alpha, 255).astype(np.uint8)

    normalized = (arr - vmin) / (vmax - vmin)
    normalized = np.clip(normalized, 0, 1)
    normalized = np.nan_to_num(normalized, nan=0.0)
    del arr

    indices = (normalized * 255).astype(np.uint8)
    del normalized

    rgb_palette = np.zeros((256, 3), dtype=np.uint8)
    for i, hex_color in enumerate(color_palette):
        hex_color = hex_color.lstrip("#")
        rgb_palette[i, 0] = int(hex_color[0:2], 16)
        rgb_palette[i, 1] = int(hex_color[2:4], 16)
        rgb_palette[i, 2] = int(hex_color[4:6], 16)

    colored = rgb_palette[indices]
    del indices

    colored[nan_mask] = rgb_palette[0]
    del nan_mask
    gc.collect()

    return colored[..., 0], colored[..., 1], colored[..., 2], alpha


def threshold_colorize(
    array: xr.DataArray,
    thresholds: tuple[float, ...],
    colors: tuple[str, ...],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Map values to discrete colors based on thresholds.

    Values below ``thresholds[0]`` and NaN pixels are fully transparent.
    Values in ``[thresholds[i], thresholds[i+1])`` receive ``colors[i]``.
    Values ``>= thresholds[-1]`` receive ``colors[-1]``.

    Args:
        array: Input data array.
        thresholds: Ascending boundary values (length N).
        colors: Hex color strings (length N).

    Returns:
        (red, green, blue, alpha) as uint8 ndarrays.
    """
    arr = np.asarray(
        array.values if hasattr(array, "values") else array, dtype=np.float32
    )
    nan_mask = np.isnan(arr)
    arr = np.nan_to_num(arr, nan=0.0)

    bin_indexes = np.digitize(arr, thresholds, right=False)
    del arr

    transparent = nan_mask | (bin_indexes == 0)
    alpha = np.where(transparent, 0, 255).astype(np.uint8)
    del nan_mask, transparent

    bin_indexes = np.clip(bin_indexes, 0, len(colors)).astype(np.uint8)

    rgb_lut = np.zeros((len(colors) + 1, 3), dtype=np.uint8)
    for i, hex_color in enumerate(colors):
        h = hex_color.lstrip("#")
        rgb_lut[i + 1, 0] = int(h[0:2], 16)
        rgb_lut[i + 1, 1] = int(h[2:4], 16)
        rgb_lut[i + 1, 2] = int(h[4:6], 16)

    colored = rgb_lut[bin_indexes]
    del bin_indexes
    gc.collect()

    return colored[..., 0], colored[..., 1], colored[..., 2], alpha


# pylint: disable=too-many-arguments, too-many-positional-arguments
def build_rgba_data_array(
    r: np.ndarray,
    g: np.ndarray,
    b: np.ndarray,
    a: np.ndarray,
    coords_x: xr.DataArray,
    coords_y: xr.DataArray,
    product_name: str,
) -> xr.DataArray:
    """Stack R/G/B/A uint8 arrays into a georeferenced RGBA xarray DataArray.

    Args:
        r, g, b, a: uint8 ndarrays (2-D, same shape).
        coords_x: x coordinate values (longitude).
        coords_y: y coordinate values (latitude).
        product_name: Name for the DataArray.

    Returns:
        4-band xr.DataArray with EPSG:4326 CRS and spatial dims set.
    """
    import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

    rgb = xr.DataArray(
        np.stack([r, g, b, a]),
        dims=["band", "y", "x"],
        coords={"band": [1, 2, 3, 4], "x": coords_x, "y": coords_y},
        name=product_name,
    )
    rgb.rio.write_crs("EPSG:4326", inplace=True)
    rgb.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    return rgb


def save_as_cog(data: xr.DataArray, output_dir: Path, image_id: str) -> Path:
    """Save an already-reprojected DataArray as a Cloud Optimized GeoTIFF (COG).

    Expects data that is already in the target CRS and clipped to the desired bounds.
    Overviews are embedded automatically by the COG driver.

    Args:
        data: Georeferenced DataArray (float32, any CRS).
        output_dir: Directory where the COG file will be written.
        image_id: Base name for the output file (without extension).

    Returns:
        Path to the written COG file.
    """
    import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

    output_path = output_dir / f"{image_id}_cog.tif"
    tmp_path = output_dir / f"{uuid.uuid4()}.tif"

    try:
        data.rio.to_raster(tmp_path, driver="COG", compress="DEFLATE", predictor=3)
        tmp_path.rename(output_path)
        logger.info("COG written: %s", output_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    return output_path


_EARTH_CIRCUMFERENCE_M = 2 * math.pi * 6378137.0
_TILE_SIZE = 256


def _mercator_resolution_for_zoom(zoom: int) -> float:
    """Web Mercator pixel size in meters at XYZ zoom level `zoom`."""
    return _EARTH_CIRCUMFERENCE_M / (2**zoom * _TILE_SIZE)


def prewarp_to_mercator_grid(
    geotiff_path: Path,
    output_dir: Path,
    max_zoom: int,
) -> Path:
    """Reproject a GeoTIFF to EPSG:3857 at the pixel size of `max_zoom`, atomically.

    Why: ``gdal2tiles`` upsamples its input via per-tile RasterIO with nearest-neighbor
    rounding. For threshold-colorized RGBA inputs (alpha is exactly 0 or 255, no
    gradient), that rounding shifts cell boundaries by sub-cell amounts and can drop
    isolated opaque cells entirely. Pre-warping to the destination tile grid makes
    the per-tile read a 1:1 pickup, so cell boundaries are preserved exactly.
    """
    output_path = output_dir / f"{geotiff_path.stem}_3857.tif"
    tmp_path = output_dir / f"{uuid.uuid4()}.tif"
    res = _mercator_resolution_for_zoom(max_zoom)

    cmd = [
        "gdalwarp",
        "-t_srs",
        "EPSG:3857",
        "-r",
        "near",
        "-tr",
        str(res),
        str(res),
        "-dstalpha",
        "-co",
        "COMPRESS=DEFLATE",
        str(geotiff_path),
        str(tmp_path),
    ]

    try:
        logger.info("Pre-warping %s to EPSG:3857 at zoom %d (%.2f m/px)...",
                    geotiff_path.name, max_zoom, res)
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=600, check=False
        )

        if result.returncode != 0:
            logger.error("gdalwarp pre-warp failed: %s", result.stderr)
            raise RuntimeError(f"gdalwarp pre-warp failed for {geotiff_path.name}")

        tmp_path.rename(output_path)
        logger.info("Pre-warped GeoTIFF written: %s", output_path)
        return output_path

    except subprocess.TimeoutExpired as exc:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError("gdalwarp pre-warp timed out") from exc
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def run_gdal2tiles(
    geotiff_path: Path,
    output_dir: Path,
    zoom_levels: str = "3-7",
    processes: int = 2,
) -> Path:
    """Run gdal2tiles to generate XYZ web tiles with atomic directory rename.

    Args:
        geotiff_path: Input GeoTIFF file.
        output_dir: Parent directory for the tile output.
        zoom_levels: Zoom range string (e.g. "3-7").
        processes: Number of gdal2tiles worker processes.

    Returns:
        Path to the final tiles directory ({output_dir}/{stem}_tiles).
    """
    tiles_output_dir = output_dir / f"{geotiff_path.stem}_tiles"
    tmp_tiles_dir = output_dir / str(uuid.uuid4())
    tmp_tiles_dir.mkdir(parents=True, exist_ok=True)

    try:
        cmd = [
            "gdal2tiles.py",
            "-z",
            zoom_levels,
            "-w",
            "none",
            "--xyz",
            "--tiledriver=WEBP",
            "--webp-lossless",
            "--resampling=near",
            f"--processes={processes}",
            str(geotiff_path),
            str(tmp_tiles_dir),
        ]

        logger.info("Generating tiles for %s...", geotiff_path.name)
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=600, check=False
        )

        if result.returncode != 0:
            logger.error("gdal2tiles failed: %s", result.stderr)
            raise RuntimeError(f"gdal2tiles failed for {geotiff_path.name}")

        if tiles_output_dir.exists():
            shutil.rmtree(tiles_output_dir)
        tmp_tiles_dir.rename(tiles_output_dir)

        logger.info("Tiles generated: %s", tiles_output_dir)
        return tiles_output_dir

    except subprocess.TimeoutExpired as exc:
        if tmp_tiles_dir.exists():
            shutil.rmtree(tmp_tiles_dir)
        raise RuntimeError("gdal2tiles timed out") from exc
    except Exception:
        if tmp_tiles_dir.exists():
            shutil.rmtree(tmp_tiles_dir)
        raise


def _compute_tile_range(
    bounds: dict, zoom: int, padding: int = 0
) -> tuple[int, int, int, int]:
    """Return inclusive (x_min, x_max, y_min, y_max) XYZ tile indices for bounds at zoom.

    Y increases southward: maxy (north) → y_min, miny (south) → y_max.
    padding expands the range by that many tiles in every direction (clamped to 0–n-1).
    """
    n = 2**zoom
    x_min = int(math.floor((bounds["minx"] + 180) / 360 * n))
    x_max = int(math.floor((bounds["maxx"] + 180) / 360 * n))

    def _lat_to_y(lat_deg: float) -> int:
        lat_r = math.radians(lat_deg)
        merc = math.log(math.tan(lat_r) + 1 / math.cos(lat_r)) / math.pi
        return int(math.floor((1 - merc) / 2 * n))

    y_min = _lat_to_y(bounds["maxy"])
    y_max = _lat_to_y(bounds["miny"])
    return (
        max(0, min(x_min - padding, n - 1)),
        max(0, min(x_max + padding, n - 1)),
        max(0, min(y_min - padding, n - 1)),
        max(0, min(y_max + padding, n - 1)),
    )


def _make_transparent_webp() -> bytes:
    """Return bytes for a 256×256 fully transparent WEBP tile."""
    from PIL import Image  # pylint: disable=import-outside-toplevel

    img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="WEBP", lossless=True)
    return buf.getvalue()


def fill_missing_tiles(  # pylint: disable=too-many-locals
    tiles_dir: Path, bounds: dict, zoom_levels: str = "3-7"
) -> int:
    """Create transparent WEBP tiles for every XYZ position within bounds that is missing.

    Iterates all expected (z, x, y) tiles derived from the geographic bounds and writes
    a 256×256 transparent WEBP for any that gdal2tiles did not produce.

    Args:
        tiles_dir: Tile directory with structure {z}/{x}/{y}.webp.
        bounds: Geographic bounds dict with keys: minx, miny, maxx, maxy (degrees).
        zoom_levels: Zoom range string (e.g. "3-7").

    Returns:
        Number of transparent tiles created.
    """
    parts = zoom_levels.split("-")
    z_min, z_max = int(parts[0]), int(parts[-1])
    transparent = _make_transparent_webp()
    created = 0

    for zoom in range(z_min, z_max + 1):
        x_min, x_max, y_min, y_max = _compute_tile_range(bounds, zoom, padding=1)
        for x in range(x_min, x_max + 1):
            x_dir = tiles_dir / str(zoom) / str(x)
            x_dir.mkdir(parents=True, exist_ok=True)
            for y in range(y_min, y_max + 1):
                tile_path = x_dir / f"{y}.webp"
                if not tile_path.exists():
                    tile_path.write_bytes(transparent)
                    created += 1

    if created:
        logger.info("Created %d transparent filler tiles in %s", created, tiles_dir)
    return created
