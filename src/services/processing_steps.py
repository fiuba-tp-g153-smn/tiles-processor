"""Shared pure-computation functions for GOES satellite image processing.

These functions contain the core algorithms used by both the single-file
processor pipeline (GoesProcessor) and the batch service classes.
"""

import gc
import io
import logging
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


def compute_flash_extent_density(  # pylint: disable=too-many-locals
    glm_file_paths: list[Path], grid_bounds: dict, grid_resolution: float = 0.02
) -> xr.DataArray:
    """
    Compute Flash Extent Density from GLM-L2-LCFA files.

    Bins flash locations from multiple GLM files into a regular lat/lon grid
    to create a density map showing flash counts per cell over a time window.

    Args:
        glm_file_paths: List of GLM-L2-LCFA NetCDF files (typically ~30 files for 10-min window)
        grid_bounds: Geographic bounds dict with keys: minx, maxx, miny, maxy (degrees)
        grid_resolution: Grid cell size in degrees (default 0.02° ≈ 2km at equator)

    Returns:
        xr.DataArray with flash counts per grid cell, georeferenced to EPSG:4326.
        Cells with zero flashes are set to NaN for transparency in visualization.

    Memory Profile:
        - Each GLM file: ~2-5 MB
        - 30 files: ~150 MB raw data
        - Output grid (e.g., 3000×1500): ~36 MB float64
        - Peak memory: ~200 MB (well within limits)
    """
    import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

    # 1. Read all flash locations from all files
    all_flash_lats = []
    all_flash_lons = []

    for file_path in glm_file_paths:
        with xr.open_dataset(file_path, engine="h5netcdf") as ds:
            # Extract flash lat/lon (GLM stores as scaled int16, xarray auto-decodes)
            flash_lat = ds["flash_lat"].values
            flash_lon = ds["flash_lon"].values
            all_flash_lats.append(flash_lat)
            all_flash_lons.append(flash_lon)

    # Flatten all arrays into single list
    all_flash_lats = np.concatenate(all_flash_lats)
    all_flash_lons = np.concatenate(all_flash_lons)

    flash_count = len(all_flash_lats)
    logger.info("Loaded %d flashes from %d GLM files", flash_count, len(glm_file_paths))

    # 2. Create grid bins
    lon_bins = np.arange(
        grid_bounds["minx"], grid_bounds["maxx"] + grid_resolution, grid_resolution
    )
    lat_bins = np.arange(
        grid_bounds["miny"], grid_bounds["maxy"] + grid_resolution, grid_resolution
    )

    # 3. Bin flashes into grid cells (2D histogram)
    # Note: histogram2d expects (x, y) order but returns (y, x) shaped array
    flash_counts, _, _ = np.histogram2d(
        all_flash_lats, all_flash_lons, bins=[lat_bins, lon_bins]
    )

    # Free memory
    del all_flash_lats, all_flash_lons
    gc.collect()

    # 4. Create xarray DataArray with proper coordinates
    lon_centers = (lon_bins[:-1] + lon_bins[1:]) / 2
    lat_centers = (lat_bins[:-1] + lat_bins[1:]) / 2

    fed_array = xr.DataArray(
        flash_counts,
        dims=["y", "x"],
        coords={"x": lon_centers, "y": lat_centers},
        name="Flash_Extent_Density",
    )

    # 5. Set CRS and spatial dims
    fed_array.rio.write_crs("EPSG:4326", inplace=True)
    fed_array.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

    # 6. Replace zeros with NaN for transparency (avoid clutter on map)
    fed_array = xr.where(fed_array > 0, fed_array, np.nan)

    if flash_count == 0:
        logger.info(
            "Computed FED grid: %d x %d cells, no flashes — transparent tiles will be generated",
            len(lat_centers),
            len(lon_centers),
        )
    else:
        logger.info(
            "Computed FED grid: %d x %d cells, max flash count: %.0f",
            len(lat_centers),
            len(lon_centers),
            float(np.nanmax(fed_array.values)),
        )

    return fed_array


def normalize_and_colorize(
    array: xr.DataArray,
    vmin: float,
    vmax: float,
    color_palette: list[str],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Normalize a DataArray to [0, 255] and map through a hex color palette.

    Args:
        array: Input data (e.g. brightness temperature).
        vmin: Value that maps to palette index 0.
        vmax: Value that maps to palette index 255.
        color_palette: List of 256 hex color strings (e.g. "#ff0000").

    Returns:
        (red, green, blue, alpha) as uint8 ndarrays.
        Alpha is 255 for valid pixels, 0 for NaN.
    """
    arr = np.asarray(
        array.values if hasattr(array, "values") else array, dtype=np.float32
    )
    nan_mask = np.isnan(arr)
    alpha = np.where(nan_mask, 0, 255).astype(np.uint8)

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
