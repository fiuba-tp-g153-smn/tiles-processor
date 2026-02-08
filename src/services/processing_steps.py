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
