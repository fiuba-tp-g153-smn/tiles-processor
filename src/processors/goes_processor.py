"""
Shared GOES processor logic.

This class implements the full pipeline for GOES satellite imagery:
Download -> Georeference -> Brightness Temp -> GeoTIFF -> Tiles -> Upload
"""

import asyncio
import gc
import logging
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Tuple

import numpy as np
import xarray as xr

from config import Config
from models.work_unit import WorkUnit
from processors.base_processor import ImageProcessor, ShutdownRequested
from clients.s3_client import S3Client
from services.generate_geotiff_files import GenerateGeoTIFFFilesService

logger = logging.getLogger(__name__)


class GoesProcessor(ImageProcessor):
    """
    Processor for GOES satellite imagery (Band 13, Band 9, etc.).

    Implements the Strategy pattern for the full processing pipeline.
    """

    # gdal2tiles settings
    GDAL_PROCESSES = 2
    ZOOM_LEVELS = "3-7"

    # Reprojection resolution in degrees (None = auto-compute from source)
    REPROJECT_RESOLUTION = None

    def __init__(self, config: Config):
        super().__init__(config)
        self._minio_client = S3Client.create_with_credentials(
            bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
            endpoint=config.S3_TILES_DATA_ENDPOINT,
            access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
            secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
            secure=config.S3_TILES_DATA_SECURE,
        )

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """Execute the full processing pipeline."""
        logger.info(
            "[%s] Starting processing for %s",
            work_unit.processor_id.upper(),
            work_unit.image_id,
        )

        # Verify input
        netcdf_path = Path(downloaded_file_path)
        if not netcdf_path.exists():
            raise FileNotFoundError(f"NetCDF file not found: {netcdf_path}")

        # Setup directories
        band_dir = self._get_band_dir(work_unit)
        geotiff_dir = self._ensure_dir(band_dir / "geotiff")
        tiles_dir = self._ensure_dir(band_dir / "tiles")

        # variables to hold data in memory
        dataset = None
        bt_data = None

        try:
            dataset, bt_data = await self._run_science_pipeline(
                netcdf_path, dataset, bt_data
            )
            await self._generate_and_upload(bt_data, geotiff_dir, tiles_dir, work_unit)
            bt_data = None
        except ShutdownRequested:
            logger.info(
                "Shutdown requested, aborting processing for %s",
                work_unit.image_id,
            )
            raise
        except Exception as e:
            logger.error("Processing failed for %s: %s", work_unit.image_id, e)
            raise
        finally:
            self._cleanup_directory(geotiff_dir)
            self._cleanup_directory(tiles_dir)
            gc.collect()

    async def _run_science_pipeline(self, netcdf_path, dataset, bt_data):
        """Run georeferencing and brightness temperature computation."""
        # 1. Georeference
        self._check_shutdown()
        logger.info("Step 1: Georeferencing")
        dataset = await asyncio.to_thread(self._apply_georeferencing, netcdf_path)

        # 2. Brightness Temperature
        self._check_shutdown()
        logger.info("Step 2: Brightness Temperature")
        bt_data = await asyncio.to_thread(self._compute_brightness_temperature, dataset)

        del dataset
        gc.collect()
        return None, bt_data

    async def _generate_and_upload(self, bt_data, geotiff_dir, tiles_dir, work_unit):
        """Generate GeoTIFF, tiles, and upload to S3."""
        band_config = work_unit.band_config

        # Determine palette
        if band_config.palette_name == "WATER_VAPOR_PALETTE":
            color_palette = GenerateGeoTIFFFilesService.WATER_VAPOR_PALETTE
        elif band_config.palette_name == "VISIBLE_PALETTE":
            color_palette = GenerateGeoTIFFFilesService.VISIBLE_PALETTE
        else:
            color_palette = GenerateGeoTIFFFilesService.CLOUD_TOPS_PALETTE

        # 3. GeoTIFF Generation
        self._check_shutdown()
        logger.info("Step 3: GeoTIFF Generation")
        geotiff_path = await asyncio.to_thread(
            self._generate_geotiff,
            bt_data,
            geotiff_dir,
            work_unit.image_id,
            work_unit.bounds,
            band_config.vmin,
            band_config.vmax,
            band_config.product_name,
            color_palette,
        )

        del bt_data
        gc.collect()

        # 4. Tile Generation
        self._check_shutdown()
        logger.info("Step 4: Tile Generation")
        tiles_output_dir = await asyncio.to_thread(
            self._generate_tiles, geotiff_path, tiles_dir
        )

        # 5. Upload to S3
        self._check_shutdown()
        logger.info("Step 5: Upload to S3")
        tileset_name = f"{geotiff_path.stem}_tiles"
        s3_prefix = f"{band_config.s3_prefix}/{tileset_name}"

        await self._minio_client.ensure_bucket_exists()
        await self._minio_client.upload_directory(tiles_output_dir, s3_prefix)

        logger.info("Processing complete: %s", s3_prefix)

        # 6. Retention Policy Cleanup
        self._check_shutdown()
        logger.info("Step 6: Enforcing Retention Policy")
        await self._enforce_retention_policy(band_config.s3_prefix)

        # Cleanup intermediate files
        self._cleanup_file(geotiff_path)
        self._cleanup_directory(tiles_output_dir)
        gc.collect()

        return geotiff_path

    async def _enforce_retention_policy(self, s3_prefix: str) -> None:
        """
        Enforce retention policy: keep only the latest N tilesets.

        This is designed to be safe for concurrent execution by multiple workers:
        - Uses defensive listing and sorting
        - Handles deletion failures gracefully
        - Does not fail the overall processing if cleanup fails

        Args:
            s3_prefix: The S3 prefix for the band (e.g., "band_13/tiles")
        """
        retention_count = 26

        try:
            prefixes = await self._minio_client.list_prefixes(
                f"{s3_prefix}/", delimiter="/"
            )

            tileset_prefixes = sorted(
                [p for p in prefixes if p.rstrip("/").endswith("_tiles")]
            )

            total_count = len(tileset_prefixes)

            if total_count <= retention_count:
                logger.debug(
                    "Retention policy check: %d <= %d, no action needed.",
                    total_count,
                    retention_count,
                )
                return

            to_delete = tileset_prefixes[:-retention_count]

            max_delete_per_pass = 10
            if len(to_delete) > max_delete_per_pass:
                logger.warning(
                    "Limiting deletion to %d tilesets (wanted to delete %d)",
                    max_delete_per_pass,
                    len(to_delete),
                )
                to_delete = to_delete[:max_delete_per_pass]

            logger.info(
                "Retention policy: Deleting %d old tilesets "
                "(total: %d, keeping: %d)",
                len(to_delete),
                total_count,
                retention_count,
            )

            deleted_count = 0
            for prefix in to_delete:
                try:
                    await self._minio_client.delete_prefix(prefix)
                    deleted_count += 1
                    logger.info("Deleted old tileset: %s", prefix)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.debug("Could not delete tileset %s: %s", prefix, e)

            if deleted_count > 0:
                logger.info(
                    "Retention policy: Successfully deleted %d tilesets",
                    deleted_count,
                )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("Error enforcing retention policy (non-fatal): %s", e)

    def _apply_georeferencing(self, netcdf_path: Path) -> xr.Dataset:
        """Apply GOES satellite projection transformation."""
        # Lazy imports to reduce idle memory footprint
        from pyproj import CRS  # pylint: disable=import-outside-toplevel
        import rioxarray  # noqa: F401  pylint: disable=import-outside-toplevel,unused-import

        with xr.open_dataset(netcdf_path, engine="h5netcdf") as dataset:
            # Get satellite perspective height
            sat_h = dataset["goes_imager_projection"].perspective_point_height

            # Scale coordinates from radians to meters
            dataset = dataset.assign_coords(
                x=dataset["x"].values * sat_h, y=dataset["y"].values * sat_h
            )

            # Extract CRS from CF conventions
            crs = CRS.from_cf(dataset["goes_imager_projection"].attrs)
            dataset.rio.write_crs(crs.to_string(), inplace=True)

            return dataset.load()

    def _compute_brightness_temperature(self, dataset: xr.Dataset) -> xr.DataArray:
        """Convert radiance to brightness temperature using Planck function."""
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

        # Filter values
        brightness_temperature = xr.where(
            (brightness_temperature >= 150) & (brightness_temperature <= 350),
            brightness_temperature,
            np.nan,
        )

        brightness_temperature.rio.write_crs(dataset.rio.crs, inplace=True)
        brightness_temperature.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

        return brightness_temperature

    def _generate_geotiff(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        self,
        bt_data: xr.DataArray,
        output_dir: Path,
        image_id: str,
        bounds: dict,
        vmin: float,
        vmax: float,
        product_name: str,
        color_palette: list,
    ) -> Path:
        """Generate a colorized RGBA GeoTIFF."""
        logger.info("Generating GeoTIFF for %s", image_id)
        logger.debug("Bounds: %s", bounds)
        logger.debug("Input data shape: %s", bt_data.shape)

        # Clean attributes
        if "grid_mapping" in bt_data.attrs:
            del bt_data.attrs["grid_mapping"]

        # Reproject
        logger.debug("Reprojecting to EPSG:4326...")
        bt_reproj = bt_data.rio.reproject(
            "EPSG:4326", resolution=self.REPROJECT_RESOLUTION
        )
        bt_reproj.rio.write_nodata(np.nan, inplace=True)
        logger.debug("Reprojected shape: %s", bt_reproj.shape)

        # Clip to bounds
        logger.debug(
            "Clipping to bounds: minx=%s, miny=%s, maxx=%s, maxy=%s",
            bounds["minx"],
            bounds["miny"],
            bounds["maxx"],
            bounds["maxy"],
        )
        bt_clipped = bt_reproj.rio.clip_box(
            minx=bounds["minx"],
            miny=bounds["miny"],
            maxx=bounds["maxx"],
            maxy=bounds["maxy"],
        )
        logger.info(
            "Clipped data shape: %s (y=%d, x=%d)",
            bt_clipped.shape,
            bt_clipped.shape[0],
            bt_clipped.shape[1],
        )

        # Warn if clipped data is very small
        if bt_clipped.shape[0] < 100 or bt_clipped.shape[1] < 100:
            logger.warning(
                "Clipped data is small (%s), this may result in missing zoom levels",
                bt_clipped.shape,
            )

        del bt_reproj
        gc.collect()

        # Normalize and Colorize
        coords_x = bt_clipped["x"]
        coords_y = bt_clipped["y"]
        r, g, b, a = self._normalize_with_palette(bt_clipped, vmin, vmax, color_palette)
        del bt_clipped
        gc.collect()

        # Create RGBA
        rgb = xr.DataArray(
            np.stack([r, g, b, a]),
            dims=["band", "y", "x"],
            coords={"band": [1, 2, 3, 4], "x": coords_x, "y": coords_y},
            name=product_name,
        )
        del r, g, b, a
        gc.collect()

        rgb.rio.write_crs("EPSG:4326", inplace=True)
        rgb.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

        # Save
        stem = Path(image_id).stem
        output_path = output_dir / f"{stem}.tif"
        tmp_path = output_dir / f"{uuid.uuid4()}.tif"

        try:
            rgb.rio.to_raster(tmp_path)
            tmp_path.rename(output_path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        del rgb
        gc.collect()
        return output_path

    def _normalize_with_palette(
        self,
        array: xr.DataArray,
        vmin: float,
        vmax: float,
        color_palette: list,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Normalize array and apply color palette."""
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

        return colored[..., 0], colored[..., 1], colored[..., 2], alpha

    def _generate_tiles(self, geotiff_path: Path, output_base_dir: Path) -> Path:
        """Generate XYZ tiles using gdal2tiles."""
        tileset_name = f"{geotiff_path.stem}_tiles"
        tiles_output_dir = output_base_dir / tileset_name
        tmp_tiles_dir = output_base_dir / str(uuid.uuid4())
        tmp_tiles_dir.mkdir(parents=True, exist_ok=True)

        try:
            cmd = [
                "gdal2tiles.py",
                "-z",
                self.ZOOM_LEVELS,
                "-w",
                "none",  # No web viewer needed, just tiles
                "--xyz",  # Use XYZ tile scheme (OSM/Slippy map standard) instead of TMS
                "--tiledriver=WEBP",
                f"--processes={self.GDAL_PROCESSES}",
                str(geotiff_path),
                str(tmp_tiles_dir),
            ]

            logger.info("Running gdal2tiles: %s", " ".join(cmd))
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600, check=False
            )

            if result.returncode != 0:
                logger.error("gdal2tiles failed with return code %d", result.returncode)
                logger.error("stderr: %s", result.stderr)
                logger.error("stdout: %s", result.stdout)
                raise RuntimeError(f"gdal2tiles failed: {result.stderr}")

            # Log any warnings from stdout/stderr
            if result.stderr:
                logger.warning("gdal2tiles stderr: %s", result.stderr)
            if result.stdout:
                logger.debug("gdal2tiles stdout: %s", result.stdout)

            # Validate that expected zoom levels were generated
            self._validate_tiles(tmp_tiles_dir)

            if tiles_output_dir.exists():
                shutil.rmtree(tiles_output_dir)
            tmp_tiles_dir.rename(tiles_output_dir)

            logger.info("Tiles generated successfully: %s", tiles_output_dir)
            return tiles_output_dir

        except subprocess.TimeoutExpired as exc:
            logger.error("gdal2tiles timed out after 600 seconds")
            if tmp_tiles_dir.exists():
                shutil.rmtree(tmp_tiles_dir)
            raise RuntimeError("gdal2tiles timed out") from exc
        except subprocess.CalledProcessError as e:
            logger.error("gdal2tiles failed: %s", e.stderr)
            if tmp_tiles_dir.exists():
                shutil.rmtree(tmp_tiles_dir)
            raise RuntimeError(f"gdal2tiles failed: {e.stderr}") from e
        except Exception:
            if tmp_tiles_dir.exists():
                shutil.rmtree(tmp_tiles_dir)
            raise

    def _validate_tiles(self, tiles_dir: Path) -> None:
        """Validate that the expected zoom levels were generated."""
        # Parse zoom range from ZOOM_LEVELS (e.g., "3-7")
        zoom_parts = self.ZOOM_LEVELS.split("-")
        min_zoom = int(zoom_parts[0])
        max_zoom = int(zoom_parts[1]) if len(zoom_parts) > 1 else min_zoom

        missing_zooms = []
        for zoom in range(min_zoom, max_zoom + 1):
            zoom_dir = tiles_dir / str(zoom)
            if not zoom_dir.exists():
                missing_zooms.append(zoom)
            else:
                # Count tiles at this zoom level
                tile_count = sum(1 for _ in zoom_dir.rglob("*.webp"))
                if tile_count == 0:
                    missing_zooms.append(zoom)
                else:
                    logger.debug("Zoom %d: %d tiles generated", zoom, tile_count)

        if missing_zooms:
            logger.warning(
                "Missing or empty zoom levels: %s. Expected range: %d-%d",
                missing_zooms,
                min_zoom,
                max_zoom,
            )
            # List what was actually generated
            generated_zooms = [
                d.name for d in tiles_dir.iterdir() if d.is_dir() and d.name.isdigit()
            ]
            logger.warning(
                "Actually generated zoom directories: %s",
                sorted(generated_zooms, key=int),
            )
