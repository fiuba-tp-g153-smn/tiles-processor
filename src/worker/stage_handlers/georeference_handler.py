"""Georeference stage handler - applies coordinate transformations."""

import logging
import pickle
from pathlib import Path

import xarray as xr
from pyproj import CRS

from config import Config
from models.work_unit import WorkUnit
from worker.stage_handlers.base_handler import BaseStageHandler

logger = logging.getLogger(__name__)


class GeoreferenceHandler(BaseStageHandler):
    """
    Handler for the GEOREFERENCE stage.

    Applies GOES satellite projection and coordinate transformation
    to the downloaded NetCDF file.

    Input: work_unit.paths.local_netcdf
    Output: work_unit.paths.georef_data (path to pickled xarray.Dataset)
    """

    def __init__(self, config: Config):
        super().__init__(config)

    async def handle(self, work_unit: WorkUnit) -> WorkUnit:
        """Apply georeferencing to the satellite data."""
        logger.info(f"[GEOREFERENCE] Starting for {work_unit.image_id}")

        if not work_unit.paths.local_netcdf:
            raise ValueError("local_netcdf path is required for GEOREFERENCE stage")

        # Prepare output directory
        georef_dir = self._ensure_dir(self._get_band_dir(work_unit) / "georef")

        # Read the NetCDF file
        netcdf_path = Path(work_unit.paths.local_netcdf)
        if not netcdf_path.exists():
            raise FileNotFoundError(f"NetCDF file not found: {netcdf_path}")

        # Apply georeferencing (synchronous operation, but wrapped for consistency)
        import asyncio

        georef_dataset = await asyncio.to_thread(
            self._apply_georeferencing, netcdf_path
        )

        # Save georeferenced dataset as pickle
        output_path = georef_dir / f"{work_unit.image_id}.georef.pkl"
        with open(output_path, "wb") as f:
            pickle.dump(georef_dataset, f)

        logger.info(f"[GEOREFERENCE] Saved to {output_path}")

        # Update work unit
        work_unit.paths.georef_data = str(output_path)

        return work_unit

    def _apply_georeferencing(self, netcdf_path: Path) -> xr.Dataset:
        """
        Apply GOES satellite projection transformation.

        This transforms x,y coordinates from radians to meters and
        writes the CRS to the dataset.
        """
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

            # Load into memory before returning (file handle will be closed)
            return dataset.load()
