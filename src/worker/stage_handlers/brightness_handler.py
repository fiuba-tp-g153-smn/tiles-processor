"""Brightness temperature stage handler - converts radiance to temperature."""

import gc
import logging
import pickle
from pathlib import Path

import numpy as np
import xarray as xr

from config import Config
from models.work_unit import WorkUnit
from worker.stage_handlers.base_handler import BaseStageHandler

logger = logging.getLogger(__name__)


class BrightnessTemperatureHandler(BaseStageHandler):
    """
    Handler for the BRIGHTNESS_TEMPERATURE stage.

    Converts satellite radiance measurements to brightness temperatures
    using the inverse Planck function.

    Input: work_unit.paths.georef_data
    Output: work_unit.paths.temp_data (path to pickled xarray.DataArray)
    """

    def __init__(self, config: Config):
        super().__init__(config)

    async def handle(self, work_unit: WorkUnit) -> WorkUnit:
        """Compute brightness temperature from radiance data."""
        logger.info(f"[BRIGHTNESS_TEMP] Starting for {work_unit.image_id}")

        if not work_unit.paths.georef_data:
            raise ValueError(
                "georef_data path is required for BRIGHTNESS_TEMPERATURE stage"
            )

        # Prepare output directory
        temp_dir = self._ensure_dir(self._get_band_dir(work_unit) / "temp")

        # Load georeferenced dataset
        georef_path = Path(work_unit.paths.georef_data)
        if not georef_path.exists():
            raise FileNotFoundError(f"Georef file not found: {georef_path}")

        with open(georef_path, "rb") as f:
            dataset = pickle.load(f)

        # Compute brightness temperature (synchronous, wrapped for consistency)
        import asyncio

        bt_data = await asyncio.to_thread(self._compute_brightness_temperature, dataset)

        # Clean up dataset from memory
        del dataset
        gc.collect()

        # Save brightness temperature as pickle
        output_path = temp_dir / f"{work_unit.image_id}.temp.pkl"
        with open(output_path, "wb") as f:
            pickle.dump(bt_data, f)

        logger.info(f"[BRIGHTNESS_TEMP] Saved to {output_path}")

        # Update work unit
        work_unit.paths.temp_data = str(output_path)

        return work_unit

    def _compute_brightness_temperature(self, dataset: xr.Dataset) -> xr.DataArray:
        """
        Convert radiance to brightness temperature using Planck function.

        Formula: T = (fk2 / ln((fk1 / L) + 1) - bc1) / bc2

        Where:
            L = measured radiance
            fk1, fk2 = Planck function constants
            bc1, bc2 = band correction factors
        """
        radiance = dataset["Rad"]

        # Get Planck constants from the dataset
        fk1 = float(dataset["planck_fk1"].values)
        fk2 = float(dataset["planck_fk2"].values)
        bc1 = float(dataset["planck_bc1"].values)
        bc2 = float(dataset["planck_bc2"].values)

        # Avoid non-physical radiance values
        radiance_safe = xr.where(radiance <= 0, 1e-10, radiance)
        del radiance
        gc.collect()

        # Calculate brightness temperature
        brightness_temperature = (fk2 / np.log((fk1 / radiance_safe) + 1.0) - bc1) / bc2
        del radiance_safe
        gc.collect()

        # Filter values outside physical range (150K to 350K)
        brightness_temperature = xr.where(
            (brightness_temperature >= 150) & (brightness_temperature <= 350),
            brightness_temperature,
            np.nan,
        )

        # Preserve CRS and spatial dims
        brightness_temperature.rio.write_crs(dataset.rio.crs, inplace=True)
        brightness_temperature.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

        return brightness_temperature
