"""
Brightness Temperature Computation Service.

This service converts satellite radiance measurements to brightness temperatures
using the inverse Planck function with band-specific calibration constants.

Physics Background:
    Satellites measure radiance (energy per unit area per steradian per wavelength).
    Brightness temperature is the temperature a blackbody would need to emit
    that same radiance. The conversion uses the Planck function:

    T_brightness = (fk2 / ln((fk1 / L) + 1) - bc1) / bc2

    Where:
        L = measured radiance
        fk1, fk2 = Planck function constants (band-specific)
        bc1, bc2 = band correction factors

    These constants are stored in each NetCDF file and vary by spectral band.

Physical Validity:
    Brightness temperatures outside 150K-350K are filtered as non-physical.
    This removes sensor artifacts, space views, and calibration errors.
"""

import asyncio
import gc
import numpy as np
import xarray as xr

# Note: Ensure you have rioxarray installed to use the rio accessor


class ComputeBrightnessTemperaturesService:
    """
    Converts satellite radiance to brightness temperature.

    This service applies the inverse Planck function to convert raw radiance
    measurements from GOES-19 into brightness temperatures (Kelvin).

    The computation uses band-specific Planck constants stored in each
    NetCDF file:
        - planck_fk1: First Planck function constant
        - planck_fk2: Second Planck function constant
        - planck_bc1: Band correction offset
        - planck_bc2: Band correction scale

    Formula:
        T = (fk2 / ln((fk1 / radiance) + 1) - bc1) / bc2

    Filtering:
        - Radiance <= 0 is replaced with 1e-10 to avoid math errors
        - Temperatures outside 150K-350K are set to NaN (non-physical)

    Args:
        georreferenced_datasets: Dict mapping filenames to georeferenced Datasets

    Returns:
        Dict mapping filenames to DataArrays containing brightness temperature
        in Kelvin, with CRS preserved from input datasets

    Memory Management:
        Uses explicit gc.collect() and del statements to manage memory
        when processing large satellite imagery arrays.
    """

    def __init__(self, georreferenced_datasets: dict[str, xr.Dataset]):
        self._georreferenced_datasets = georreferenced_datasets

    async def run(self) -> dict[str, xr.DataArray]:
        import logging

        logger = logging.getLogger(__name__)
        tasks = []
        file_names = []

        for file_name, dataset in self._georreferenced_datasets.items():
            file_names.append(file_name)
            tasks.append(
                asyncio.to_thread(self._compute_brightness_temperatures, dataset)
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        brightness_temperature_data = {}
        failed = []

        for file_name, result in zip(file_names, results):
            if isinstance(result, Exception):
                failed.append((file_name, result))
            else:
                brightness_temperature_data[file_name] = result

        if failed:
            for name, err in failed:
                logger.error(f"Brightness temp computation failed for {name}: {err}")
            raise RuntimeError(
                f"Brightness temp computation failed for {len(failed)}/{len(tasks)} files"
            )

        return brightness_temperature_data

    def _compute_brightness_temperatures(self, dataset: xr.Dataset) -> xr.DataArray:
        radiance = dataset["Rad"]

        # Planck constants specific to the channel, obtained from the NetCDF file
        fk1 = float(dataset["planck_fk1"].values)
        fk2 = float(dataset["planck_fk2"].values)
        bc1 = float(dataset["planck_bc1"].values)
        bc2 = float(dataset["planck_bc2"].values)

        # Avoid non-physical radiance values
        radiance_safe = xr.where(radiance <= 0, 1e-10, radiance)
        del radiance
        gc.collect()

        # Calculate brightness temperature using the Planck function
        brightness_temperature = (fk2 / np.log((fk1 / radiance_safe) + 1.0) - bc1) / bc2
        del radiance_safe
        gc.collect()

        # Filter values outside the expected physical range (150K to 350K)
        brightness_temperature = xr.where(
            (brightness_temperature >= 150) & (brightness_temperature <= 350),
            brightness_temperature,
            np.nan,
        )

        brightness_temperature.rio.write_crs(dataset.rio.crs, inplace=True)
        brightness_temperature.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
        return brightness_temperature
