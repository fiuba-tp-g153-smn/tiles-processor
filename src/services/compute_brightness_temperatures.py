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

import xarray as xr

from services.concurrent_runner import run_concurrently
from services.processing_steps import compute_brightness_temperature


class ComputeBrightnessTemperaturesService:  # pylint: disable=too-few-public-methods
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

    def __init__(
        self, georreferenced_datasets: dict[str, xr.Dataset], max_concurrency: int = 4
    ):
        self._georreferenced_datasets = georreferenced_datasets
        self._max_concurrency = max_concurrency

    async def run(self) -> dict[str, xr.DataArray]:
        """Compute brightness temperatures with bounded concurrency."""
        return await run_concurrently(
            items=self._georreferenced_datasets,
            worker_fn=lambda _name, ds: compute_brightness_temperature(ds),
            max_concurrency=self._max_concurrency,
            task_name="Brightness temp computation",
        )
