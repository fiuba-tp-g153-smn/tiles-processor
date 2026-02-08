"""
GOES-19 Georeferencing Service.

This service applies geographic coordinate transformations to raw GOES-19
satellite data, enabling proper spatial alignment for downstream processing.

GOES Geostationary Projection:
    GOES satellites use a geostationary projection where x,y coordinates
    are in radians from the satellite's sub-point. This service:
    1. Scales x,y by satellite height to get projection coordinates
    2. Extracts CRS from the goes_imager_projection variable
    3. Writes the CRS to the dataset for rioxarray compatibility
"""

import xarray as xr

from services.concurrent_runner import run_concurrently
from services.processing_steps import apply_goes_georeferencing


class SetupGOESGeorreferencingService:  # pylint: disable=too-few-public-methods
    """
    Applies georeferencing to GOES-19 satellite datasets.

    This service transforms raw GOES data (stored in radians) into properly
    georeferenced datasets with correct coordinate reference system (CRS)
    information for further geospatial processing.

    The transformation process:
        1. Opens NetCDF bytes using h5netcdf engine
        2. Reads satellite perspective height from goes_imager_projection
        3. Scales x,y coordinates: coord_meters = coord_radians * sat_height
        4. Extracts CRS from CF conventions in goes_imager_projection.attrs
        5. Writes CRS to dataset using rioxarray

    Args:
        goes_data: Dictionary mapping filenames to raw NetCDF bytes

    Returns:
        Dictionary mapping filenames to georeferenced xarray Datasets
        with proper CRS and scaled coordinates

    Note:
        Datasets are loaded into memory (dataset.load()) before returning
        to avoid issues with closed file handles in async processing.
    """

    def __init__(self, goes_data: dict[str, bytes], max_concurrency: int = 4):
        self._goes_data = goes_data
        self._max_concurrency = max_concurrency

    async def run(self) -> dict[str, xr.Dataset]:
        """Georeference all datasets with bounded concurrency."""
        return await run_concurrently(
            items=self._goes_data,
            worker_fn=lambda _name, content: apply_goes_georeferencing(content),
            max_concurrency=self._max_concurrency,
            task_name="Georeferencing",
        )
