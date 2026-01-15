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

import asyncio
import io
from pyproj import CRS
from typing import Dict
import xarray as xr

# Note: Ensure you have rioxarray installed to use the rio accessor


class SetupGOESGeorreferencingService:
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

    def __init__(self, goes_data: Dict[str, bytes], max_concurrency: int = 4):
        self._goes_data = goes_data
        self._max_concurrency = max_concurrency

    async def run(self) -> Dict[str, xr.Dataset]:
        """
        Async Concurrency Pattern: Semaphore + to_thread + gather.

        This pattern enables controlled parallelism for CPU-bound georeferencing tasks:
        1. Semaphore limits concurrent executions (default: 4) to prevent memory exhaustion.
        2. asyncio.to_thread runs blocking netCDF/numpy operations in a separate thread.
        3. asyncio.gather coordinates all tasks and collects results.

        This approach ensures the event loop remains responsive while processing heavy
        satellite data files, without overwhelming system resources.
        """
        import logging

        logger = logging.getLogger(__name__)
        tasks = []
        file_names = []

        # Semaphore limits concurrent georeferencing to prevent memory exhaustion
        semaphore = asyncio.Semaphore(self._max_concurrency)

        async def bounded_georeferencing(content: bytes):
            # Acquire semaphore permit (blocks if max_concurrency reached)
            async with semaphore:
                # Offload CPU-bound georeferencing to thread pool
                return await asyncio.to_thread(self._apply_georeferencing, content)

        for file_name, content in self._goes_data.items():
            file_names.append(file_name)
            tasks.append(bounded_georeferencing(content))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        georeferenced_data = {}
        failed = []

        for file_name, result in zip(file_names, results):
            if isinstance(result, Exception):
                failed.append((file_name, result))
            else:
                georeferenced_data[file_name] = result

        if failed:
            for name, err in failed:
                logger.error(f"Georeferencing failed for {name}: {err}")
            raise RuntimeError(
                f"Georeferencing failed for {len(failed)}/{len(tasks)} files"
            )

        return georeferenced_data

    def _apply_georeferencing(self, content: bytes) -> xr.Dataset:
        with xr.open_dataset(io.BytesIO(content), engine="h5netcdf") as dataset:
            sat_h = dataset["goes_imager_projection"].perspective_point_height
            dataset = dataset.assign_coords(
                x=dataset["x"].values * sat_h, y=dataset["y"].values * sat_h
            )
            crs = CRS.from_cf(dataset["goes_imager_projection"].attrs)
            dataset.rio.write_crs(crs.to_string(), inplace=True)
            # Load the dataset into memory before returning
            return dataset.load()
