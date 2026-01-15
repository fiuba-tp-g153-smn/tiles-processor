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

    def __init__(self, goes_data: Dict[str, bytes]):
        self._goes_data = goes_data

    async def run(self) -> Dict[str, xr.Dataset]:
        tasks = []
        file_names = []

        for file_name, content in self._goes_data.items():
            file_names.append(file_name)
            tasks.append(asyncio.to_thread(self._apply_georeferencing, content))

        georeferenced_contents = await asyncio.gather(*tasks)

        georeferenced_data = {}
        for file_name, content in zip(file_names, georeferenced_contents):
            georeferenced_data[file_name] = content

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
