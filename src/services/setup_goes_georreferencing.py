import asyncio
import io
from pyproj import CRS
from typing import Dict
import xarray as xr

# Note: Ensure you have rioxarray installed to use the rio accessor


class SetupGOESGeorreferencingService:
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
