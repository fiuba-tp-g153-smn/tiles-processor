import asyncio
import gc
import numpy as np
import xarray as xr

# Note: Ensure you have rioxarray installed to use the rio accessor


class ComputeBrightnessTemperaturesService:
    def __init__(self, georreferenced_datasets: dict[str, xr.Dataset]):
        self._georreferenced_datasets = georreferenced_datasets

    async def run(self) -> dict[str, xr.DataArray]:
        tasks = []
        file_names = []

        for file_name, dataset in self._georreferenced_datasets.items():
            file_names.append(file_name)
            tasks.append(
                asyncio.to_thread(self._compute_brightness_temperatures, dataset)
            )

        brightness_temperature_datasets = await asyncio.gather(*tasks)

        brightness_temperature_data = {}
        for file_name, data_array in zip(file_names, brightness_temperature_datasets):
            brightness_temperature_data[file_name] = data_array

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
