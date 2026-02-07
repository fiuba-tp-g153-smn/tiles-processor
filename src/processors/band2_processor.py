"""
Band 2 processor with downsampling for high-resolution visible imagery.

Band 2 (0.64 µm Red Visible) has 500m resolution vs 2km for Band 13/9,
resulting in ~16x more pixels and ~7x larger files (~200MB vs ~30MB).

This processor downsamples the data by 4x BEFORE reprojection to:
  1. Reduce memory from ~2GB to ~125MB during reprojection
  2. Reduce CPU time from ~30s to ~3s for GeoTIFF generation
  3. Produce output at comparable resolution to Band 13/9 tiles

Processing differences from GoesProcessor:
  - Downsamples 4x before reprojection (500m → 2km effective resolution)
  - Computes reflectance factor instead of brightness temperature
  - Uses grayscale VISIBLE_PALETTE
  - Uses 1 GDAL process for tile generation (less CPU pressure)
"""

import gc
import logging
from pathlib import Path

import numpy as np
import xarray as xr

from processors.goes_processor import GoesProcessor

logger = logging.getLogger(__name__)


class Band2Processor(GoesProcessor):
    """
    Processor for Band 2 (0.64 µm Red Visible) with downsampling.

    Overrides georeferencing to downsample 4x and brightness temperature
    computation to use reflectance factor (kappa0 * radiance) instead
    of the Planck function (which only applies to IR bands).
    """

    # 500m / 4 = 2km (matches Band 13/9 native resolution)
    DOWNSAMPLE_FACTOR = 4

    # Fewer GDAL processes for tile generation to reduce CPU pressure
    GDAL_PROCESSES = 1

    def _apply_georeferencing(self, netcdf_path: Path) -> xr.Dataset:
        """
        Apply GOES projection with 4x downsampling before CRS assignment.

        Extracts raw data, scales coordinates, coarsens the radiance array
        by DOWNSAMPLE_FACTOR, then assigns the CRS. This reduces the data
        from ~10000x10000 to ~2500x2500 pixels BEFORE the expensive
        reprojection step in _generate_geotiff.
        """
        from pyproj import CRS
        import rioxarray  # noqa: F401 - registers .rio accessor

        with xr.open_dataset(netcdf_path, engine="h5netcdf") as dataset:
            # Extract metadata before any transformation
            sat_h = dataset["goes_imager_projection"].perspective_point_height
            crs = CRS.from_cf(dataset["goes_imager_projection"].attrs)
            kappa0_val = float(dataset["kappa0"].values)

            # Scale coordinates from radians to meters
            x_scaled = dataset["x"].values * sat_h
            y_scaled = dataset["y"].values * sat_h

            # Extract radiance into memory
            rad_data = dataset["Rad"].values

        # Build DataArray with scaled coordinates (outside context manager)
        rad_da = xr.DataArray(
            rad_data,
            dims=["y", "x"],
            coords={"y": y_scaled, "x": x_scaled},
            name="Rad",
        )
        del rad_data
        gc.collect()

        # Downsample BEFORE reprojection — this is the key optimization
        original_shape = rad_da.shape
        rad_da = rad_da.coarsen(
            x=self.DOWNSAMPLE_FACTOR,
            y=self.DOWNSAMPLE_FACTOR,
            boundary="trim",
        ).mean()
        logger.info(
            f"Downsampled Band 2: {original_shape} → {rad_da.shape} "
            f"({self.DOWNSAMPLE_FACTOR}x reduction)"
        )

        # Build output dataset with kappa0 for reflectance computation
        ds = rad_da.to_dataset(name="Rad")
        ds["kappa0"] = xr.DataArray(kappa0_val)
        ds.rio.write_crs(crs.to_string(), inplace=True)

        return ds

    def _compute_brightness_temperature(self, dataset: xr.Dataset) -> xr.DataArray:
        """
        Compute reflectance factor for visible Band 2.

        Band 2 measures reflected sunlight, not thermal emission.
        The conversion is simply: reflectance = kappa0 * radiance

        Values outside [0.0, 1.2] are filtered as non-physical
        (sensor artifacts, calibration errors).
        """
        radiance = dataset["Rad"]
        kappa0 = float(dataset["kappa0"].values)

        logger.info(f"Computing reflectance factor (kappa0={kappa0:.6f})")

        reflectance = radiance * kappa0
        del radiance
        gc.collect()

        # Filter non-physical values
        reflectance = xr.where(
            (reflectance >= 0.0) & (reflectance <= 1.2),
            reflectance,
            np.nan,
        )

        reflectance.rio.write_crs(dataset.rio.crs, inplace=True)
        reflectance.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

        return reflectance

    def _generate_geotiff(
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
        """
        Generate GeoTIFF with dynamic vmax based on percentile 95.

        Per SMN recommendation for visible channel:
        The 95th percentile of reflectance is used to dynamically adjust
        vmax so images don't appear too dark at sunrise/sunset.

        Formula:
            perc = np.nanpercentile(data, 95)
            if perc < 0.05:  vmax = 1.0   (nighttime / very dark)
            elif perc > 0.7: vmax = 0.9   (bright daylight)
            else:            vmax = perc + 0.2
        """
        # Compute dynamic vmax from the reflectance data
        valid_data = bt_data.values
        perc = float(np.nanpercentile(valid_data, 95))

        if perc < 0.05:
            dynamic_vmax = 1.0
        elif perc > 0.7:
            dynamic_vmax = 0.9
        else:
            dynamic_vmax = perc + 0.2

        logger.info(
            f"Dynamic vmax for Band 2: percentile_95={perc:.4f}, vmax={dynamic_vmax:.4f}"
        )

        return super()._generate_geotiff(
            bt_data,
            output_dir,
            image_id,
            bounds,
            vmin,
            dynamic_vmax,
            product_name,
            color_palette,
        )
