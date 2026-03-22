"""
Band 2 processor with downsampling for high-resolution visible imagery.

Band 2 (0.64 um Red Visible) has 500m resolution vs 2km for Band 13/9.
Full Disk grid is 21696x21696 (~470M pixels). CF-decoding this to float64
would consume ~3.7 GB, so this processor loads raw int16 and
downsamples 4x with coarsen().mean() BEFORE applying scale_factor/add_offset
on the small 5424x5424 result.

Processing differences from GoesProcessor:
  - Loads raw int16 (mask_and_scale=False), downsamples, then CF-decodes
  - Computes reflectance factor instead of brightness temperature
  - Dynamic vmax from 95th percentile on clipped region (SMN recommendation)
  - Uses grayscale VISIBLE_PALETTE
  - Uses 1 GDAL process for tile generation (less CPU pressure)
"""

from typing import cast, override
import gc
import logging
from pathlib import Path

import numpy as np
import xarray as xr

from processors.goes_processor import GoesProcessor

logger = logging.getLogger(__name__)


class Band2Processor(GoesProcessor):
    """
    Processor for Band 2 (0.64 um Red Visible) with downsampling.

    Overrides georeferencing to downsample 4x and brightness temperature
    computation to use reflectance factor (kappa0 * radiance) instead
    of the Planck function (which only applies to IR bands).
    """

    # 500m / 4 = 2km (matches Band 13/9 native resolution)
    DOWNSAMPLE_FACTOR = 4

    # Fewer GDAL processes for tile generation to reduce CPU pressure
    GDAL_PROCESSES = 1

    # Opaque black for nighttime — prevents transparent map bleed-through
    NAN_ALPHA: int = 255

    @override
    def _apply_georeferencing(  # pylint: disable=too-many-locals
        self, netcdf_path: Path
    ) -> xr.Dataset:
        """
        Apply GOES projection with 4x downsampling before CRS assignment.

        Reads raw int16 Rad data instead of CF-decoded float64
        coarsens to 5424x5424, then applies scale_factor and
        add_offset on the small array.
        """
        from pyproj import CRS  # pylint: disable=import-outside-toplevel
        import rioxarray  # noqa: F401  pylint: disable=import-outside-toplevel,unused-import

        # First open: extract metadata and coordinates (CF-decoded, all tiny)
        with xr.open_dataset(netcdf_path, engine="h5netcdf") as dataset:
            sat_h = dataset["goes_imager_projection"].perspective_point_height
            crs = CRS.from_cf(dataset["goes_imager_projection"].attrs)
            kappa0_val = float(dataset["kappa0"].values)

            x_scaled = dataset["x"].values * sat_h
            y_scaled = dataset["y"].values * sat_h

            # Capture Rad encoding params (no data loaded yet)
            rad_encoding = dataset["Rad"].encoding
            scale_factor = np.float32(rad_encoding.get("scale_factor", 1.0))
            add_offset = np.float32(rad_encoding.get("add_offset", 0.0))

        # Second open: read raw int16 Rad
        with xr.open_dataset(
            netcdf_path, engine="h5netcdf", mask_and_scale=False
        ) as raw_ds:
            raw_data = raw_ds["Rad"].values

        logger.info(
            "Loaded raw Band 2: shape=%s, dtype=%s, size=%.0f MB",
            raw_data.shape,
            raw_data.dtype,
            raw_data.nbytes / 1024**2,
        )

        # Build DataArray with raw int16 and scaled coordinates
        raw_da = xr.DataArray(
            raw_data,
            dims=["y", "x"],
            coords={"y": y_scaled, "x": x_scaled},
            name="Rad",
        )
        del raw_data
        gc.collect()

        # Downsample BEFORE CF decode - key memory optimization
        # coarsen().mean() on int16 uses float64 accumulator for precision
        original_shape = raw_da.shape
        coarsen_obj = raw_da.coarsen(
            x=self.DOWNSAMPLE_FACTOR,
            y=self.DOWNSAMPLE_FACTOR,
            boundary="trim",
        )
        coarsened: xr.DataArray = cast(xr.DataArray, coarsen_obj.mean())  # type: ignore[attr-defined]  # pylint: disable=no-member
        del raw_da
        gc.collect()

        logger.info(
            "Downsampled Band 2: %s -> %s (%dx reduction)",
            original_shape,
            coarsened.shape,
            self.DOWNSAMPLE_FACTOR,
        )

        # Apply CF decode (scale_factor + add_offset) on the small array
        rad_values = coarsened.values.astype(np.float32)
        rad_values *= scale_factor
        rad_values += add_offset

        rad_da = xr.DataArray(
            rad_values,
            dims=["y", "x"],
            coords={
                "y": coarsened.coords["y"].values,
                "x": coarsened.coords["x"].values,
            },
            name="Rad",
        )
        del coarsened, rad_values
        gc.collect()

        # Build output dataset with kappa0 for reflectance computation
        ds = rad_da.to_dataset(name="Rad")
        ds["kappa0"] = xr.DataArray(kappa0_val)
        ds.rio.write_crs(crs.to_string(), inplace=True)

        return ds

    @override
    def _compute_brightness_temperature(self, dataset: xr.Dataset) -> xr.DataArray:
        """
        Compute reflectance factor for visible Band 2 with gamma correction.

        Band 2 measures reflected sunlight, not thermal emission.
        The conversion is: reflectance = kappa0 * radiance

        Nighttime noise floor (< 0.005) is masked to NaN to prevent
        sensor noise from appearing as speckle in dark areas and from
        polluting the dynamic vmax percentile calculation.

        Square-root gamma (γ = 0.5) is then applied to produce
        perceptually uniform display values matching conventional
        visible satellite imagery rendering.
        """
        radiance = dataset["Rad"]
        kappa0 = float(dataset["kappa0"].values)

        logger.info("Computing reflectance factor (kappa0=%.6f)", kappa0)

        reflectance = radiance * kappa0
        del radiance
        gc.collect()

        # Mask nighttime noise floor (< 0.005 = sensor noise, not real signal)
        reflectance = xr.where(
            (reflectance >= 0.005) & (reflectance <= 1.2),
            reflectance,
            np.nan,
        )

        # Square-root gamma correction (γ = 0.5)
        # Converts linear physical reflectance to perceptually uniform display values.
        # Max physical value: sqrt(1.2) ≈ 1.095
        reflectance = reflectance**0.5

        reflectance.rio.write_crs(dataset.rio.crs, inplace=True)
        reflectance.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

        return reflectance

    @override
    def _get_vmax(self, reprojected_data: xr.DataArray, vmax_config: float) -> float:
        """Compute dynamic vmax from the 95th percentile of the clipped region.

        Per SMN recommendation for visible channel:
        The 95th percentile of gamma-corrected reflectance (sqrt domain) is used
        to dynamically adjust vmax so images don't appear too dark at sunrise/sunset.
        Computing on already-clipped data (Argentina bounds) is more representative
        than using the full satellite disk.

        Thresholds are in the gamma-corrected (sqrt) domain:
            perc = np.nanpercentile(data, 95)
            if isnan(perc) or perc < 0.22:  vmax = 1.0   (nighttime / very dark)
            elif perc > 0.84:               vmax = 0.95  (bright daylight)
            else:                           vmax = min(perc + 0.10, 1.0)
        """
        perc = float(np.nanpercentile(reprojected_data.values, 95))

        if np.isnan(perc) or perc < 0.22:
            dynamic_vmax = 1.0  # mostly dark / terminator — use full range
        elif perc > 0.84:
            dynamic_vmax = 0.95  # bright daylight — cap just above physical max
        else:
            dynamic_vmax = min(perc + 0.10, 1.0)  # transition — add headroom

        logger.info(
            "Dynamic vmax for Band 2: percentile_95=%.4f, vmax=%.4f",
            perc,
            dynamic_vmax,
        )
        return dynamic_vmax
