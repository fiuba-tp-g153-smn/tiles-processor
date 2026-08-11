"""Base class for processors whose products are contour overlays."""

import xarray as xr

from config import Config
from processors.base_processor import ImageProcessor
from services.contouring import smooth_and_contour


class ContourProcessor(ImageProcessor):
    """An `ImageProcessor` that emits smoothed isoline overlays.

    Holds the two knobs every contour product shares — the Gaussian sigma and
    the geometry simplification tolerance — so subclasses contour a field in one
    call instead of repeating the same four arguments at every site.
    """

    def __init__(self, config: Config):
        super().__init__(config)
        self._sigma = config.GFS_SMOOTHING_SIGMA
        self._tolerance = config.GFS_ISOLINE_SIMPLIFY_TOLERANCE

    def _contour(
        self, field: xr.DataArray, step: float, value_property: str
    ) -> list[dict]:
        """Smooth then contour `field` at every multiple of `step`."""
        return smooth_and_contour(
            field,
            sigma=self._sigma,
            step=step,
            simplify_tolerance=self._tolerance,
            value_property=value_property,
        )
