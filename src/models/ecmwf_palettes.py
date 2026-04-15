"""Color palettes for ECMWF model products."""

PRECIPITATION_THRESHOLDS: tuple[float, ...] = (0.5, 2.0, 4.0, 10.0, 25.0, 50.0, 100.0)

PRECIPITATION_COLORS: tuple[str, ...] = (
    "#00FFFF",  # 0.5 - 2 mm
    "#007FFF",  # 2 - 4 mm
    "#0000FF",  # 4 - 10 mm
    "#D900FF",  # 10 - 25 mm
    "#FF00FF",  # 25 - 50 mm
    "#FF7F00",  # 50 - 100 mm
    "#FF0000",  # > 100 mm
)
