"""Wind-speed palettes for the GFS upper-level products."""

WIND_500_THRESHOLDS: tuple[float, ...] = (
    80.0,
    100.0,
    120.0,
    140.0,
    160.0,
    180.0,
    200.0,
)

WIND_500_COLORS: tuple[str, ...] = (
    "#82D2FF",  # 43 — 80-100 kt
    "#4BB4F0",  # 45 — 100-120
    "#1E8CC8",  # 48 — 120-140
    "#C0B4FF",  # 52 — 140-160
    "#8070EB",  # 54 — 160-180
    "#483CC8",  # 56 — 180-200
    "#2800A0",  # 59 — > 200
)

WIND_250_THRESHOLDS: tuple[float, ...] = (
    80.0,
    90.0,
    100.0,
    110.0,
    120.0,
    130.0,
    140.0,
    150.0,
    160.0,
    170.0,
    180.0,
    190.0,
    200.0,
)

WIND_250_COLORS: tuple[str, ...] = (
    "#FFFAAA",  # 21 — 80-90 kt
    "#FFE878",  # 22 — 90-100
    "#FFC03C",  # 23 — 100-110
    "#FFA000",  # 24 — 110-120
    "#FF6000",  # 25 — 120-130
    "#FF3200",  # 26 — 130-140
    "#C0B4FF",  # 52 — 140-150
    "#A08CFF",  # 53 — 150-160
    "#8070EB",  # 54 — 160-170
    "#7060DC",  # 55 — 170-180
    "#483CC8",  # 56 — 180-190
    "#3C28B4",  # 57 — 190-200
    "#2D1EA5",  # 58 — > 200
)

_BY_PRODUCT: dict[str, tuple[tuple[float, ...], tuple[str, ...]]] = {
    "500": (WIND_500_THRESHOLDS, WIND_500_COLORS),
    "250": (WIND_250_THRESHOLDS, WIND_250_COLORS),
}


def wind_palette(product_id: str) -> tuple[tuple[float, ...], tuple[str, ...]]:
    """Thresholds (kt) and colours for one upper-level product."""
    if product_id not in _BY_PRODUCT:
        raise ValueError(
            f"No wind palette for GFS product '{product_id}'. "
            f"Valid: {list(_BY_PRODUCT)}"
        )
    return _BY_PRODUCT[product_id]
