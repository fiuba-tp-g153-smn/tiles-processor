"""INTA radar processor - reads Rainbow5 volumes, reuses the SINARAME pipeline."""

from logging import getLogger
from pathlib import Path

from exceptions import UnprocessableInputError
from models.radar_config import get_inta_product_config
from models.radar_scan import ScanIdentity
from models.rainbow_header import (
    HEADER_WINDOW_BYTES,
    RainbowHeaderError,
    parse_inta_filename,
    parse_rainbow_header,
)
from processors.radar_processor import RadarProcessor

logger = getLogger(__name__)


def _patch_rainbow_slice_defaults() -> None:
    """Carry ``rangestep``/``anglestep`` from the first sweep to the rest.

    Rainbow5 lets a volume declare a scan parameter once and leave it implicit
    on the sweeps that repeat it, and the INTA sites differ in whether they do:
    Paraná writes ``<rangestep>`` on every slice, Pergamino writes it twice for
    a 12-sweep volume (the pargroup and the first slice). PyART's reader expects
    it per slice and raises on the first sweep that omits it, so Pergamino never
    decoded.

    Filling from the first sweep is safe because these are properties of the
    scan strategy, constant across a volume — the pargroup declares them once
    for exactly that reason. Anything a file does state explicitly is left
    alone; only absent keys are filled.

    Patching PyART's module global is the only seam available: the omission is
    handled inside ``read_rainbow`` before the reader hands anything back. The
    wrapper marks itself so repeated calls cannot stack wrappers on top of each
    other, which is what a naive re-patch per read would do.
    """
    # pylint: disable=import-outside-toplevel
    import pyart.aux_io.rainbow_wrl as rainbow_wrl

    if getattr(rainbow_wrl.read_rainbow, "_inta_slice_defaults", False):
        return

    original = rainbow_wrl.read_rainbow

    def read_rainbow_with_slice_defaults(*args, **kwargs):
        volume = original(*args, **kwargs)
        slices = volume.get("volume", {}).get("scan", {}).get("slice", [])
        if isinstance(slices, list) and slices:
            first = slices[0]
            for sweep in slices[1:]:
                for key in ("rangestep", "anglestep"):
                    if key not in sweep and key in first:
                        sweep[key] = first[key]
        return volume

    read_rainbow_with_slice_defaults._inta_slice_defaults = True
    rainbow_wrl.read_rainbow = read_rainbow_with_slice_defaults


class IntaRadarProcessor(RadarProcessor):
    """
    Processor for the INTA radars (Anguil, Paraná, Pergamino).

    The INTA feed differs from SINARAME only in how a scan is identified and
    read: Rainbow5 volumes instead of ODIM-HDF5. Both carry the same physical
    moments on a comparable geometry — 240 km of range, and 0.5°/0.9°/1.3° as
    the first three sweeps, matching ``SWEEPS`` exactly — so everything from the
    polar-to-cartesian mapping onwards is inherited unchanged, palettes
    included. Only the S3 prefix differs, and that rides on the product config.
    """

    @property
    def zoom_spec(self) -> str:
        """INTA has its own zoom range in settings.json."""
        return self.config.INTA_ZOOM.spec

    def _resolve_scan(self, source_uri: str) -> ScanIdentity:
        """Resolve the scan from the Rainbow5 header rather than the filename.

        The station cannot come from the name: production files are prefixed
        (``PAR2026…dBZ.vol``) but the SMN's own samples are not
        (``2026…dBZ.vol``). The header carries it either way, so it is the only
        thing that works for both.
        """
        path = Path(source_uri)
        parsed = parse_inta_filename(path.name)
        product_config = get_inta_product_config(parsed["variable"])

        return ScanIdentity(
            radar_id=self._read_radar_id(path, fallback=parsed["radar_id"]),
            timestamp=parsed["timestamp"],
            product_config=product_config,
            variable_id=product_config.variable,
        )

    def _read_radar(self, scan_path: Path):
        """Read a Rainbow5 volume using PyART."""
        import pyart  # pylint: disable=import-outside-toplevel

        _patch_rainbow_slice_defaults()

        logger.info("[RADAR-INTA] Reading %s", scan_path.name)

        try:
            radar = pyart.aux_io.read_rainbow_wrl(str(scan_path))
        except (ValueError, KeyError, OSError) as exc:
            # Rainbow5 is a moving target — the SMN warns the format changes
            # between releases and PyART only guarantees a handful of versions.
            # A volume this reader cannot decode is bad input, not a bug worth
            # retrying through the DLQ.
            raise UnprocessableInputError(
                f"Unreadable Rainbow5 volume {scan_path.name}: {exc}"
            ) from exc

        logger.info(
            "[RADAR-INTA] Fields: %s, Sweeps: %d, Center: (%.4f, %.4f)",
            list(radar.fields.keys()),
            radar.nsweeps,
            radar.latitude["data"][0],
            radar.longitude["data"][0],
        )
        return radar

    @staticmethod
    def _read_radar_id(path: Path, fallback: str) -> str:
        """Read the station id from the file's XML header.

        Falls back to the filename prefix when the header cannot be parsed, and
        fails only when neither yields an id — at that point the scan has no
        station to publish under.
        """
        try:
            with open(path, "rb") as handle:
                return parse_rainbow_header(handle.read(HEADER_WINDOW_BYTES)).radar_id
        except (RainbowHeaderError, OSError) as exc:
            if fallback:
                logger.warning(
                    "[RADAR-INTA] Unreadable header in %s (%s); falling back to "
                    "the filename prefix %s",
                    path.name,
                    exc,
                    fallback,
                )
                return fallback
            raise UnprocessableInputError(
                f"Cannot identify the radar for {path.name}: no readable header "
                f"and no station prefix in the name ({exc})"
            ) from exc
