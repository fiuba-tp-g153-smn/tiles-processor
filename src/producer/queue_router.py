"""Routing policy that decides which work queue a work unit belongs to."""

from dataclasses import dataclass

from models.work_unit import WorkUnit


@dataclass(frozen=True, slots=True)
class QueueRouter:
    """Decide the target work queue for a work unit.

    Lightweight units (all radar, plus a configured set of WRF products) go to
    the light queue so a larger pool of cheap workers can drain them in parallel
    with the heavy GOES/GLM/ECMWF queue. Everything else goes to the normal
    queue. Membership is driven by config (settings.json), so products can be
    re-pointed without a code change.
    """

    normal_queue: str
    light_queue: str
    all_radar_light: bool
    light_wrf_products: frozenset[str]

    def route(self, work_unit: WorkUnit) -> str:
        """Return the queue name this work unit should be published to."""
        if self._is_light(work_unit):
            return self.light_queue
        return self.normal_queue

    def _is_light(self, work_unit: WorkUnit) -> bool:
        """True when the unit is a lightweight radar/WRF task."""
        data_source_id = work_unit.data_source_id
        if self.all_radar_light and data_source_id.startswith("radar_"):
            return True
        if data_source_id.startswith("wrf_"):
            product_id = data_source_id.removeprefix("wrf_")
            return product_id in self.light_wrf_products
        return False
