"""Routing policy that decides which work queue a work unit belongs to."""

from dataclasses import dataclass

from models.work_unit import WorkUnit


@dataclass(frozen=True, slots=True)
class QueueRouter:
    """Decide the target work queue for a work unit.

    Lightweight units go to one of two light queues so a larger pool of cheap
    workers can drain them in parallel with the heavy GOES/GLM/ECMWF queue, and
    so radar and WRF never head-of-line block each other: all radar to the radar
    light queue, a configured set of WRF products to the WRF light queue.
    Everything else goes to the normal queue. Membership is driven by config
    (settings.json), so products can be re-pointed without a code change.
    """

    normal_queue: str
    radar_light_queue: str
    wrf_light_queue: str
    all_radar_light: bool
    light_wrf_products: frozenset[str]

    def route(self, work_unit: WorkUnit) -> str:
        """Return the queue name this work unit should be published to."""
        data_source_id = work_unit.data_source_id
        if self.all_radar_light and data_source_id.startswith("radar_"):
            return self.radar_light_queue
        if data_source_id.startswith("wrf_"):
            product_id = data_source_id.removeprefix("wrf_")
            if product_id in self.light_wrf_products:
                return self.wrf_light_queue
        return self.normal_queue
