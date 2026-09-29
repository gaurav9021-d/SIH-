"""Reconcile overlapping satellite views of one fire into single observations.

Without this, one fire seen by VIIRS on Suomi NPP and by MODIS on Terra minutes
apart counts as two detections, inflating both `detection_count` and the density
that `min_samples` is tested against. The bias is worst exactly where satellite
swaths overlap, which is not where fires are.

Only detections from *different* product/satellite pairs are reconciled. Adjacent
pixels from the same sensor on the same overpass are genuine spatial structure of
one extended fire, so they stay separate observations and let the clustering stage
group them on its own terms.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime

from clustering.core.geo import distance_m
from clustering.core.models import Detection, Observation, observation_key_for
from clustering.core.params import ClusterParams


class CausalOrderError(ValueError):
    """Input arrived out of acquisition order, breaking the causal contract."""


@dataclass(slots=True)
class _OpenObservation:
    anchor: Detection
    key: str
    detections: list[Detection] = field(default_factory=list)
    sources: set[tuple[str, str]] = field(default_factory=set)

    def accepts(self, detection: Detection, params: ClusterParams) -> bool:
        if detection.source in self.sources:
            return False
        gap_s = (detection.as_of - self.anchor.as_of).total_seconds()
        if gap_s > params.reconcile_temporal_seconds:
            return False
        return (
            distance_m(detection.lat, detection.lon, self.anchor.lat, self.anchor.lon)
            <= params.reconcile_spatial_m
        )

    def absorb(self, detection: Detection) -> None:
        self.detections.append(detection)
        self.sources.add(detection.source)

    def finish(self) -> Observation:
        frp_values = [d.frp for d in self.detections]
        return Observation(
            observation_key=self.key,
            lat=self.anchor.lat,
            lon=self.anchor.lon,
            as_of=self.anchor.as_of,
            frp=sum(frp_values) / len(frp_values),
            day_night=self.anchor.day_night,
            detection_count=len(self.detections),
            products=tuple(sorted({d.product for d in self.detections})),
            satellites=tuple(sorted({d.satellite for d in self.detections})),
            detection_refs=tuple(
                sorted((d.detection_id, d.as_of) for d in self.detections)
            ),
        )


def reconcile(
    detections: Iterable[Detection], params: ClusterParams
) -> Iterator[Observation]:
    """Yield observations in non-decreasing anchor time.

    An observation is emitted only once its temporal tolerance has elapsed, so it
    is final before the clustering step ever sees it. That confines provisional
    state to one bounded stage.
    """
    if not params.reconcile_enabled:
        yield from _passthrough(detections)
        return

    open_observations: list[_OpenObservation] = []
    last_seen: datetime | None = None

    for detection in detections:
        if last_seen is not None and detection.as_of < last_seen:
            raise CausalOrderError(
                f"detection {detection.detection_id} at {detection.as_of.isoformat()} "
                f"arrived after {last_seen.isoformat()}"
            )
        last_seen = detection.as_of

        # Anchors are created in non-decreasing time, so the open list is sorted
        # and flushing a prefix keeps emission monotone.
        flush_before = detection.as_of
        while open_observations:
            candidate = open_observations[0]
            elapsed = (flush_before - candidate.anchor.as_of).total_seconds()
            if elapsed <= params.reconcile_temporal_seconds:
                break
            yield open_observations.pop(0).finish()

        best: _OpenObservation | None = None
        best_distance = float("inf")
        for candidate in open_observations:
            if not candidate.accepts(detection, params):
                continue
            gap = distance_m(
                detection.lat,
                detection.lon,
                candidate.anchor.lat,
                candidate.anchor.lon,
            )
            if gap < best_distance or (gap == best_distance and best is not None and candidate.key < best.key):
                best, best_distance = candidate, gap

        if best is not None:
            best.absorb(detection)
            continue

        fresh = _OpenObservation(
            anchor=detection, key=observation_key_for(detection)
        )
        fresh.absorb(detection)
        open_observations.append(fresh)

    for remaining in open_observations:
        yield remaining.finish()


def _passthrough(detections: Iterable[Detection]) -> Iterator[Observation]:
    last_seen: datetime | None = None
    for detection in detections:
        if last_seen is not None and detection.as_of < last_seen:
            raise CausalOrderError(
                f"detection {detection.detection_id} arrived out of order"
            )
        last_seen = detection.as_of
        yield Observation(
            observation_key=observation_key_for(detection),
            lat=detection.lat,
            lon=detection.lon,
            as_of=detection.as_of,
            frp=detection.frp,
            day_night=detection.day_night,
            detection_count=1,
            products=(detection.product,),
            satellites=(detection.satellite,),
            detection_refs=((detection.detection_id, detection.as_of),),
        )
