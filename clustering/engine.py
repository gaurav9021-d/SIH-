"""Database-backed clustering driver.

Kept separate from `dbscan.py` so that module stays free of I/O: the step function
is the thing causality tests depend on, and it must remain testable without a
database.

The pipeline is reader -> reconcile -> step -> writer, one causal pass. Clusters
are flushed as they close, so memory tracks the active set rather than the range.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from ingestion.core.db import Database
from ingestion.core.logging import get_logger, log_context, log_event, redact
from ingestion.core.settings import Aoi

from clustering.core.models import Cluster, Detection, Observation, StepEffect
from clustering.core.params import ClusterParams
from clustering.core.stats import Distribution, SizeHistogram
from clustering.dbscan import finalize, new_state, step
from clustering.reader import stream_detections
from clustering.spatial_dedup import reconcile
from clustering.writer import ClusterWriter, new_run_id

LOGGER = get_logger("clustering.engine")


@dataclass(slots=True)
class RunSummary:
    """Everything a run reports, in one comparable structure."""

    run_id: str
    aoi: str
    window_start: datetime
    window_end: datetime
    evaluation_time: datetime
    settled_horizon: datetime
    params: dict[str, Any]
    detections_read: int = 0
    observations: int = 0
    clusters_open: int = 0
    clusters_closed: int = 0
    noise_count: int = 0
    pending_count: int = 0
    max_extent_m: float = 0.0
    duration_s: float = 0.0
    persisted: bool = False
    sizes: list[int] = field(default_factory=list)
    extents: list[float] = field(default_factory=list)

    @property
    def cluster_count(self) -> int:
        return self.clusters_open + self.clusters_closed

    @property
    def noise_fraction(self) -> float:
        if self.observations == 0:
            return 0.0
        return self.noise_count / self.observations

    def size_distribution(self) -> Distribution:
        return Distribution.of([float(s) for s in self.sizes])

    def extent_distribution(self) -> Distribution:
        return Distribution.of(self.extents)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "aoi": self.aoi,
            "window_start": self.window_start.isoformat(),
            "window_end_exclusive": self.window_end.isoformat(),
            "evaluation_time": self.evaluation_time.isoformat(),
            "settled_horizon": self.settled_horizon.isoformat(),
            "params": self.params,
            "detections_read": self.detections_read,
            "observations": self.observations,
            "cluster_count": self.cluster_count,
            "clusters_open": self.clusters_open,
            "clusters_closed": self.clusters_closed,
            "noise_count": self.noise_count,
            "noise_fraction": round(self.noise_fraction, 6),
            "pending_count": self.pending_count,
            "max_extent_m": round(self.max_extent_m, 3),
            "max_cluster_diameter_m": self.params["max_cluster_diameter_m"],
            "duration_s": round(self.duration_s, 3),
            "persisted": self.persisted,
            "size_distribution": self.size_distribution().as_dict(),
            "size_histogram": SizeHistogram.of(self.sizes).as_dict(),
            "extent_distribution": self.extent_distribution().as_dict(),
        }


def day_bounds(start: date, end_inclusive: date) -> tuple[datetime, datetime]:
    """Inclusive calendar dates to a half-open UTC interval."""
    window_start = datetime.combine(start, datetime.min.time(), tzinfo=UTC)
    window_end = datetime.combine(
        end_inclusive + timedelta(days=1), datetime.min.time(), tzinfo=UTC
    )
    return window_start, window_end


def settled_horizon_for(
    evaluation_time: datetime, params: ClusterParams
) -> datetime:
    return evaluation_time - timedelta(
        seconds=params.linkage_gap_seconds + params.reconcile_temporal_seconds
    )


def cluster_detection_stream(
    detections: Iterable[Detection],
    params: ClusterParams,
    *,
    aoi_name: str,
    window_start: datetime,
    window_end: datetime,
    evaluation_time: datetime,
    writer: ClusterWriter | None = None,
    flush_every: int = 5_000,
    run_id: str | None = None,
) -> RunSummary:
    """Run one causal pass, optionally persisting as clusters close."""
    started = time.perf_counter()
    summary = RunSummary(
        run_id=run_id or (writer.run_id if writer else new_run_id()),
        aoi=aoi_name,
        window_start=window_start,
        window_end=window_end,
        evaluation_time=evaluation_time,
        settled_horizon=settled_horizon_for(evaluation_time, params),
        params=params.as_metadata(),
        persisted=writer is not None,
    )

    state = new_state(params)
    members: dict[str, list[Observation]] = {}
    closed_batch: list[tuple[Cluster, list[Observation], bool]] = []
    noise_batch: list[Observation] = []

    def absorb(effect: StepEffect) -> None:
        for cluster in effect.opened:
            members.setdefault(cluster.cluster_id, [])
        for cluster_id, observation in effect.admitted:
            members.setdefault(cluster_id, []).append(observation)
        for cluster in effect.closed:
            closed_batch.append(
                (cluster, members.pop(cluster.cluster_id, []), False)
            )
            summary.clusters_closed += 1
            summary.sizes.append(cluster.observation_count)
            summary.extents.append(cluster.extent_m)
            summary.max_extent_m = max(summary.max_extent_m, cluster.extent_m)
        noise_batch.extend(effect.noise)
        summary.noise_count += len(effect.noise)

    def flush(force: bool = False) -> None:
        if writer is None:
            closed_batch.clear()
            noise_batch.clear()
            return
        if force or len(closed_batch) >= flush_every:
            writer.write_clusters(closed_batch)
            closed_batch.clear()
        if force or len(noise_batch) >= flush_every:
            writer.write_noise(noise_batch)
            noise_batch.clear()

    def counted(source: Iterable[Detection]) -> Iterable[Detection]:
        for detection in source:
            summary.detections_read += 1
            yield detection

    for observation in reconcile(counted(detections), params):
        summary.observations += 1
        absorb(step(state, observation, params))
        flush()

    absorb(finalize(state, evaluation_time, params))

    for cluster_id, cluster in state.open_clusters.items():
        closed_batch.append((cluster, members.get(cluster_id, []), True))
        summary.clusters_open += 1
        summary.sizes.append(cluster.observation_count)
        summary.extents.append(cluster.extent_m)
        summary.max_extent_m = max(summary.max_extent_m, cluster.extent_m)

    summary.pending_count = len(state.pending)
    flush(force=True)
    summary.duration_s = time.perf_counter() - started
    return summary


def run_from_store(
    database: Database,
    params: ClusterParams,
    *,
    aoi: Aoi,
    start: date,
    end_inclusive: date,
    evaluation_time: datetime | None = None,
    products: Sequence[str] | None = None,
    batch_size: int = 20_000,
    flush_every: int = 5_000,
    persist: bool = True,
) -> RunSummary:
    """Cluster a date range read from the ingestion store."""
    window_start, window_end = day_bounds(start, end_inclusive)
    cutoff = evaluation_time or window_end
    run_id = new_run_id()

    writer = ClusterWriter(database, params, aoi_name=aoi.name, run_id=run_id)
    writer.require_schema()

    with log_context(
        source="clustering",
        run_id=run_id,
        aoi=aoi.name,
        window={
            "start": window_start.isoformat(),
            "end_exclusive": window_end.isoformat(),
        },
        evaluation_time=cutoff.isoformat(),
    ):
        if persist:
            writer.start_run(
                window_start=window_start,
                window_end=window_end,
                evaluation_time=cutoff,
                settled_horizon=settled_horizon_for(cutoff, params),
                products=products,
            )
        log_event(
            LOGGER,
            logging.INFO,
            "clustering_run_started",
            persist=persist,
            eps_m=params.eps_m,
            min_samples=params.min_samples,
            max_cluster_diameter_m=params.max_cluster_diameter_m,
        )

        detections = stream_detections(
            database,
            aoi=aoi,
            window_start=window_start,
            window_end=window_end,
            evaluation_time=cutoff,
            products=products,
            batch_size=batch_size,
        )
        try:
            summary = cluster_detection_stream(
                detections,
                params,
                aoi_name=aoi.name,
                window_start=window_start,
                window_end=window_end,
                evaluation_time=cutoff,
                writer=writer if persist else None,
                flush_every=flush_every,
                run_id=run_id,
            )
        except Exception as exc:
            if persist:
                _best_effort_fail(writer, exc)
            LOGGER.error(
                "clustering_run_failed",
                exc_info=True,
                extra={"event": "clustering_run_failed"},
            )
            raise

        if persist:
            writer.finish_run(
                "succeeded",
                {
                    "detections_read": summary.detections_read,
                    "observations": summary.observations,
                    "clusters_open": summary.clusters_open,
                    "clusters_closed": summary.clusters_closed,
                    "noise_count": summary.noise_count,
                    "pending_count": summary.pending_count,
                    "max_extent_m": summary.max_extent_m,
                    "duration_s": summary.duration_s,
                },
            )
        log_event(
            LOGGER,
            logging.INFO,
            "clustering_run_finished",
            cluster_count=summary.cluster_count,
            observations=summary.observations,
            noise_count=summary.noise_count,
            max_extent_m=round(summary.max_extent_m, 1),
            duration_s=round(summary.duration_s, 3),
        )
        return summary


def _best_effort_fail(writer: ClusterWriter, error: BaseException) -> None:
    try:
        writer.finish_run(
            "failed",
            {"error": f"{type(error).__name__}: {redact(str(error))}"},
        )
    except Exception as bookkeeping_error:  # pragma: no cover - defensive
        log_event(
            LOGGER,
            logging.ERROR,
            "clustering_run_finalization_failed",
            original_error=f"{type(error).__name__}: {redact(str(error))}",
            bookkeeping_error=(
                f"{type(bookkeeping_error).__name__}: "
                f"{redact(str(bookkeeping_error))}"
            ),
        )
