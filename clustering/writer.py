"""Idempotent persistence into the clustering schema.

Cluster identifiers are deterministic, so re-running a range converges on the same
rows instead of duplicating them. Clusters are flushed as they close, which is why
memory tracks the active set rather than the range length.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text

from ingestion.core.db import Database
from ingestion.core.errors import IngestionError
from ingestion.core.logging import redact

from clustering.core.models import Cluster, Observation
from clustering.core.params import ClusterParams

EXPECTED_REVISION = "20260929_0002"
REQUIRED_TABLES = (
    "clustering.runs",
    "clustering.clusters",
    "clustering.observations",
    "clustering.cluster_members",
    "clustering.observation_detections",
    "clustering.noise_observations",
    "clustering.alembic_version",
)


class ExtentInvariantError(IngestionError):
    """A cluster exceeded the configured diameter. The run must not persist it."""


class SchemaNotReadyError(IngestionError):
    """The clustering schema is missing or out of date."""


_RUN_INSERT = text(
    """
    INSERT INTO clustering.runs
      (run_id, status, aoi, window_start, window_end, evaluation_time,
       settled_horizon, params, params_fingerprint, algorithm_version, products)
    VALUES
      (CAST(:run_id AS uuid), :status, :aoi, :window_start, :window_end,
       :evaluation_time, :settled_horizon, CAST(:params AS jsonb),
       :params_fingerprint, :algorithm_version, :products)
    ON CONFLICT (run_id) DO NOTHING
    """
)

_RUN_FINISH = text(
    """
    UPDATE clustering.runs
    SET status = :status, finished_at = now(), detections_read = :detections_read,
        observations = :observations, clusters_open = :clusters_open,
        clusters_closed = :clusters_closed, noise_count = :noise_count,
        pending_count = :pending_count, max_extent_m = :max_extent_m,
        duration_s = :duration_s, error_summary = :error_summary
    WHERE run_id = CAST(:run_id AS uuid)
    """
)

_OBSERVATION_UPSERT = text(
    """
    INSERT INTO clustering.observations
      (observation_key, params_fingerprint, lat, lon, as_of, frp, day_night,
       detection_count, products, satellites)
    VALUES
      (:observation_key, :params_fingerprint, :lat, :lon, :as_of, :frp,
       :day_night, :detection_count, :products, :satellites)
    ON CONFLICT (observation_key) DO UPDATE SET
      detection_count = EXCLUDED.detection_count,
      products = EXCLUDED.products,
      satellites = EXCLUDED.satellites
    """
)

_OBSERVATION_DETECTION_INSERT = text(
    """
    INSERT INTO clustering.observation_detections
      (observation_key, detection_id, detection_as_of)
    VALUES (:observation_key, :detection_id, :detection_as_of)
    ON CONFLICT DO NOTHING
    """
)

_CLUSTER_UPSERT = text(
    """
    INSERT INTO clustering.clusters
      (cluster_id, params_fingerprint, algorithm_version, status, aoi, formed_at,
       first_detected, last_detected, closed_at, centroid, min_lat, min_lon,
       max_lat, max_lon, extent_m, detection_count, observation_count, frp_mean,
       frp_var, day_night_ratio, persistence_days, features_as_of, provisional,
       run_id)
    VALUES
      (:cluster_id, :params_fingerprint, :algorithm_version, :status, :aoi,
       :formed_at, :first_detected, :last_detected, :closed_at,
       ST_SetSRID(ST_MakePoint(:centroid_lon, :centroid_lat), 4326),
       :min_lat, :min_lon, :max_lat, :max_lon, :extent_m, :detection_count,
       :observation_count, :frp_mean, :frp_var, :day_night_ratio,
       :persistence_days, :features_as_of, :provisional, CAST(:run_id AS uuid))
    ON CONFLICT (cluster_id) DO UPDATE SET
      status = EXCLUDED.status,
      last_detected = EXCLUDED.last_detected,
      closed_at = EXCLUDED.closed_at,
      centroid = EXCLUDED.centroid,
      min_lat = EXCLUDED.min_lat, min_lon = EXCLUDED.min_lon,
      max_lat = EXCLUDED.max_lat, max_lon = EXCLUDED.max_lon,
      extent_m = EXCLUDED.extent_m,
      detection_count = EXCLUDED.detection_count,
      observation_count = EXCLUDED.observation_count,
      frp_mean = EXCLUDED.frp_mean, frp_var = EXCLUDED.frp_var,
      day_night_ratio = EXCLUDED.day_night_ratio,
      persistence_days = EXCLUDED.persistence_days,
      features_as_of = EXCLUDED.features_as_of,
      provisional = EXCLUDED.provisional,
      run_id = EXCLUDED.run_id,
      updated_at = now()
    """
)

_MEMBER_INSERT = text(
    """
    INSERT INTO clustering.cluster_members (cluster_id, observation_key, as_of)
    VALUES (:cluster_id, :observation_key, :as_of)
    ON CONFLICT DO NOTHING
    """
)

_NOISE_UPSERT = text(
    """
    INSERT INTO clustering.noise_observations
      (observation_key, params_fingerprint, as_of, settled_at, run_id)
    VALUES
      (:observation_key, :params_fingerprint, :as_of, :settled_at,
       CAST(:run_id AS uuid))
    ON CONFLICT (observation_key) DO UPDATE SET
      settled_at = EXCLUDED.settled_at, run_id = EXCLUDED.run_id
    """
)


class ClusterWriter:
    def __init__(
        self,
        database: Database,
        params: ClusterParams,
        *,
        aoi_name: str,
        run_id: str,
    ) -> None:
        self.database = database
        self.params = params
        self.aoi_name = aoi_name
        self.run_id = run_id
        self._fingerprint = params.fingerprint()

    # -- schema ---------------------------------------------------------
    def require_schema(self) -> None:
        def operation() -> tuple[dict[str, bool], str | None]:
            with self.database.engine.connect() as connection:
                present = {
                    name: bool(
                        connection.execute(
                            text("SELECT to_regclass(:name) IS NOT NULL"),
                            {"name": name},
                        ).scalar_one()
                    )
                    for name in REQUIRED_TABLES
                }
                revision = None
                if present["clustering.alembic_version"]:
                    revision = connection.execute(
                        text("SELECT version_num FROM clustering.alembic_version")
                    ).scalar_one_or_none()
                return present, revision

        present, revision = self.database._retry("clustering_schema_check", operation)
        missing = [name for name, ok in present.items() if not ok]
        if missing:
            raise SchemaNotReadyError(
                "The clustering schema is not migrated; run "
                f"`python -m clustering migrate` (missing: {', '.join(missing)})"
            )
        if revision != EXPECTED_REVISION:
            raise SchemaNotReadyError(
                f"clustering schema revision is {revision!r}, expected "
                f"{EXPECTED_REVISION!r}; run `python -m clustering migrate`"
            )

    # -- run lifecycle --------------------------------------------------
    def start_run(
        self,
        *,
        window_start: datetime,
        window_end: datetime,
        evaluation_time: datetime,
        settled_horizon: datetime,
        products: Sequence[str] | None,
    ) -> None:
        payload = {
            "run_id": self.run_id,
            "status": "running",
            "aoi": self.aoi_name,
            "window_start": window_start,
            "window_end": window_end,
            "evaluation_time": evaluation_time,
            "settled_horizon": settled_horizon,
            "params": json.dumps(self.params.as_metadata(), sort_keys=True),
            "params_fingerprint": self._fingerprint,
            "algorithm_version": self.params.algorithm_version,
            "products": list(products) if products else [],
        }

        def operation() -> None:
            with self.database.engine.begin() as connection:
                connection.execute(_RUN_INSERT, payload)

        self.database._retry("clustering_start_run", operation)

    def finish_run(self, status: str, summary: dict[str, Any]) -> None:
        payload = {
            "run_id": self.run_id,
            "status": status,
            "detections_read": int(summary.get("detections_read", 0)),
            "observations": int(summary.get("observations", 0)),
            "clusters_open": int(summary.get("clusters_open", 0)),
            "clusters_closed": int(summary.get("clusters_closed", 0)),
            "noise_count": int(summary.get("noise_count", 0)),
            "pending_count": int(summary.get("pending_count", 0)),
            "max_extent_m": summary.get("max_extent_m"),
            "duration_s": summary.get("duration_s"),
            "error_summary": (
                redact(str(summary["error"]))[:2000] if summary.get("error") else None
            ),
        }

        def operation() -> None:
            with self.database.engine.begin() as connection:
                connection.execute(_RUN_FINISH, payload)

        self.database._retry("clustering_finish_run", operation)

    # -- data -----------------------------------------------------------
    def write_clusters(
        self, entries: Sequence[tuple[Cluster, list[Observation], bool]]
    ) -> None:
        """Persist clusters with their members, verifying the extent invariant."""
        if not entries:
            return

        for cluster, _, _ in entries:
            if cluster.extent_m > self.params.max_cluster_diameter_m:
                raise ExtentInvariantError(
                    f"cluster {cluster.cluster_id} has extent "
                    f"{cluster.extent_m:.1f} m, above the configured "
                    f"{self.params.max_cluster_diameter_m:.1f} m limit"
                )

        observation_rows: list[dict[str, Any]] = []
        detection_rows: list[dict[str, Any]] = []
        cluster_rows: list[dict[str, Any]] = []
        member_rows: list[dict[str, Any]] = []

        for cluster, members, provisional in entries:
            centroid_lat, centroid_lon = cluster.centroid
            min_lat, min_lon, max_lat, max_lon = cluster.bounding_box()
            cluster_rows.append(
                {
                    "cluster_id": cluster.cluster_id,
                    "params_fingerprint": self._fingerprint,
                    "algorithm_version": self.params.algorithm_version,
                    "status": cluster.status,
                    "aoi": self.aoi_name,
                    "formed_at": cluster.formed_at,
                    "first_detected": cluster.first_detected,
                    "last_detected": cluster.last_detected,
                    "closed_at": cluster.closed_at,
                    "centroid_lat": centroid_lat,
                    "centroid_lon": centroid_lon,
                    "min_lat": min_lat,
                    "min_lon": min_lon,
                    "max_lat": max_lat,
                    "max_lon": max_lon,
                    "extent_m": cluster.extent_m,
                    "detection_count": cluster.detection_count,
                    "observation_count": cluster.observation_count,
                    "frp_mean": cluster.frp_mean,
                    "frp_var": cluster.frp_var,
                    "day_night_ratio": cluster.day_night_ratio,
                    "persistence_days": cluster.persistence_days,
                    "features_as_of": cluster.last_detected,
                    "provisional": provisional,
                    "run_id": self.run_id,
                }
            )
            for member in members:
                observation_rows.append(self._observation_row(member))
                detection_rows.extend(self._detection_rows(member))
                member_rows.append(
                    {
                        "cluster_id": cluster.cluster_id,
                        "observation_key": member.observation_key,
                        "as_of": member.as_of,
                    }
                )

        def operation() -> None:
            with self.database.engine.begin() as connection:
                if observation_rows:
                    connection.execute(_OBSERVATION_UPSERT, observation_rows)
                if detection_rows:
                    connection.execute(_OBSERVATION_DETECTION_INSERT, detection_rows)
                connection.execute(_CLUSTER_UPSERT, cluster_rows)
                if member_rows:
                    connection.execute(_MEMBER_INSERT, member_rows)

        self.database._retry("clustering_write_clusters", operation)

    def write_noise(self, observations: Sequence[Observation]) -> None:
        if not observations:
            return
        observation_rows = [self._observation_row(o) for o in observations]
        detection_rows = [row for o in observations for row in self._detection_rows(o)]
        noise_rows = [
            {
                "observation_key": o.observation_key,
                "params_fingerprint": self._fingerprint,
                "as_of": o.as_of,
                "settled_at": o.as_of
                + timedelta(seconds=self.params.linkage_gap_seconds),
                "run_id": self.run_id,
            }
            for o in observations
        ]

        def operation() -> None:
            with self.database.engine.begin() as connection:
                connection.execute(_OBSERVATION_UPSERT, observation_rows)
                if detection_rows:
                    connection.execute(_OBSERVATION_DETECTION_INSERT, detection_rows)
                connection.execute(_NOISE_UPSERT, noise_rows)

        self.database._retry("clustering_write_noise", operation)

    def _observation_row(self, observation: Observation) -> dict[str, Any]:
        return {
            "observation_key": observation.observation_key,
            "params_fingerprint": self._fingerprint,
            "lat": observation.lat,
            "lon": observation.lon,
            "as_of": observation.as_of,
            "frp": observation.frp,
            "day_night": observation.day_night,
            "detection_count": observation.detection_count,
            "products": list(observation.products),
            "satellites": list(observation.satellites),
        }

    @staticmethod
    def _detection_rows(observation: Observation) -> list[dict[str, Any]]:
        return [
            {
                "observation_key": observation.observation_key,
                "detection_id": detection_id,
                "detection_as_of": detection_as_of,
            }
            for detection_id, detection_as_of in observation.detection_refs
        ]


def new_run_id() -> str:
    return str(uuid.uuid4())


__all__ = [
    "ClusterWriter",
    "ExtentInvariantError",
    "SchemaNotReadyError",
    "new_run_id",
]
