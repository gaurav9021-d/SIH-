"""Emit clusters in the `data-contracts.md` shape.

Only cluster identity, geometry, timing and `features.firms` are populated. The
enrichment blocks, `classification` and `anomaly` are emitted as null because
they belong to later stages, and a null is honest where a placeholder would not
be.
"""

from __future__ import annotations

from typing import Any

from clustering.core.models import Cluster, isoformat_z
from clustering.core.params import ALGORITHM_NAME, ClusterParams


def to_contract(
    cluster: Cluster,
    params: ClusterParams,
    *,
    provisional: bool = False,
) -> dict[str, Any]:
    centroid_lat, centroid_lon = cluster.centroid
    min_lat, min_lon, max_lat, max_lon = cluster.bounding_box()
    return {
        "cluster_id": cluster.cluster_id,
        "centroid": {"lat": round(centroid_lat, 6), "lon": round(centroid_lon, 6)},
        "bounding_box": {
            "min_lat": round(min_lat, 6),
            "min_lon": round(min_lon, 6),
            "max_lat": round(max_lat, 6),
            "max_lon": round(max_lon, 6),
        },
        "first_detected": isoformat_z(cluster.first_detected),
        "last_detected": isoformat_z(cluster.last_detected),
        "detection_count": cluster.detection_count,
        "observation_count": cluster.observation_count,
        "features": {
            "firms": {
                "persistence_days": round(cluster.persistence_days, 4),
                "frp_mean": round(cluster.frp_mean, 4),
                "frp_var": round(cluster.frp_var, 4),
                "day_night_ratio": round(cluster.day_night_ratio, 4),
                "as_of": isoformat_z(cluster.last_detected),
            },
            "land_cover": None,
            "osm": None,
            "no2": None,
            "optical": None,
            "wind": None,
        },
        "classification": None,
        "anomaly": None,
        "algorithm": {
            "name": ALGORITHM_NAME,
            "version": params.algorithm_version,
            "params_fingerprint": params.fingerprint(),
            "eps_m": params.eps_m,
            "min_samples": params.min_samples,
            "max_cluster_diameter_m": params.max_cluster_diameter_m,
            "extent_m": round(cluster.extent_m, 3),
            "diameter_bounded": True,
            "merging": "disabled",
            "formed_at": isoformat_z(cluster.formed_at),
            "status": cluster.status,
            "closed_at": isoformat_z(cluster.closed_at) if cluster.closed_at else None,
            "provisional": provisional,
        },
    }


REQUIRED_TOP_LEVEL = (
    "cluster_id",
    "centroid",
    "bounding_box",
    "first_detected",
    "last_detected",
    "detection_count",
    "features",
    "classification",
    "anomaly",
)

DEFERRED_FEATURE_BLOCKS = ("land_cover", "osm", "no2", "optical", "wind")
