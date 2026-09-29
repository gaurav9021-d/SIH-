"""Validated clustering parameters.

The epsilon ceiling is a hard refusal rather than a clamp: silently widening the
neighbourhood would change the meaning of every cluster the run produced.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from typing import Any

import h3

from ingestion.core.errors import ConfigurationError

from clustering.core.geo import point_to_segment_distance_m

EPS_CEILING_M = 750.0
ALGORITHM_NAME = "causal diameter-bounded spatiotemporal DBSCAN"
ALGORITHM_VERSION = "cdb-dbscan-1"

# Deterministic sample of positions used to measure the smallest H3 cell
# inradius at a resolution. The H3 library only exposes an *average* edge
# length, and cells vary, so the halo check measures real geometry instead of
# trusting the average.
_HALO_SAMPLE_LATS = (0.0, 7.5, 15.0, 22.5, 30.0, 37.5, 45.0, 60.0, 75.0, 85.0)
_HALO_SAMPLE_LONS = (-175.0, -90.0, -30.0, 0.0, 30.0, 77.0, 140.0, 179.0)


@dataclass(frozen=True, slots=True)
class ClusterParams:
    eps_m: float = 750.0
    min_samples: int = 2
    inactivity_days: float = 7.0
    max_linkage_gap_days: float | None = None  # defaults to inactivity_days
    max_cluster_diameter_m: float = 2000.0
    reconcile_spatial_m: float = 375.0
    reconcile_temporal_minutes: float = 15.0
    reconcile_enabled: bool = True
    h3_resolution: int = 6
    h3_ring: int = 1
    algorithm_version: str = ALGORITHM_VERSION

    @property
    def linkage_gap_days(self) -> float:
        return (
            self.inactivity_days
            if self.max_linkage_gap_days is None
            else self.max_linkage_gap_days
        )

    @property
    def linkage_gap_seconds(self) -> float:
        return self.linkage_gap_days * 86400.0

    @property
    def inactivity_seconds(self) -> float:
        return self.inactivity_days * 86400.0

    @property
    def reconcile_temporal_seconds(self) -> float:
        return self.reconcile_temporal_minutes * 60.0

    def resolved(self) -> "ClusterParams":
        """Materialise the linkage-gap default so the fingerprint is explicit."""
        if self.max_linkage_gap_days is None:
            return replace(self, max_linkage_gap_days=self.inactivity_days)
        return self

    def fingerprint(self) -> str:
        payload = asdict(self.resolved())
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]

    def as_metadata(self) -> dict[str, Any]:
        data = asdict(self.resolved())
        data["params_fingerprint"] = self.fingerprint()
        data["algorithm_name"] = ALGORITHM_NAME
        data["diameter_bounded"] = True
        data["merging"] = "disabled"
        return data


def min_cell_inradius_m(resolution: int) -> float:
    """Smallest measured inradius of an H3 cell at this resolution.

    The inradius is the distance from the cell centre to its nearest edge, which
    is the correct bound for a halo: a neighbour further than this from the
    centre may fall outside the cell.
    """
    smallest = math.inf
    for lat in _HALO_SAMPLE_LATS:
        for lon in _HALO_SAMPLE_LONS:
            cell = h3.latlng_to_cell(lat, lon, resolution)
            centre_lat, centre_lon = h3.cell_to_latlng(cell)
            boundary = h3.cell_to_boundary(cell)
            for index in range(len(boundary)):
                a_lat, a_lon = boundary[index]
                b_lat, b_lon = boundary[(index + 1) % len(boundary)]
                smallest = min(
                    smallest,
                    point_to_segment_distance_m(
                        centre_lat, centre_lon, a_lat, a_lon, b_lat, b_lon
                    ),
                )
    return smallest


def validate(params: ClusterParams) -> ClusterParams:
    """Return resolved parameters, or raise ConfigurationError."""
    resolved = params.resolved()

    if resolved.eps_m <= 0.0:
        raise ConfigurationError("eps_m must be greater than zero")
    if resolved.eps_m > EPS_CEILING_M:
        raise ConfigurationError(
            f"eps_m={resolved.eps_m} exceeds the {EPS_CEILING_M} m ceiling; "
            "refusing to cluster with a wider neighbourhood"
        )
    if resolved.min_samples < 1:
        raise ConfigurationError("min_samples must be at least 1")
    if resolved.inactivity_days <= 0.0:
        raise ConfigurationError("inactivity_days must be greater than zero")
    if resolved.linkage_gap_days <= 0.0:
        raise ConfigurationError("max_linkage_gap_days must be greater than zero")
    if resolved.max_cluster_diameter_m < resolved.eps_m:
        raise ConfigurationError(
            "max_cluster_diameter_m must be at least eps_m, otherwise no two "
            "reachable observations could ever share a cluster"
        )
    if resolved.reconcile_spatial_m <= 0.0:
        raise ConfigurationError("reconcile_spatial_m must be greater than zero")
    if resolved.reconcile_spatial_m > resolved.eps_m:
        raise ConfigurationError(
            "reconcile_spatial_m must not exceed eps_m, otherwise reconciliation "
            "would merge fires that clustering would have kept apart"
        )
    if resolved.reconcile_temporal_minutes <= 0.0:
        raise ConfigurationError(
            "reconcile_temporal_minutes must be greater than zero"
        )
    if not 0 <= resolved.h3_resolution <= 15:
        raise ConfigurationError("h3_resolution must be between 0 and 15")
    if resolved.h3_ring < 1:
        raise ConfigurationError("h3_ring must be at least 1")

    inradius = min_cell_inradius_m(resolved.h3_resolution)
    halo_m = resolved.h3_ring * inradius
    if halo_m < resolved.eps_m:
        raise ConfigurationError(
            f"H3 resolution {resolved.h3_resolution} with ring {resolved.h3_ring} "
            f"gives a {halo_m:.0f} m halo, below eps_m={resolved.eps_m}. A "
            "truncated neighbourhood silently fragments clusters; lower the "
            "resolution or raise the ring."
        )
    return resolved


def from_mapping(data: dict[str, Any] | None) -> ClusterParams:
    """Build parameters from a configuration mapping, rejecting unknown keys."""
    if not data:
        return validate(ClusterParams())
    known = {f for f in ClusterParams.__dataclass_fields__}
    unknown = set(data) - known
    if unknown:
        raise ConfigurationError(
            f"Unknown clustering parameters: {', '.join(sorted(unknown))}"
        )
    return validate(ClusterParams(**data))
