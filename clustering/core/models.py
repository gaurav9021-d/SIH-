"""Data types for detections, observations and clusters."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from clustering.core.geo import Hull, bounding_box

_COORD_PRECISION = 6  # ~0.11 m, finer than any FIRMS product reports


@dataclass(frozen=True, slots=True)
class Detection:
    """One FIRMS pixel flagged as fire at one acquisition time."""

    detection_id: int
    as_of: datetime
    lat: float
    lon: float
    frp: float
    day_night: str
    product: str
    satellite: str

    @property
    def source(self) -> tuple[str, str]:
        return self.product, self.satellite


@dataclass(frozen=True, slots=True)
class Observation:
    """One physical fire sighting, after reconciling overlapping sensors."""

    observation_key: str
    lat: float
    lon: float
    as_of: datetime
    frp: float
    day_night: str
    detection_count: int
    products: tuple[str, ...]
    satellites: tuple[str, ...]
    detection_refs: tuple[tuple[int, datetime], ...]


def observation_key_for(detection: Detection) -> str:
    """Deterministic identity for an observation, from its anchor detection."""
    payload = "|".join(
        (
            f"{detection.lat:.{_COORD_PRECISION}f}",
            f"{detection.lon:.{_COORD_PRECISION}f}",
            detection.as_of.isoformat(),
            detection.product,
            detection.satellite,
        )
    )
    return "o-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def cluster_id_for(
    *, algorithm_version: str, params_fingerprint: str, seed_key: str
) -> str:
    """Deterministic cluster identity, scoped to its parameter set."""
    payload = f"{algorithm_version}|{params_fingerprint}|{seed_key}"
    return "c-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


@dataclass(slots=True)
class Cluster:
    """A site-scale group of observations, grown causally.

    Only the hull and O(1) aggregates are retained. Reachability is answered from
    the shared neighbour index, and full membership is streamed out through
    `StepEffect`, so a long-lived cluster never accumulates a member list.
    """

    cluster_id: str
    seed_key: str
    formed_at: datetime
    first_detected: datetime
    last_detected: datetime
    hull: Hull = field(default_factory=Hull)
    closed_at: datetime | None = None
    observation_count: int = 0
    detection_count: int = 0
    day_count: int = 0
    night_count: int = 0
    _frp_mean: float = 0.0
    _frp_m2: float = 0.0
    _lat_sum: float = 0.0
    _lon_sum: float = 0.0
    min_lat: float = 90.0
    min_lon: float = 180.0
    max_lat: float = -90.0
    max_lon: float = -180.0

    @property
    def status(self) -> str:
        return "closed" if self.closed_at is not None else "open"

    @property
    def extent_m(self) -> float:
        return self.hull.diameter_m

    @property
    def centroid(self) -> tuple[float, float]:
        if self.observation_count == 0:
            return 0.0, 0.0
        return (
            self._lat_sum / self.observation_count,
            self._lon_sum / self.observation_count,
        )

    @property
    def frp_mean(self) -> float:
        return self._frp_mean

    @property
    def frp_var(self) -> float:
        """Population variance, ddof=0, so a single observation gives 0.0."""
        if self.observation_count == 0:
            return 0.0
        return self._frp_m2 / self.observation_count

    @property
    def day_night_ratio(self) -> float:
        """Day fraction: 0.0 fully nocturnal, 1.0 fully diurnal."""
        total = self.day_count + self.night_count
        if total == 0:
            return 0.0
        return self.day_count / total

    @property
    def persistence_days(self) -> float:
        return (self.last_detected - self.first_detected).total_seconds() / 86400.0

    def absorb(self, observation: Observation) -> None:
        """Admit an observation and update every aggregate in constant time."""
        self.hull.add(observation.lat, observation.lon)
        self.observation_count += 1
        self.detection_count += observation.detection_count
        if observation.day_night == "D":
            self.day_count += 1
        else:
            self.night_count += 1

        # Welford, for numerically stable variance over long-lived clusters.
        delta = observation.frp - self._frp_mean
        self._frp_mean += delta / self.observation_count
        self._frp_m2 += delta * (observation.frp - self._frp_mean)

        self._lat_sum += observation.lat
        self._lon_sum += observation.lon
        self.min_lat = min(self.min_lat, observation.lat)
        self.min_lon = min(self.min_lon, observation.lon)
        self.max_lat = max(self.max_lat, observation.lat)
        self.max_lon = max(self.max_lon, observation.lon)

        if observation.as_of < self.first_detected:
            self.first_detected = observation.as_of
        if observation.as_of > self.last_detected:
            self.last_detected = observation.as_of

    def closes_at(self, inactivity_seconds: float) -> datetime:
        """Acquisition-time instant this cluster becomes closed.

        Derived from the cluster's own history rather than from whichever
        observation happened to trigger the check, so closure is a pure function
        of the data and survives replay at a different cutoff.
        """
        return self.last_detected + timedelta(seconds=inactivity_seconds)

    def bounding_box(self) -> tuple[float, float, float, float]:
        return self.min_lat, self.min_lon, self.max_lat, self.max_lon


@dataclass(slots=True)
class StepEffect:
    """What one observation changed. Lets the driver stream results out."""

    opened: list[Cluster] = field(default_factory=list)
    admitted: list[tuple[str, Observation]] = field(default_factory=list)
    closed: list[Cluster] = field(default_factory=list)
    noise: list[Observation] = field(default_factory=list)
    pending_added: list[Observation] = field(default_factory=list)

    def merge(self, other: "StepEffect") -> None:
        self.opened.extend(other.opened)
        self.admitted.extend(other.admitted)
        self.closed.extend(other.closed)
        self.noise.extend(other.noise)
        self.pending_added.extend(other.pending_added)


def hull_bounding_box(coords: list[tuple[float, float]]) -> dict[str, float]:
    min_lat, min_lon, max_lat, max_lon = bounding_box(coords)
    return {
        "min_lat": min_lat,
        "min_lon": min_lon,
        "max_lat": max_lat,
        "max_lon": max_lon,
    }


def isoformat_z(value: datetime) -> str:
    """UTC ISO 8601 with a trailing Z, matching the data contract.

    Converts to UTC rather than trusting the incoming offset: `timestamptz`
    values arrive in whatever timezone the database session uses, so a non-UTC
    session would otherwise emit `+05:30` and silently break the contract.

    A naive datetime has no defined zone, so assuming one here would mislabel
    local time as UTC. That is a programming error, and failing is better than
    writing a wrong timestamp into a feature downstream stages depend on.
    """
    if value.tzinfo is None:
        raise ValueError(
            "refusing to format a naive datetime as UTC; every timestamp in the "
            "pipeline must be timezone-aware"
        )
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def as_json_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return isoformat_z(value)
    return value
