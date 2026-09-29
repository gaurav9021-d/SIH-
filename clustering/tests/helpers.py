"""Shared test builders."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from clustering.core.geo import meters_per_degree
from clustering.core.models import Detection, Observation, observation_key_for
from clustering.core.params import ClusterParams
from clustering.dbscan import ClusteringResult, run_observations
from clustering.spatial_dedup import reconcile

BASE_TIME = datetime(2025, 11, 1, 0, 0, tzinfo=UTC)
BASE_LAT = 30.5
BASE_LON = 75.0


def offset(lat: float, north_m: float, east_m: float) -> tuple[float, float]:
    m_lat, m_lon = meters_per_degree(lat)
    return lat + north_m / m_lat, east_m / m_lon


def detection(
    index: int,
    *,
    minutes: float = 0.0,
    north_m: float = 0.0,
    east_m: float = 0.0,
    frp: float = 10.0,
    day_night: str = "D",
    product: str = "VIIRS_SNPP_SP",
    satellite: str = "N",
    lat: float = BASE_LAT,
    lon: float = BASE_LON,
) -> Detection:
    m_lat, m_lon = meters_per_degree(lat)
    return Detection(
        detection_id=index,
        as_of=BASE_TIME + timedelta(minutes=minutes),
        lat=lat + north_m / m_lat,
        lon=lon + east_m / m_lon,
        frp=frp,
        day_night=day_night,
        product=product,
        satellite=satellite,
    )


def observation(
    index: int,
    *,
    minutes: float = 0.0,
    north_m: float = 0.0,
    east_m: float = 0.0,
    frp: float = 10.0,
    day_night: str = "D",
) -> Observation:
    source = detection(
        index,
        minutes=minutes,
        north_m=north_m,
        east_m=east_m,
        frp=frp,
        day_night=day_night,
    )
    return Observation(
        observation_key=observation_key_for(source),
        lat=source.lat,
        lon=source.lon,
        as_of=source.as_of,
        frp=source.frp,
        day_night=source.day_night,
        detection_count=1,
        products=(source.product,),
        satellites=(source.satellite,),
        detection_refs=((source.detection_id, source.as_of),),
    )


def observations_from(
    detections: list[Detection], params: ClusterParams
) -> list[Observation]:
    return list(reconcile(detections, params))


def cluster_detections(
    detections: list[Detection],
    params: ClusterParams,
    *,
    evaluation_time: datetime | None = None,
    index=None,
) -> ClusteringResult:
    return run_observations(
        observations_from(detections, params),
        params,
        evaluation_time=evaluation_time,
        index=index,
    )


def max_extent(result: ClusteringResult) -> float:
    if not result.clusters:
        return 0.0
    return max(c.extent_m for c in result.clusters.values())
