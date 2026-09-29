"""Deterministic synthetic detection streams.

These are SYNTHETIC. They exist to prove algorithm behaviour under geometric
extremes without expanding the ingestion area of interest, and they must never be
presented as observed fire data.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from clustering.core.geo import meters_per_degree
from clustering.core.models import Detection

SYNTHETIC = True
_EPOCH = datetime(2026, 9, 8, 0, 0, tzinfo=UTC)


def _east_offset_deg(lat: float, metres: float) -> float:
    _, m_per_deg_lon = meters_per_degree(lat)
    return metres / m_per_deg_lon


def _north_offset_deg(lat: float, metres: float) -> float:
    m_per_deg_lat, _ = meters_per_degree(lat)
    return metres / m_per_deg_lat


def linear_chain_20km(
    *,
    lat: float = 30.5,
    lon: float = 75.0,
    spacing_m: float = 700.0,
    length_m: float = 20_000.0,
    step_minutes: int = 60,
) -> list[Detection]:
    """A chain of detections spaced inside epsilon, spanning far beyond the bound.

    Every consecutive pair sits within the 750 m epsilon, so textbook DBSCAN
    density-connects the whole line into one cluster roughly 20 km across. This is
    the exact geometry the extent guard exists to break up.
    """
    count = int(length_m // spacing_m) + 1
    detections: list[Detection] = []
    for index in range(count):
        detections.append(
            Detection(
                detection_id=1_000_000 + index,
                as_of=_EPOCH + timedelta(minutes=step_minutes * index),
                lat=lat,
                lon=lon + _east_offset_deg(lat, spacing_m * index),
                frp=12.0 + (index % 7),
                day_night="D" if index % 2 == 0 else "N",
                product="VIIRS_SNPP_SP",
                satellite="N",
            )
        )
    return detections


def dense_savanna_grid(
    *,
    lat: float = 12.0,
    lon: float = 20.0,
    side_m: float = 20_000.0,
    spacing_m: float = 550.0,
    jitter_m: float = 75.0,
    seed: int = 20260908,
    step_seconds: int = 10,
) -> list[Detection]:
    """A jittered lattice dense enough that plain DBSCAN returns one blob.

    Stands in for peak savanna burning on 8 September, where detections carpet a
    region rather than forming discrete sites.
    """
    rng = random.Random(seed)
    per_side = int(side_m // spacing_m) + 1
    detections: list[Detection] = []
    index = 0
    for row in range(per_side):
        for column in range(per_side):
            north_m = row * spacing_m + rng.uniform(-jitter_m, jitter_m)
            east_m = column * spacing_m + rng.uniform(-jitter_m, jitter_m)
            detections.append(
                Detection(
                    detection_id=2_000_000 + index,
                    as_of=_EPOCH + timedelta(seconds=step_seconds * index),
                    lat=lat + _north_offset_deg(lat, north_m),
                    lon=lon + _east_offset_deg(lat, east_m),
                    frp=5.0 + rng.uniform(0.0, 40.0),
                    day_night="D" if (row + column) % 3 else "N",
                    product="VIIRS_NOAA20_SP",
                    satellite="N20",
                )
            )
            index += 1
    return detections


FIXTURES: dict[str, Callable[[], list[Detection]]] = {
    "linear_chain_20km": linear_chain_20km,
    "dense_savanna_grid": dense_savanna_grid,
}


def build(name: str) -> list[Detection]:
    try:
        factory = FIXTURES[name]
    except KeyError as exc:
        available = ", ".join(sorted(FIXTURES))
        raise KeyError(f"Unknown fixture {name!r}; available: {available}") from exc
    return factory()
