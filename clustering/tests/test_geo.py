"""Geometry: metric distance and exact hull diameter."""

from __future__ import annotations

import math
import random

import pytest
from geographiclib.geodesic import Geodesic

from clustering.core.geo import (
    Hull,
    distance_m,
    meters_per_degree,
    normalize_lon_delta,
)

GEOD = Geodesic.WGS84
DOCUMENTED_ERROR_BOUND_M = 0.05


@pytest.mark.parametrize("lat", [0.0, 12.0, 30.5, 45.0, 60.0, 80.0])
def test_distance_matches_geodesic(lat: float) -> None:
    """Pin the hot-path approximation to an exact Karney geodesic."""
    worst = 0.0
    for north_m, east_m in ((0, 5000), (5000, 0), (3000, 4000), (-2000, 1500)):
        m_lat, m_lon = meters_per_degree(lat)
        lat2 = lat + north_m / m_lat
        lon2 = 77.0 + east_m / m_lon
        exact = GEOD.Inverse(lat, 77.0, lat2, lon2)["s12"]
        worst = max(worst, abs(exact - distance_m(lat, 77.0, lat2, lon2)))
    assert worst < DOCUMENTED_ERROR_BOUND_M


def test_longitude_is_scaled_by_latitude() -> None:
    """One degree of longitude must shrink toward the poles."""
    equator = distance_m(0.0, 0.0, 0.0, 1.0)
    high = distance_m(60.0, 0.0, 60.0, 1.0)
    assert high < equator * 0.55


def test_antimeridian_wrap() -> None:
    assert normalize_lon_delta(359.0) == pytest.approx(-1.0)
    near = distance_m(10.0, 179.999, 10.0, -179.999)
    assert near < 300.0


def test_hull_diameter_matches_brute_force() -> None:
    rng = random.Random(7)
    for _ in range(40):
        points = [
            (30.5 + rng.uniform(-0.01, 0.01), 75.0 + rng.uniform(-0.01, 0.01))
            for _ in range(rng.randint(2, 40))
        ]
        hull = Hull()
        for lat, lon in points:
            hull.add(lat, lon)

        brute = max(
            distance_m(a[0], a[1], b[0], b[1])
            for index, a in enumerate(points)
            for b in points[index + 1 :]
        )
        assert hull.diameter_m == pytest.approx(brute, abs=1e-6)


def test_hull_stays_small_as_membership_grows() -> None:
    """Bounded memory: a long-lived cluster keeps vertices, not members."""
    rng = random.Random(11)
    hull = Hull()
    for _ in range(3000):
        hull.add(30.5 + rng.uniform(-0.005, 0.005), 75.0 + rng.uniform(-0.005, 0.005))

    assert hull.vertex_count < 40


def test_extent_after_adding_predicts_the_new_diameter() -> None:
    hull = Hull()
    hull.add(30.5, 75.0)
    hull.add(30.5, 75.005)

    predicted = hull.extent_after_adding(30.505, 75.0)
    hull.add(30.505, 75.0)
    assert hull.diameter_m == pytest.approx(predicted, abs=1e-9)


def test_empty_hull_has_no_extent() -> None:
    hull = Hull()
    assert hull.is_empty()
    assert hull.diameter_m == 0.0
    assert hull.extent_after_adding(1.0, 2.0) == 0.0


def test_single_point_hull_has_zero_diameter() -> None:
    hull = Hull()
    hull.add(30.5, 75.0)
    assert hull.diameter_m == 0.0
    assert math.isclose(hull.extent_after_adding(30.5, 75.0), 0.0, abs_tol=1e-9)
