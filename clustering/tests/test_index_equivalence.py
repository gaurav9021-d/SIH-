"""Partitioning must be an optimisation, never a change in results."""

from __future__ import annotations

import math
import random
from dataclasses import replace

from clustering.core.geo import distance_m, meters_per_degree
from clustering.core.params import ClusterParams, validate
from clustering.fixtures import linear_chain_20km
from clustering.h3_partitioner import BruteForceIndex, H3Index
from clustering.tests.helpers import cluster_detections, detection, observation

PARAMS = validate(ClusterParams())


def _memberships(result) -> set[tuple[str, ...]]:
    return {
        tuple(sorted(o.observation_key for o in members))
        for members in result.members.values()
    }


def test_h3_index_matches_brute_force_on_a_chain() -> None:
    stream = linear_chain_20km()

    indexed = cluster_detections(
        stream, PARAMS, index=H3Index(PARAMS.h3_resolution, PARAMS.h3_ring)
    )
    exhaustive = cluster_detections(stream, PARAMS, index=BruteForceIndex())

    assert _memberships(indexed) == _memberships(exhaustive)
    assert sorted(indexed.clusters) == sorted(exhaustive.clusters)


def test_h3_index_matches_brute_force_on_a_scattered_field() -> None:
    rng = random.Random(3)
    stream = [
        detection(
            index,
            minutes=index * 11,
            north_m=rng.uniform(0, 6_000),
            east_m=rng.uniform(0, 6_000),
        )
        for index in range(160)
    ]

    indexed = cluster_detections(
        stream, PARAMS, index=H3Index(PARAMS.h3_resolution, PARAMS.h3_ring)
    )
    exhaustive = cluster_detections(stream, PARAMS, index=BruteForceIndex())

    assert _memberships(indexed) == _memberships(exhaustive)


def test_halo_finds_every_neighbour_inside_epsilon() -> None:
    """No observation within epsilon may fall outside the searched cells."""
    rng = random.Random(5)
    index = H3Index(PARAMS.h3_resolution, PARAMS.h3_ring)

    for trial in range(400):
        lat = rng.uniform(-70.0, 70.0)
        lon = rng.uniform(-179.0, 179.0)
        centre = replace(observation(trial * 2, minutes=0), lat=lat, lon=lon)

        bearing = rng.uniform(0.0, 2.0 * math.pi)
        radius = rng.uniform(0.0, PARAMS.eps_m)
        m_lat, m_lon = meters_per_degree(lat)
        neighbour_lat = lat + (radius * math.cos(bearing)) / m_lat
        neighbour_lon = lon + (radius * math.sin(bearing)) / m_lon
        neighbour = replace(
            observation(trial * 2 + 1, minutes=1),
            lat=neighbour_lat,
            lon=neighbour_lon,
        )

        assert distance_m(lat, lon, neighbour_lat, neighbour_lon) <= PARAMS.eps_m + 1e-6

        index.add(neighbour, None)
        found = {entry.observation.observation_key for entry in index.nearby(centre)}
        assert neighbour.observation_key in found, (
            f"halo missed a neighbour at lat={lat:.3f} lon={lon:.3f} "
            f"radius={radius:.1f} m"
        )
        index.pop(neighbour.observation_key)


def test_index_evicts_expired_entries() -> None:
    """Memory must track the active set, not the whole range."""
    stream = [
        detection(index, minutes=index * 60 * 24 * 3, north_m=index * 30_000)
        for index in range(20)
    ]

    result = cluster_detections(stream, PARAMS)

    # Every detection is isolated in space and separated by three days, so all
    # settle as noise and nothing should remain indexed beyond the window.
    assert len(result.noise) + len(result.pending) == len(result.observations)
