"""The reason this spec exists: epsilon alone cannot bound cluster extent.

DBSCAN groups by transitive density connectivity. A chain of detections each
inside epsilon of the next merges into one cluster of unbounded length, so an
epsilon of 750 m does not imply a cluster smaller than 2 km. These tests prove
the separate extent guard is what actually bounds it.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from clustering.core.geo import distance_m
from clustering.core.params import ClusterParams, validate
from clustering.fixtures import dense_savanna_grid, linear_chain_20km
from clustering.tests.helpers import cluster_detections, max_extent

PARAMS = validate(ClusterParams())
UNBOUNDED = validate(replace(PARAMS, max_cluster_diameter_m=1_000_000.0))


def test_chain_is_genuinely_density_connected() -> None:
    """Confirm the fixture really is a DBSCAN chain, not a set of islands."""
    detections = linear_chain_20km()
    assert len(detections) >= 25
    for previous, current in zip(detections, detections[1:]):
        gap = distance_m(previous.lat, previous.lon, current.lat, current.lon)
        assert gap <= PARAMS.eps_m, "consecutive detections must be within epsilon"

    span = distance_m(
        detections[0].lat,
        detections[0].lon,
        detections[-1].lat,
        detections[-1].lon,
    )
    assert span > 19_000.0


def test_without_the_guard_the_chain_becomes_one_runaway_cluster() -> None:
    """Baseline: with the extent bound effectively removed, DBSCAN chains."""
    result = cluster_detections(linear_chain_20km(), UNBOUNDED)
    assert len(result.clusters) == 1
    only = next(iter(result.clusters.values()))
    assert only.extent_m > 19_000.0


def test_the_guard_splits_the_chain_into_bounded_clusters() -> None:
    result = cluster_detections(linear_chain_20km(), PARAMS)

    assert len(result.clusters) >= 10, (
        "the 20 km chain must be split, not merged into a super-cluster"
    )
    assert max_extent(result) <= PARAMS.max_cluster_diameter_m
    # Every observation is accounted for: clustered, settled noise or pending.
    clustered = sum(len(m) for m in result.members.values())
    assert clustered + len(result.noise) + len(result.pending) == len(
        result.observations
    )


def test_dense_region_tiles_into_many_bounded_clusters() -> None:
    result = cluster_detections(dense_savanna_grid(), PARAMS)

    assert len(result.clusters) >= 100
    assert max_extent(result) <= PARAMS.max_cluster_diameter_m


def test_without_the_guard_the_dense_region_collapses() -> None:
    result = cluster_detections(dense_savanna_grid(), UNBOUNDED)

    assert len(result.clusters) == 1
    only = next(iter(result.clusters.values()))
    assert only.extent_m > 10_000.0


@pytest.mark.parametrize("diameter_m", [800.0, 1200.0, 2000.0, 3000.0])
def test_extent_bound_is_respected_for_any_configured_diameter(
    diameter_m: float,
) -> None:
    params = validate(replace(PARAMS, max_cluster_diameter_m=diameter_m))
    result = cluster_detections(linear_chain_20km(), params)

    assert result.clusters
    assert max_extent(result) <= diameter_m


def test_extent_equals_exact_member_diameter() -> None:
    """The stored extent must be the true diameter, not a bbox approximation."""
    result = cluster_detections(dense_savanna_grid(), PARAMS)

    for cluster_id, cluster in result.clusters.items():
        members = result.members[cluster_id]
        brute_force = max(
            (
                distance_m(a.lat, a.lon, b.lat, b.lon)
                for index, a in enumerate(members)
                for b in members[index + 1 :]
            ),
            default=0.0,
        )
        assert cluster.extent_m == pytest.approx(brute_force, abs=1e-6)
