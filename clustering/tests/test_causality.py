"""Causality and replay determinism.

The contract: at any evaluation time T the engine may use only detections
acquired at or before T, and a later run must never rewrite what an earlier run
reported. These tests replay one stream to different cutoffs and compare.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from clustering.core.params import ClusterParams, validate
from clustering.dbscan import run_observations
from clustering.spatial_dedup import CausalOrderError
from clustering.tests.helpers import (
    BASE_TIME,
    cluster_detections,
    detection,
    observation,
    observations_from,
)

PARAMS = validate(ClusterParams())


def _mixed_stream() -> list:
    """A stream with a growing site, an isolated ping, and a reopening gap."""
    return [
        # A site detected repeatedly over three days.
        detection(1, minutes=0, north_m=0, east_m=0),
        detection(2, minutes=90, north_m=120, east_m=80),
        detection(3, minutes=60 * 26, north_m=200, east_m=150),
        detection(4, minutes=60 * 50, north_m=60, east_m=240),
        # An isolated ping 40 km away that should settle as noise.
        detection(5, minutes=60 * 52, north_m=40_000, east_m=0),
        # Same site again after more than the 7 day inactivity cutoff.
        detection(6, minutes=60 * 24 * 11, north_m=100, east_m=100),
        detection(7, minutes=60 * 24 * 11 + 45, north_m=150, east_m=160),
    ]


def test_determinism_across_repeated_runs() -> None:
    stream = _mixed_stream()
    first = cluster_detections(stream, PARAMS).final_view()
    second = cluster_detections(stream, PARAMS).final_view()

    assert first == second


def test_cluster_ids_are_stable_across_runs() -> None:
    stream = _mixed_stream()
    first = cluster_detections(stream, PARAMS)
    second = cluster_detections(stream, PARAMS)

    assert sorted(first.clusters) == sorted(second.clusters)


@pytest.mark.parametrize("cutoff_hours", [2, 27, 51, 53, 24 * 9, 24 * 11 + 1, 24 * 20])
def test_replay_prefix_property(cutoff_hours: int) -> None:
    """A later run's view as of T1 must equal an earlier run's own output."""
    stream = _mixed_stream()
    cutoff = BASE_TIME + timedelta(hours=cutoff_hours)
    final_time = BASE_TIME + timedelta(days=40)

    truncated = [d for d in stream if d.as_of <= cutoff]
    early = cluster_detections(truncated, PARAMS, evaluation_time=cutoff)

    late = cluster_detections(stream, PARAMS, evaluation_time=final_time)

    assert late.view_as_of(cutoff) == early.final_view()


def test_removing_future_observations_does_not_change_the_present() -> None:
    stream = _mixed_stream()
    cutoff = BASE_TIME + timedelta(hours=51)

    with_future = cluster_detections(
        stream, PARAMS, evaluation_time=BASE_TIME + timedelta(days=40)
    ).view_as_of(cutoff)
    without_future = cluster_detections(
        [d for d in stream if d.as_of <= cutoff], PARAMS, evaluation_time=cutoff
    ).view_as_of(cutoff)

    assert with_future == without_future


def test_closure_is_final_and_a_later_detection_opens_a_new_cluster() -> None:
    result = cluster_detections(_mixed_stream(), PARAMS)

    # The pre-gap site and the post-gap site must not share a cluster.
    assert len(result.clusters) >= 2
    early = [
        c
        for c in result.clusters.values()
        if c.first_detected < BASE_TIME + timedelta(days=3)
    ]
    late = [
        c
        for c in result.clusters.values()
        if c.first_detected > BASE_TIME + timedelta(days=10)
    ]
    assert early and late
    assert early[0].cluster_id != late[0].cluster_id
    assert early[0].status == "closed"
    assert early[0].closed_at is not None


def test_closure_time_depends_only_on_the_clusters_own_history() -> None:
    """Closure must not depend on which observation happened to trigger it."""
    stream = _mixed_stream()
    short = cluster_detections(stream, PARAMS, evaluation_time=BASE_TIME + timedelta(days=20))
    long = cluster_detections(stream, PARAMS, evaluation_time=BASE_TIME + timedelta(days=90))

    shared = set(short.clusters) & set(long.clusters)
    assert shared
    for cluster_id in shared:
        a, b = short.clusters[cluster_id], long.clusters[cluster_id]
        if a.closed_at is not None and b.closed_at is not None:
            assert a.closed_at == b.closed_at


def test_out_of_order_input_is_rejected() -> None:
    forwards = observation(1, minutes=120)
    backwards = observation(2, minutes=10)

    with pytest.raises(CausalOrderError):
        run_observations([forwards, backwards], PARAMS)


def test_out_of_order_detections_are_rejected_during_reconciliation() -> None:
    stream = [detection(1, minutes=120), detection(2, minutes=5)]

    with pytest.raises(CausalOrderError):
        observations_from(stream, PARAMS)


def test_pending_observation_is_provisional_then_settles_as_noise() -> None:
    """An isolated ping is undecided inside its linkage window, noise after."""
    stream = [detection(1, minutes=0, north_m=0, east_m=0)]

    inside = cluster_detections(
        stream, PARAMS, evaluation_time=BASE_TIME + timedelta(days=3)
    ).final_view()
    assert inside.pending_keys and not inside.noise_keys

    outside = cluster_detections(
        stream, PARAMS, evaluation_time=BASE_TIME + timedelta(days=9)
    ).final_view()
    assert outside.noise_keys and not outside.pending_keys
