"""Property-based invariants over generated observation streams.

Parametrised cases prove the algorithm on geometries we thought of. These
properties push it at geometries we did not.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from clustering.core.geo import distance_m
from clustering.core.params import ClusterParams, validate
from clustering.dbscan import run_observations
from clustering.tests.helpers import BASE_TIME, observation

PARAMS = validate(ClusterParams())
PROFILE = settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

# Points within a few kilometres and a few days: the regime where chaining,
# closure and the extent bound all interact.
_placements = st.lists(
    st.tuples(
        st.floats(min_value=0.0, max_value=14.0 * 24 * 60),  # minutes
        st.floats(min_value=-3_000.0, max_value=3_000.0),  # metres north
        st.floats(min_value=-3_000.0, max_value=3_000.0),  # metres east
    ),
    min_size=1,
    max_size=60,
)


def _stream(placements):
    ordered = sorted(placements, key=lambda p: p[0])
    return [
        observation(index, minutes=minutes, north_m=north, east_m=east)
        for index, (minutes, north, east) in enumerate(ordered)
    ]


@PROFILE
@given(_placements)
def test_extent_never_exceeds_the_configured_bound(placements) -> None:
    result = run_observations(_stream(placements), PARAMS)

    for cluster in result.clusters.values():
        assert cluster.extent_m <= PARAMS.max_cluster_diameter_m + 1e-6


@PROFILE
@given(_placements, st.floats(min_value=760.0, max_value=4_000.0))
def test_extent_bound_holds_for_any_valid_diameter(placements, diameter_m) -> None:
    params = validate(replace(PARAMS, max_cluster_diameter_m=diameter_m))
    result = run_observations(_stream(placements), params)

    for cluster in result.clusters.values():
        assert cluster.extent_m <= diameter_m + 1e-6


@PROFILE
@given(_placements)
def test_stored_extent_is_the_true_member_diameter(placements) -> None:
    result = run_observations(_stream(placements), PARAMS)

    for cluster_id, cluster in result.clusters.items():
        members = result.members[cluster_id]
        brute = max(
            (
                distance_m(a.lat, a.lon, b.lat, b.lon)
                for index, a in enumerate(members)
                for b in members[index + 1 :]
            ),
            default=0.0,
        )
        assert abs(cluster.extent_m - brute) < 1e-6


@PROFILE
@given(_placements)
def test_runs_are_deterministic(placements) -> None:
    stream = _stream(placements)

    assert run_observations(stream, PARAMS).final_view() == run_observations(
        stream, PARAMS
    ).final_view()


@PROFILE
@given(_placements, st.floats(min_value=0.0, max_value=20.0 * 24 * 60))
def test_replay_prefix_holds_at_arbitrary_cutoffs(placements, cutoff_minutes) -> None:
    stream = _stream(placements)
    cutoff = BASE_TIME + timedelta(minutes=cutoff_minutes)
    final_time = BASE_TIME + timedelta(days=40)

    early = run_observations(
        [o for o in stream if o.as_of <= cutoff], PARAMS, evaluation_time=cutoff
    )
    late = run_observations(stream, PARAMS, evaluation_time=final_time)

    assert late.view_as_of(cutoff) == early.final_view()


@PROFILE
@given(_placements)
def test_every_observation_is_clustered_noise_or_pending(placements) -> None:
    result = run_observations(_stream(placements), PARAMS)

    clustered = {
        member.observation_key
        for members in result.members.values()
        for member in members
    }
    noise = {o.observation_key for o in result.noise}
    pending = {o.observation_key for o in result.pending}
    every = {o.observation_key for o in result.observations}

    assert clustered | noise | pending == every
    assert not clustered & noise
    assert not clustered & pending
    assert not noise & pending


@PROFILE
@given(_placements)
def test_cluster_aggregates_match_their_members(placements) -> None:
    result = run_observations(_stream(placements), PARAMS)

    for cluster_id, cluster in result.clusters.items():
        members = result.members[cluster_id]
        assert cluster.observation_count == len(members)
        assert cluster.detection_count == sum(m.detection_count for m in members)
        assert cluster.first_detected == min(m.as_of for m in members)
        assert cluster.last_detected == max(m.as_of for m in members)
        assert 0.0 <= cluster.day_night_ratio <= 1.0
        assert cluster.frp_var >= 0.0
        assert cluster.persistence_days >= 0.0


@PROFILE
@given(_placements)
def test_closed_clusters_are_idle_beyond_the_cutoff(placements) -> None:
    result = run_observations(_stream(placements), PARAMS)

    for cluster in result.clusters.values():
        if cluster.closed_at is None:
            continue
        idle = (result.evaluation_time - cluster.last_detected).total_seconds()
        assert idle > PARAMS.inactivity_seconds
        assert cluster.closed_at == cluster.closes_at(PARAMS.inactivity_seconds)
