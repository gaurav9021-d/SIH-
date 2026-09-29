"""Causal diameter-bounded spatiotemporal DBSCAN.

`step` is free of I/O, clocks and global state, which is what makes causality
directly testable: feed the same observation stream to different cutoffs and
compare. It mutates `ClusterState` in place and returns the effect rather than
returning a fresh state, because rebuilding persistent state per observation
would be O(n) per step. Determinism does not depend on immutability here; it
depends on the decision for each observation using only state accumulated
strictly before it.

Three deviations from textbook DBSCAN, each deliberate:

1. Diameter-bounded growth. Epsilon bounds the neighbourhood; DBSCAN's transitive
   density connectivity then lets a chain of near neighbours span any distance.
   `max_cluster_diameter_m` bounds the cluster itself.
2. Merging disabled. An observation reachable from two clusters joins the nearest
   one. Merging would break the extent bound and would force one of the two
   cluster identities to be discarded.
3. Streaming core-point determination. Density is evaluated as observations
   arrive, so cluster-or-noise status is provisional until the linkage window
   closes.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from heapq import heappop, heappush

from clustering.core.geo import Hull, distance_m
from clustering.core.models import (
    Cluster,
    Observation,
    StepEffect,
    cluster_id_for,
)
from clustering.core.params import ClusterParams
from clustering.h3_partitioner import H3Index, NeighbourIndex
from clustering.spatial_dedup import CausalOrderError


@dataclass(slots=True)
class ClusterState:
    """Active clustering state: open clusters plus undecided observations."""

    index: NeighbourIndex
    open_clusters: dict[str, Cluster] = field(default_factory=dict)
    pending: dict[str, Observation] = field(default_factory=dict)
    eviction: deque[tuple[datetime, str]] = field(default_factory=deque)
    closure_heap: list[tuple[datetime, str]] = field(default_factory=list)
    last_t: datetime | None = None


def new_state(params: ClusterParams, index: NeighbourIndex | None = None) -> ClusterState:
    return ClusterState(
        index=index if index is not None else H3Index(params.h3_resolution, params.h3_ring)
    )


def step(
    state: ClusterState, observation: Observation, params: ClusterParams
) -> StepEffect:
    """Advance the state by one observation."""
    effect = StepEffect()

    if state.last_t is not None and observation.as_of < state.last_t:
        raise CausalOrderError(
            f"observation {observation.observation_key} at "
            f"{observation.as_of.isoformat()} arrived after {state.last_t.isoformat()}"
        )
    state.last_t = observation.as_of

    _evict_expired(state, observation.as_of, params, effect)
    _close_idle(state, observation.as_of, params, effect)

    nearest_by_cluster: dict[str, float] = {}
    partners: list[tuple[float, Observation]] = []
    for entry in state.index.nearby(observation):
        elapsed = (observation.as_of - entry.observation.as_of).total_seconds()
        if elapsed > params.linkage_gap_seconds:
            continue
        gap = distance_m(
            observation.lat,
            observation.lon,
            entry.observation.lat,
            entry.observation.lon,
        )
        if gap > params.eps_m:
            continue
        if entry.owner is None:
            partners.append((gap, entry.observation))
        else:
            previous = nearest_by_cluster.get(entry.owner)
            if previous is None or gap < previous:
                nearest_by_cluster[entry.owner] = gap

    eligible: list[tuple[float, str]] = []
    for cluster_id, gap in nearest_by_cluster.items():
        cluster = state.open_clusters.get(cluster_id)
        if cluster is None:
            continue  # owner already closed; closed clusters never admit
        extent = cluster.hull.extent_after_adding(observation.lat, observation.lon)
        if extent > params.max_cluster_diameter_m:
            continue  # the diameter guard
        eligible.append((gap, cluster_id))

    if eligible:
        eligible.sort()
        _, cluster_id = eligible[0]
        _admit(state, state.open_clusters[cluster_id], observation, effect)
        return effect

    group = _extent_safe_group(observation, partners, params)
    if len(group) >= params.min_samples:
        _open_cluster(state, observation, group, params, effect)
    else:
        state.pending[observation.observation_key] = observation
        state.index.add(observation, None)
        state.eviction.append((observation.as_of, observation.observation_key))
        effect.pending_added.append(observation)
    return effect


def finalize(
    state: ClusterState, evaluation_time: datetime, params: ClusterParams
) -> StepEffect:
    """Settle everything decidable at the evaluation time.

    Closure and noise both become pure functions of the evaluation time, so a
    replay at the same cutoff reaches the same verdict regardless of which
    observation happened to arrive last.
    """
    effect = StepEffect()

    for cluster_id in list(state.open_clusters):
        cluster = state.open_clusters[cluster_id]
        idle = (evaluation_time - cluster.last_detected).total_seconds()
        if idle > params.inactivity_seconds:
            _close(state, cluster, params, effect)

    for key, observation in list(state.pending.items()):
        age = (evaluation_time - observation.as_of).total_seconds()
        if age > params.linkage_gap_seconds:
            del state.pending[key]
            state.index.pop(key)
            effect.noise.append(observation)
    return effect


def _evict_expired(
    state: ClusterState,
    now: datetime,
    params: ClusterParams,
    effect: StepEffect,
) -> None:
    """Drop index entries past the linkage window, settling pending as noise.

    Amortised O(1) per observation, and the reason memory tracks the active set
    rather than the range length.
    """
    while state.eviction:
        as_of, key = state.eviction[0]
        if (now - as_of).total_seconds() <= params.linkage_gap_seconds:
            break
        state.eviction.popleft()
        entry = state.index.pop(key)
        if entry is None:
            continue
        if entry.owner is None:
            state.pending.pop(key, None)
            effect.noise.append(entry.observation)


def _close_idle(
    state: ClusterState,
    now: datetime,
    params: ClusterParams,
    effect: StepEffect,
) -> None:
    """Close idle clusters before candidate selection, so they cannot be picked."""
    heap = state.closure_heap
    while heap:
        last_detected, cluster_id = heap[0]
        cluster = state.open_clusters.get(cluster_id)
        if cluster is None or cluster.last_detected != last_detected:
            heappop(heap)  # stale entry from an earlier admission
            continue
        if (now - last_detected).total_seconds() > params.inactivity_seconds:
            heappop(heap)
            _close(state, cluster, params, effect)
            continue
        break


def _close(
    state: ClusterState,
    cluster: Cluster,
    params: ClusterParams,
    effect: StepEffect,
) -> None:
    cluster.closed_at = cluster.closes_at(params.inactivity_seconds)
    state.open_clusters.pop(cluster.cluster_id, None)
    effect.closed.append(cluster)


def _admit(
    state: ClusterState,
    cluster: Cluster,
    observation: Observation,
    effect: StepEffect,
) -> None:
    cluster.absorb(observation)
    state.index.add(observation, cluster.cluster_id)
    state.eviction.append((observation.as_of, observation.observation_key))
    effect.admitted.append((cluster.cluster_id, observation))
    heappush(state.closure_heap, (cluster.last_detected, cluster.cluster_id))


def _extent_safe_group(
    observation: Observation,
    partners: list[tuple[float, Observation]],
    params: ClusterParams,
) -> list[Observation]:
    """Largest extent-safe group, added in ascending distance then key order."""
    group = [observation]
    hull = Hull()
    hull.add(observation.lat, observation.lon)
    for _, partner in sorted(partners, key=lambda p: (p[0], p[1].observation_key)):
        if hull.extent_after_adding(partner.lat, partner.lon) > params.max_cluster_diameter_m:
            continue
        hull.add(partner.lat, partner.lon)
        group.append(partner)
    return group


def _open_cluster(
    state: ClusterState,
    trigger: Observation,
    group: Sequence[Observation],
    params: ClusterParams,
    effect: StepEffect,
) -> None:
    ordered = sorted(group, key=lambda o: (o.as_of, o.observation_key))
    seed = ordered[0]
    cluster_id = cluster_id_for(
        algorithm_version=params.algorithm_version,
        params_fingerprint=params.fingerprint(),
        seed_key=seed.observation_key,
    )
    cluster = Cluster(
        cluster_id=cluster_id,
        seed_key=seed.observation_key,
        formed_at=trigger.as_of,
        first_detected=seed.as_of,
        last_detected=seed.as_of,
    )
    for member in ordered:
        cluster.absorb(member)
        effect.admitted.append((cluster_id, member))
        if member.observation_key in state.pending:
            del state.pending[member.observation_key]
            state.index.set_owner(member.observation_key, cluster_id)
        else:
            state.index.add(member, cluster_id)
            state.eviction.append((member.as_of, member.observation_key))

    state.open_clusters[cluster_id] = cluster
    heappush(state.closure_heap, (cluster.last_detected, cluster_id))
    effect.opened.append(cluster)


# --------------------------------------------------------------------------
# In-memory driver and as-of views, used by the causality and fixture proofs
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ClusterSnapshot:
    cluster_id: str
    formed_at: datetime
    first_detected: datetime
    last_detected: datetime
    status: str
    member_keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StateView:
    """Comparable state as known at one evaluation time."""

    evaluation_time: datetime
    settled_horizon: datetime
    clusters: tuple[ClusterSnapshot, ...]
    noise_keys: tuple[str, ...]
    pending_keys: tuple[str, ...]


@dataclass(slots=True)
class ClusteringResult:
    params: ClusterParams
    evaluation_time: datetime
    clusters: dict[str, Cluster]
    members: dict[str, list[Observation]]
    observations: list[Observation]
    noise: list[Observation]
    pending: list[Observation]

    @property
    def detection_count(self) -> int:
        return sum(o.detection_count for o in self.observations)

    def settled_horizon(self, at: datetime | None = None) -> datetime:
        moment = self.evaluation_time if at is None else at
        return moment - timedelta(
            seconds=self.params.linkage_gap_seconds
            + self.params.reconcile_temporal_seconds
        )

    def view_as_of(self, at: datetime) -> StateView:
        """Reconstruct the state as it was knowable at `at`.

        Clusters that became knowable only after `at` are excluded by their
        `formed_at`, rather than being retroactively inserted into history.
        """
        snapshots: list[ClusterSnapshot] = []
        assigned: set[str] = set()
        for cluster_id, cluster in self.clusters.items():
            if cluster.formed_at > at:
                continue
            visible = [m for m in self.members[cluster_id] if m.as_of <= at]
            if not visible:
                continue
            keys = tuple(sorted(m.observation_key for m in visible))
            assigned.update(keys)
            last_detected = max(m.as_of for m in visible)
            idle = (at - last_detected).total_seconds()
            snapshots.append(
                ClusterSnapshot(
                    cluster_id=cluster_id,
                    formed_at=cluster.formed_at,
                    first_detected=min(m.as_of for m in visible),
                    last_detected=last_detected,
                    status="closed"
                    if idle > self.params.inactivity_seconds
                    else "open",
                    member_keys=keys,
                )
            )

        horizon_seconds = self.params.linkage_gap_seconds
        noise: list[str] = []
        pending: list[str] = []
        for observation in self.observations:
            if observation.as_of > at:
                continue
            if observation.observation_key in assigned:
                continue
            if (at - observation.as_of).total_seconds() > horizon_seconds:
                noise.append(observation.observation_key)
            else:
                pending.append(observation.observation_key)

        return StateView(
            evaluation_time=at,
            settled_horizon=self.settled_horizon(at),
            clusters=tuple(sorted(snapshots, key=lambda c: c.cluster_id)),
            noise_keys=tuple(sorted(noise)),
            pending_keys=tuple(sorted(pending)),
        )

    def final_view(self) -> StateView:
        return self.view_as_of(self.evaluation_time)


def run_observations(
    observations: Iterable[Observation],
    params: ClusterParams,
    *,
    evaluation_time: datetime | None = None,
    index: NeighbourIndex | None = None,
) -> ClusteringResult:
    """Cluster an observation stream in memory. Used by proofs and fixtures."""
    state = new_state(params, index)
    clusters: dict[str, Cluster] = {}
    members: dict[str, list[Observation]] = {}
    collected: list[Observation] = []
    noise: list[Observation] = []

    def apply(effect: StepEffect) -> None:
        for cluster in effect.opened:
            clusters[cluster.cluster_id] = cluster
            members.setdefault(cluster.cluster_id, [])
        for cluster_id, observation in effect.admitted:
            members.setdefault(cluster_id, []).append(observation)
        noise.extend(effect.noise)

    latest: datetime | None = None
    for observation in observations:
        collected.append(observation)
        latest = observation.as_of
        apply(step(state, observation, params))

    cutoff = evaluation_time if evaluation_time is not None else latest
    if cutoff is None:
        cutoff = datetime.min
    apply(finalize(state, cutoff, params))

    return ClusteringResult(
        params=params,
        evaluation_time=cutoff,
        clusters=clusters,
        members=members,
        observations=collected,
        noise=noise,
        pending=list(state.pending.values()),
    )
