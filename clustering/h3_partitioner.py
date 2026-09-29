"""Neighbour index over observations inside the linkage window.

This is an index, not a work-splitting scheme. Sharding the run by cell and
clustering shards independently would change memberships at cell boundaries, so
partitioning here must never alter results. `BruteForceIndex` exists to prove
that: the two implementations must produce identical clusters.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Protocol

import h3

from clustering.core.models import Observation


@dataclass(slots=True)
class Entry:
    observation: Observation
    owner: str | None  # cluster id, or None while pending


class NeighbourIndex(Protocol):
    def add(self, observation: Observation, owner: str | None) -> None: ...

    def set_owner(self, observation_key: str, owner: str | None) -> None: ...

    def pop(self, observation_key: str) -> Entry | None: ...

    def nearby(self, observation: Observation) -> Iterator[Entry]: ...

    def __len__(self) -> int: ...


class H3Index:
    """Bucket observations by H3 cell and search the cell plus a ring halo."""

    __slots__ = ("_resolution", "_ring", "_cells", "_located")

    def __init__(self, resolution: int, ring: int) -> None:
        self._resolution = resolution
        self._ring = ring
        self._cells: dict[str, dict[str, Entry]] = {}
        self._located: dict[str, str] = {}

    def _cell_of(self, observation: Observation) -> str:
        return h3.latlng_to_cell(observation.lat, observation.lon, self._resolution)

    def add(self, observation: Observation, owner: str | None) -> None:
        cell = self._cell_of(observation)
        self._cells.setdefault(cell, {})[observation.observation_key] = Entry(
            observation, owner
        )
        self._located[observation.observation_key] = cell

    def set_owner(self, observation_key: str, owner: str | None) -> None:
        cell = self._located.get(observation_key)
        if cell is None:
            return
        entry = self._cells[cell].get(observation_key)
        if entry is not None:
            entry.owner = owner

    def pop(self, observation_key: str) -> Entry | None:
        cell = self._located.pop(observation_key, None)
        if cell is None:
            return None
        bucket = self._cells.get(cell)
        if bucket is None:
            return None
        entry = bucket.pop(observation_key, None)
        if not bucket:
            del self._cells[cell]
        return entry

    def nearby(self, observation: Observation) -> Iterator[Entry]:
        origin = self._cell_of(observation)
        for cell in h3.grid_disk(origin, self._ring):
            bucket = self._cells.get(cell)
            if not bucket:
                continue
            yield from bucket.values()

    def __len__(self) -> int:
        return len(self._located)


class BruteForceIndex:
    """Exhaustive reference index, for equivalence testing only."""

    __slots__ = ("_entries",)

    def __init__(self) -> None:
        self._entries: dict[str, Entry] = {}

    def add(self, observation: Observation, owner: str | None) -> None:
        self._entries[observation.observation_key] = Entry(observation, owner)

    def set_owner(self, observation_key: str, owner: str | None) -> None:
        entry = self._entries.get(observation_key)
        if entry is not None:
            entry.owner = owner

    def pop(self, observation_key: str) -> Entry | None:
        return self._entries.pop(observation_key, None)

    def nearby(self, observation: Observation) -> Iterator[Entry]:
        yield from list(self._entries.values())

    def __len__(self) -> int:
        return len(self._entries)
