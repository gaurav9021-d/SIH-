"""Size and extent distributions for run summaries."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any

_SIZE_BUCKETS = ((1, 1), (2, 2), (3, 5), (6, 10), (11, 25), (26, 50), (51, 100), (101, 400))


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile: rank = ceil(fraction * n), 1-indexed."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if fraction <= 0.0:
        return ordered[0]
    if fraction >= 1.0:
        return ordered[-1]
    rank = math.ceil(fraction * len(ordered))
    index = min(len(ordered) - 1, max(0, rank - 1))
    return ordered[index]


@dataclass(slots=True)
class Distribution:
    count: int = 0
    minimum: float = 0.0
    median: float = 0.0
    p90: float = 0.0
    p99: float = 0.0
    maximum: float = 0.0
    mean: float = 0.0

    @classmethod
    def of(cls, values: list[float]) -> "Distribution":
        if not values:
            return cls()
        return cls(
            count=len(values),
            minimum=min(values),
            median=percentile(values, 0.5),
            p90=percentile(values, 0.9),
            p99=percentile(values, 0.99),
            maximum=max(values),
            mean=sum(values) / len(values),
        )

    def as_dict(self) -> dict[str, Any]:
        return {key: round(value, 4) if isinstance(value, float) else value
                for key, value in asdict(self).items()}


@dataclass(slots=True)
class SizeHistogram:
    buckets: list[tuple[str, int]] = field(default_factory=list)

    @classmethod
    def of(cls, sizes: list[int]) -> "SizeHistogram":
        buckets: list[tuple[str, int]] = []
        for low, high in _SIZE_BUCKETS:
            label = str(low) if low == high else f"{low}-{high}"
            buckets.append((label, sum(1 for s in sizes if low <= s <= high)))
        overflow = sum(1 for s in sizes if s > _SIZE_BUCKETS[-1][1])
        buckets.append((f">{_SIZE_BUCKETS[-1][1]}", overflow))
        return cls(buckets=buckets)

    def as_dict(self) -> dict[str, int]:
        return dict(self.buckets)

    def render(self, width: int = 40) -> list[str]:
        peak = max((count for _, count in self.buckets), default=0)
        lines: list[str] = []
        for label, count in self.buckets:
            if count == 0 and peak == 0:
                bar = ""
            else:
                bar = "#" * max(0, round(width * count / peak)) if peak else ""
            lines.append(f"    {label:>6} | {count:>7} {bar}")
        return lines
