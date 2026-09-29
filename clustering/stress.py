"""Stress scenarios that validate the parameters against known extremes.

Two scenarios read the live store; two are deterministic fixtures that run with no
database and no credentials, which is what makes the extent guard provable in CI.
A store-backed scenario with no data reports `skipped`, never a false pass.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ingestion.core.db import Database
from ingestion.core.errors import IngestionError

from clustering.core.params import ClusterParams
from clustering.engine import RunSummary, cluster_detection_stream, day_bounds, run_from_store
from clustering.fixtures import SYNTHETIC, build
from clustering.settings import ClusteringConfig, Scenario
from clustering.writer import SchemaNotReadyError


@dataclass(slots=True)
class ScenarioResult:
    name: str
    description: str
    source: str
    verdict: str  # passed | failed | skipped
    reason: str | None = None
    synthetic: bool = False
    summary: RunSummary | None = None
    failures: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "source": self.source,
            "verdict": self.verdict,
            "synthetic": self.synthetic,
        }
        if self.reason:
            payload["reason"] = self.reason
        if self.failures:
            payload["failures"] = self.failures
        if self.summary is not None:
            payload["summary"] = self.summary.as_dict()
        return payload


def run_scenario(
    scenario: Scenario,
    config: ClusteringConfig,
    *,
    params: ClusterParams | None = None,
    database: Database | None = None,
    persist: bool = False,
) -> ScenarioResult:
    effective = params or config.params

    if scenario.source == "fixture":
        return _run_fixture(scenario, effective)
    return _run_store(scenario, config, effective, database, persist)


def _run_fixture(scenario: Scenario, params: ClusterParams) -> ScenarioResult:
    detections = build(str(scenario.fixture))
    if not detections:
        return ScenarioResult(
            name=scenario.name,
            description=scenario.description,
            source=scenario.source,
            verdict="skipped",
            reason=f"fixture {scenario.fixture!r} produced no detections",
            synthetic=True,
        )

    window_start = min(d.as_of for d in detections)
    window_end = max(d.as_of for d in detections)
    summary = cluster_detection_stream(
        detections,
        params,
        aoi_name=f"fixture:{scenario.fixture}",
        window_start=window_start,
        window_end=window_end,
        evaluation_time=window_end,
    )
    failures = _evaluate(scenario.expect, summary)
    return ScenarioResult(
        name=scenario.name,
        description=scenario.description,
        source=scenario.source,
        verdict="failed" if failures else "passed",
        synthetic=SYNTHETIC,
        summary=summary,
        failures=failures,
    )


def _run_store(
    scenario: Scenario,
    config: ClusteringConfig,
    params: ClusterParams,
    database: Database | None,
    persist: bool,
) -> ScenarioResult:
    if database is None:
        return ScenarioResult(
            name=scenario.name,
            description=scenario.description,
            source=scenario.source,
            verdict="skipped",
            reason="no database connection available for a store-backed scenario",
        )
    if scenario.start is None or scenario.end is None:
        return ScenarioResult(
            name=scenario.name,
            description=scenario.description,
            source=scenario.source,
            verdict="skipped",
            reason="scenario has no date range",
        )

    aoi = config.get_aoi(scenario.aoi)
    from clustering.reader import count_detections

    window_start, window_end = day_bounds(scenario.start, scenario.end)

    # A store problem must not fail the harness: the fixture scenarios still carry
    # the extent proof, and an unreachable or unmigrated database is an
    # environment gap rather than an algorithm defect.
    try:
        available = count_detections(
            database,
            aoi=aoi,
            window_start=window_start,
            window_end=window_end,
            evaluation_time=window_end,
        )
    except IngestionError as exc:
        return ScenarioResult(
            name=scenario.name,
            description=scenario.description,
            source=scenario.source,
            verdict="skipped",
            reason=f"store unavailable: {type(exc).__name__}",
        )

    if available == 0:
        return ScenarioResult(
            name=scenario.name,
            description=scenario.description,
            source=scenario.source,
            verdict="skipped",
            reason=(
                f"no detections in {aoi.name} between {scenario.start} and "
                f"{scenario.end}; ingest that range first"
            ),
        )

    try:
        summary = run_from_store(
            database,
            params,
            aoi=aoi,
            start=scenario.start,
            end_inclusive=scenario.end,
            batch_size=config.runtime.batch_size,
            flush_every=config.runtime.flush_every,
            persist=persist,
        )
    except SchemaNotReadyError as exc:
        return ScenarioResult(
            name=scenario.name,
            description=scenario.description,
            source=scenario.source,
            verdict="skipped",
            reason=str(exc),
        )
    failures = _evaluate(scenario.expect, summary)
    return ScenarioResult(
        name=scenario.name,
        description=scenario.description,
        source=scenario.source,
        verdict="failed" if failures else "passed",
        summary=summary,
        failures=failures,
    )


def _evaluate(expect: dict[str, Any], summary: RunSummary) -> list[str]:
    failures: list[str] = []

    limit = expect.get("max_extent_m")
    if limit is not None and summary.max_extent_m > float(limit):
        failures.append(
            f"max cluster extent {summary.max_extent_m:.1f} m exceeds {float(limit):.1f} m"
        )

    largest = expect.get("max_observations_per_cluster")
    if largest is not None and summary.sizes and max(summary.sizes) > int(largest):
        failures.append(
            f"largest cluster holds {max(summary.sizes)} observations, above {largest}"
        )

    minimum = expect.get("min_clusters")
    if minimum is not None and summary.cluster_count < int(minimum):
        failures.append(
            f"only {summary.cluster_count} clusters formed, below {minimum}; a low "
            "count under dense input is the signature of runaway merging"
        )

    mean_cap = expect.get("max_mean_observations_per_cluster")
    if mean_cap is not None and summary.sizes:
        mean = sum(summary.sizes) / len(summary.sizes)
        if mean > float(mean_cap):
            failures.append(
                f"mean cluster size {mean:.1f} observations exceeds {float(mean_cap)}"
            )

    return failures


def run_all(
    config: ClusteringConfig,
    *,
    params: ClusterParams | None = None,
    database: Database | None = None,
    only: str | None = None,
    persist: bool = False,
) -> list[ScenarioResult]:
    scenarios = [config.scenario(only)] if only else list(config.scenarios)

    # Fixture scenarios first: they need no database, and they carry the extent
    # proof, so they must report even when the store is unreachable.
    ordered = sorted(scenarios, key=lambda s: s.source != "fixture")
    results = [
        run_scenario(
            scenario, config, params=params, database=database, persist=persist
        )
        for scenario in ordered
    ]
    order = {scenario.name: index for index, scenario in enumerate(scenarios)}
    return sorted(results, key=lambda r: order[r.name])


def utcnow() -> datetime:
    return datetime.now(UTC)
