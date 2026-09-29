"""Engine driver, run summary and distributions."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from clustering.core.params import ClusterParams, validate
from clustering.core.stats import Distribution, SizeHistogram, percentile
from clustering.engine import cluster_detection_stream, day_bounds, settled_horizon_for
from clustering.fixtures import dense_savanna_grid, linear_chain_20km
from clustering.settings import load_config

PARAMS = validate(ClusterParams())


def _summary(detections):
    window_start = min(d.as_of for d in detections)
    window_end = max(d.as_of for d in detections)
    return cluster_detection_stream(
        detections,
        PARAMS,
        aoi_name="fixture",
        window_start=window_start,
        window_end=window_end,
        evaluation_time=window_end,
    )


def test_inclusive_dates_become_a_half_open_utc_interval() -> None:
    start, end = day_bounds(date(2025, 11, 1), date(2025, 11, 20))

    assert start == datetime(2025, 11, 1, tzinfo=UTC)
    assert end == datetime(2025, 11, 21, tzinfo=UTC)
    assert (end - start).days == 20, "November 20 must be inside the window"


def test_settled_horizon_trails_by_linkage_plus_reconciliation() -> None:
    now = datetime(2025, 11, 21, tzinfo=UTC)
    horizon = settled_horizon_for(now, PARAMS)

    expected_seconds = PARAMS.linkage_gap_seconds + PARAMS.reconcile_temporal_seconds
    assert (now - horizon).total_seconds() == pytest.approx(expected_seconds)


def test_summary_accounts_for_every_observation() -> None:
    summary = _summary(dense_savanna_grid())

    assert summary.detections_read == 1369
    assert summary.observations == 1369
    assert summary.cluster_count == summary.clusters_open + summary.clusters_closed
    assert summary.max_extent_m <= PARAMS.max_cluster_diameter_m
    assert 0.0 <= summary.noise_fraction <= 1.0


def test_summary_reports_distributions() -> None:
    summary = _summary(linear_chain_20km())
    payload = summary.as_dict()

    assert payload["cluster_count"] >= 10
    assert payload["size_distribution"]["count"] == summary.cluster_count
    assert payload["extent_distribution"]["maximum"] <= 2000.0
    assert payload["max_cluster_diameter_m"] == 2000.0
    assert sum(payload["size_histogram"].values()) == summary.cluster_count
    assert payload["window_end_exclusive"] > payload["window_start"]


def test_preview_does_not_persist() -> None:
    summary = _summary(linear_chain_20km())
    assert summary.persisted is False


def test_percentiles_and_histogram() -> None:
    values = [float(v) for v in range(1, 101)]
    assert percentile(values, 0.5) == pytest.approx(50.0)
    assert percentile(values, 0.99) == pytest.approx(99.0)
    assert percentile([], 0.5) == 0.0

    distribution = Distribution.of(values)
    assert distribution.minimum == 1.0
    assert distribution.maximum == 100.0
    assert distribution.mean == pytest.approx(50.5)

    histogram = SizeHistogram.of([1, 2, 3, 7, 30, 500])
    assert histogram.as_dict()["1"] == 1
    assert histogram.as_dict()[">400"] == 1


def test_shipped_config_is_valid_and_defaults_to_india() -> None:
    config = load_config()

    assert config.default_aoi == "india"
    assert config.get_aoi().bounds == (68.0, 6.5, 97.5, 37.5)
    assert config.params.eps_m == 750.0
    assert config.params.max_cluster_diameter_m == 2000.0
    assert {s.name for s in config.scenarios} == {
        "india_stubble_nov2025",
        "india_sparse_sep8_2026",
        "savanna_dense_sep8_2026",
        "chain_runaway_guard",
    }


def test_shipped_scenarios_cover_both_regimes() -> None:
    config = load_config()
    by_name = {s.name: s for s in config.scenarios}

    dense = by_name["india_stubble_nov2025"]
    assert dense.source == "store"
    assert dense.start == date(2025, 11, 1)
    assert dense.end == date(2025, 11, 20)

    sparse = by_name["india_sparse_sep8_2026"]
    assert sparse.start == date(2026, 9, 8) == sparse.end

    savanna = by_name["savanna_dense_sep8_2026"]
    assert savanna.source == "fixture"
    assert savanna.fixture == "dense_savanna_grid"

    for scenario in config.scenarios:
        assert scenario.expect["max_extent_m"] == 2000.0
