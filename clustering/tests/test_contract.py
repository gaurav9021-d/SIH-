"""Contract conformance and the FIRMS feature formulas."""

from __future__ import annotations

import math
from datetime import timedelta

import pytest

from clustering.contract import (
    DEFERRED_FEATURE_BLOCKS,
    REQUIRED_TOP_LEVEL,
    to_contract,
)
from clustering.core.params import ClusterParams, validate
from clustering.dbscan import run_observations
from clustering.fixtures import linear_chain_20km
from clustering.tests.helpers import BASE_TIME, cluster_detections, observation

PARAMS = validate(ClusterParams())


def _single_cluster(observations):
    result = run_observations(observations, PARAMS)
    assert len(result.clusters) == 1
    cluster_id = next(iter(result.clusters))
    return result.clusters[cluster_id]


def test_contract_has_every_required_field() -> None:
    result = cluster_detections(linear_chain_20km(), PARAMS)
    cluster = next(iter(result.clusters.values()))

    payload = to_contract(cluster, PARAMS)

    for field in REQUIRED_TOP_LEVEL:
        assert field in payload
    assert set(payload["centroid"]) == {"lat", "lon"}
    assert set(payload["bounding_box"]) == {
        "min_lat",
        "min_lon",
        "max_lat",
        "max_lon",
    }


def test_downstream_blocks_are_null_not_fabricated() -> None:
    result = cluster_detections(linear_chain_20km(), PARAMS)
    payload = to_contract(next(iter(result.clusters.values())), PARAMS)

    for block in DEFERRED_FEATURE_BLOCKS:
        assert payload["features"][block] is None
    assert payload["classification"] is None
    assert payload["anomaly"] is None


def test_firms_block_is_populated() -> None:
    result = cluster_detections(linear_chain_20km(), PARAMS)
    firms = to_contract(next(iter(result.clusters.values())), PARAMS)["features"]["firms"]

    assert set(firms) == {
        "persistence_days",
        "frp_mean",
        "frp_var",
        "day_night_ratio",
        "as_of",
    }
    assert firms["as_of"].endswith("Z")


def test_algorithm_block_records_the_deviations() -> None:
    result = cluster_detections(linear_chain_20km(), PARAMS)
    cluster = next(iter(result.clusters.values()))

    algorithm = to_contract(cluster, PARAMS)["algorithm"]

    assert algorithm["diameter_bounded"] is True
    assert algorithm["merging"] == "disabled"
    assert algorithm["extent_m"] <= PARAMS.max_cluster_diameter_m
    assert algorithm["params_fingerprint"] == PARAMS.fingerprint()


def test_features_as_of_is_the_latest_acquisition_not_the_write_time() -> None:
    cluster = _single_cluster(
        [
            observation(1, minutes=0),
            observation(2, minutes=30, north_m=100),
            observation(3, minutes=90, north_m=150),
        ]
    )

    payload = to_contract(cluster, PARAMS)
    assert payload["features"]["firms"]["as_of"] == payload["last_detected"]
    assert cluster.last_detected == BASE_TIME + timedelta(minutes=90)


def test_persistence_days_spans_first_to_last_detection() -> None:
    cluster = _single_cluster(
        [observation(1, minutes=0), observation(2, minutes=60 * 24, north_m=100)]
    )
    assert cluster.persistence_days == pytest.approx(1.0)


def test_single_observation_cluster_has_zero_persistence_and_variance() -> None:
    params = validate(ClusterParams(min_samples=1))
    result = run_observations([observation(1, minutes=0)], params)
    cluster = next(iter(result.clusters.values()))

    assert cluster.observation_count == 1
    assert cluster.persistence_days == 0.0
    assert cluster.frp_var == 0.0
    assert cluster.frp_mean == pytest.approx(10.0)


def test_frp_mean_and_population_variance() -> None:
    cluster = _single_cluster(
        [
            observation(1, minutes=0, frp=10.0),
            observation(2, minutes=30, north_m=100, frp=20.0),
            observation(3, minutes=60, north_m=150, frp=30.0),
        ]
    )

    assert cluster.frp_mean == pytest.approx(20.0)
    # Population variance (ddof=0): mean of squared deviations.
    assert cluster.frp_var == pytest.approx(200.0 / 3.0)


def test_day_night_ratio_is_the_day_fraction() -> None:
    cluster = _single_cluster(
        [
            observation(1, minutes=0, day_night="D"),
            observation(2, minutes=20, north_m=80, day_night="N"),
            observation(3, minutes=40, north_m=120, day_night="N"),
            observation(4, minutes=60, north_m=160, day_night="N"),
        ]
    )

    assert cluster.day_night_ratio == pytest.approx(0.25)


def test_day_night_ratio_bounds() -> None:
    all_day = _single_cluster(
        [observation(1, minutes=0), observation(2, minutes=20, north_m=80)]
    )
    assert all_day.day_night_ratio == 1.0

    all_night = _single_cluster(
        [
            observation(1, minutes=0, day_night="N"),
            observation(2, minutes=20, north_m=80, day_night="N"),
        ]
    )
    assert all_night.day_night_ratio == 0.0


def test_bounding_box_contains_the_centroid() -> None:
    result = cluster_detections(linear_chain_20km(), PARAMS)
    for cluster in result.clusters.values():
        payload = to_contract(cluster, PARAMS)
        box, centroid = payload["bounding_box"], payload["centroid"]
        assert box["min_lat"] - 1e-6 <= centroid["lat"] <= box["max_lat"] + 1e-6
        assert box["min_lon"] - 1e-6 <= centroid["lon"] <= box["max_lon"] + 1e-6


def test_detection_count_and_observation_count_are_distinct_fields() -> None:
    """detection_count stays literal; the reconciled figure is separate."""
    result = cluster_detections(linear_chain_20km(), PARAMS)
    payload = to_contract(next(iter(result.clusters.values())), PARAMS)

    assert payload["detection_count"] >= payload["observation_count"]
    assert math.isfinite(payload["features"]["firms"]["frp_mean"])
