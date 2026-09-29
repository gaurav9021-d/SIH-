"""Cross-satellite reconciliation."""

from __future__ import annotations

from dataclasses import replace

import pytest

from clustering.core.params import ClusterParams, validate
from clustering.spatial_dedup import reconcile
from clustering.tests.helpers import detection, observations_from

PARAMS = validate(ClusterParams())


def test_two_satellites_seeing_one_fire_collapse_to_one_observation() -> None:
    stream = [
        detection(1, minutes=0, product="VIIRS_SNPP_SP", satellite="N"),
        detection(2, minutes=6, north_m=120, product="MODIS_SP", satellite="Terra"),
    ]

    observations = observations_from(stream, PARAMS)

    assert len(observations) == 1
    only = observations[0]
    assert only.detection_count == 2
    assert only.products == ("MODIS_SP", "VIIRS_SNPP_SP")
    assert only.satellites == ("N", "Terra")


def test_nrt_and_standard_processing_of_one_overpass_collapse() -> None:
    """Same satellite, different product: the same overpass reprocessed."""
    stream = [
        detection(1, minutes=0, product="VIIRS_SNPP_NRT", satellite="N"),
        detection(2, minutes=0, north_m=5, product="VIIRS_SNPP_SP", satellite="N"),
    ]

    observations = observations_from(stream, PARAMS)

    assert len(observations) == 1
    assert observations[0].detection_count == 2


def test_adjacent_pixels_from_one_sensor_stay_separate() -> None:
    """Within-sensor spatial structure is real fire extent, not overlap."""
    stream = [
        detection(1, minutes=0, product="VIIRS_SNPP_SP", satellite="N"),
        detection(2, minutes=0, east_m=370, product="VIIRS_SNPP_SP", satellite="N"),
    ]

    observations = observations_from(stream, PARAMS)

    assert len(observations) == 2


def test_detections_beyond_the_spatial_tolerance_stay_separate() -> None:
    stream = [
        detection(1, minutes=0, product="VIIRS_SNPP_SP", satellite="N"),
        detection(2, minutes=3, east_m=600, product="MODIS_SP", satellite="Terra"),
    ]

    assert len(observations_from(stream, PARAMS)) == 2


def test_detections_beyond_the_temporal_tolerance_stay_separate() -> None:
    stream = [
        detection(1, minutes=0, product="VIIRS_SNPP_SP", satellite="N"),
        detection(2, minutes=40, north_m=50, product="MODIS_SP", satellite="Terra"),
    ]

    assert len(observations_from(stream, PARAMS)) == 2


def test_observations_are_emitted_in_non_decreasing_anchor_time() -> None:
    stream = [
        detection(index, minutes=index * 7, east_m=index * 5_000)
        for index in range(12)
    ]

    observations = observations_from(stream, PARAMS)

    times = [o.as_of for o in observations]
    assert times == sorted(times)


def test_reconciliation_is_deterministic() -> None:
    stream = [
        detection(1, minutes=0, product="VIIRS_SNPP_SP", satellite="N"),
        detection(2, minutes=2, north_m=100, product="MODIS_SP", satellite="Terra"),
        detection(3, minutes=4, north_m=200, product="VIIRS_NOAA20_SP", satellite="N20"),
    ]

    first = observations_from(stream, PARAMS)
    second = observations_from(stream, PARAMS)

    assert [o.observation_key for o in first] == [o.observation_key for o in second]
    assert [o.frp for o in first] == [o.frp for o in second]


def test_observation_position_is_the_anchor_not_a_moving_centroid() -> None:
    anchor = detection(1, minutes=0, product="VIIRS_SNPP_SP", satellite="N")
    stream = [
        anchor,
        detection(2, minutes=5, north_m=300, product="MODIS_SP", satellite="Terra"),
    ]

    observation = observations_from(stream, PARAMS)[0]

    assert observation.lat == anchor.lat
    assert observation.lon == anchor.lon
    assert observation.as_of == anchor.as_of


def test_frp_is_averaged_over_reconciled_detections() -> None:
    stream = [
        detection(1, minutes=0, frp=10.0, product="VIIRS_SNPP_SP", satellite="N"),
        detection(2, minutes=4, frp=30.0, north_m=50, product="MODIS_SP", satellite="Terra"),
    ]

    assert observations_from(stream, PARAMS)[0].frp == pytest.approx(20.0)


def test_disabling_reconciliation_keeps_every_detection() -> None:
    params = validate(replace(PARAMS, reconcile_enabled=False))
    stream = [
        detection(1, minutes=0, product="VIIRS_SNPP_SP", satellite="N"),
        detection(2, minutes=2, north_m=50, product="MODIS_SP", satellite="Terra"),
    ]

    observations = list(reconcile(stream, params))

    assert len(observations) == 2
    assert all(o.detection_count == 1 for o in observations)
