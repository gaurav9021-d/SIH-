"""Parameter validation, including the epsilon ceiling and the H3 halo check."""

from __future__ import annotations

from dataclasses import replace

import pytest

from ingestion.core.errors import ConfigurationError

from clustering.core.params import (
    EPS_CEILING_M,
    ClusterParams,
    from_mapping,
    min_cell_inradius_m,
    validate,
)


def test_defaults_are_valid() -> None:
    params = validate(ClusterParams())
    assert params.eps_m == 750.0
    assert params.min_samples == 2
    assert params.inactivity_days == 7.0
    assert params.max_cluster_diameter_m == 2000.0
    assert params.linkage_gap_days == 7.0


@pytest.mark.parametrize("eps_m", [750.01, 800.0, 1500.0, 5000.0])
def test_epsilon_above_the_ceiling_is_refused(eps_m: float) -> None:
    with pytest.raises(ConfigurationError, match="ceiling"):
        validate(replace(ClusterParams(), eps_m=eps_m))


@pytest.mark.parametrize("eps_m", [0.0, -1.0])
def test_non_positive_epsilon_is_refused(eps_m: float) -> None:
    with pytest.raises(ConfigurationError, match="greater than zero"):
        validate(replace(ClusterParams(), eps_m=eps_m))


def test_epsilon_at_the_ceiling_is_allowed() -> None:
    assert validate(replace(ClusterParams(), eps_m=EPS_CEILING_M)).eps_m == EPS_CEILING_M


def test_diameter_below_epsilon_is_refused() -> None:
    with pytest.raises(ConfigurationError, match="at least eps_m"):
        validate(replace(ClusterParams(), max_cluster_diameter_m=500.0))


def test_reconcile_tolerance_above_epsilon_is_refused() -> None:
    with pytest.raises(ConfigurationError, match="reconcile_spatial_m"):
        validate(replace(ClusterParams(), reconcile_spatial_m=760.0))


@pytest.mark.parametrize("min_samples", [0, -1])
def test_min_samples_below_one_is_refused(min_samples: int) -> None:
    with pytest.raises(ConfigurationError, match="min_samples"):
        validate(replace(ClusterParams(), min_samples=min_samples))


def test_measured_halo_admits_resolution_six_and_refuses_resolution_eight() -> None:
    """Resolution 6 clears 750 m comfortably; resolution 8 cannot."""
    assert min_cell_inradius_m(6) > 750.0
    assert min_cell_inradius_m(8) < 750.0

    validate(replace(ClusterParams(), h3_resolution=6))
    with pytest.raises(ConfigurationError, match="halo"):
        validate(replace(ClusterParams(), h3_resolution=8))


def test_resolution_seven_passes_but_is_marginal() -> None:
    inradius = min_cell_inradius_m(7)
    assert 750.0 < inradius < 1100.0
    validate(replace(ClusterParams(), h3_resolution=7))


def test_a_wider_ring_rescues_a_fine_resolution() -> None:
    validate(replace(ClusterParams(), h3_resolution=8, h3_ring=3))


def test_fingerprint_is_stable_and_sensitive() -> None:
    base = validate(ClusterParams())
    assert base.fingerprint() == validate(ClusterParams()).fingerprint()
    assert base.fingerprint() != validate(replace(base, eps_m=700.0)).fingerprint()
    assert base.fingerprint() != validate(replace(base, min_samples=3)).fingerprint()


def test_linkage_gap_default_is_materialised_in_the_fingerprint() -> None:
    implicit = validate(ClusterParams(max_linkage_gap_days=None))
    explicit = validate(replace(ClusterParams(), max_linkage_gap_days=7.0))
    assert implicit.fingerprint() == explicit.fingerprint()


def test_unknown_parameter_is_refused() -> None:
    with pytest.raises(ConfigurationError, match="Unknown clustering parameters"):
        from_mapping({"eps_m": 700.0, "epsilon": 700.0})


def test_metadata_records_the_deviations() -> None:
    metadata = validate(ClusterParams()).as_metadata()
    assert metadata["diameter_bounded"] is True
    assert metadata["merging"] == "disabled"
    assert metadata["params_fingerprint"]
