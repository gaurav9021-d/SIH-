"""PostGIS integration tests.

Skipped cleanly when no database is reachable, so the suite stays runnable without
credentials. Set CLUSTER_TEST_DATABASE_URL to opt in; DATABASE_URL is used as a
fallback.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

from ingestion.core.db import Database
from ingestion.core.settings import DatabaseConfig, load_secrets

from clustering.core.params import ClusterParams, validate
from clustering.engine import cluster_detection_stream, day_bounds
from clustering.fixtures import linear_chain_20km
from clustering.writer import ClusterWriter, ExtentInvariantError, new_run_id

PARAMS = validate(ClusterParams())
DB_CONFIG = DatabaseConfig(
    pool_size=2,
    max_overflow=1,
    pool_timeout_s=10.0,
    pool_recycle_s=1800,
    batch_size=1000,
    operation_max_attempts=1,
    connection_budget=40,
)


def _database_url() -> str | None:
    """Resolve the test database the same way the application does.

    `os.getenv` alone is not enough: DATABASE_URL normally lives in `.env`, which
    only the settings loader reads. Relying on the bare environment variable made
    this suite skip even with a perfectly good database configured.
    """
    override = os.getenv("CLUSTER_TEST_DATABASE_URL")
    if override:
        return override
    try:
        return load_secrets().require_database_url()
    except Exception:
        return None


def _probe(url: str, sql: str, timeout_s: int = 10):
    """Run one statement with a generous connect timeout.

    A remote pooler handshake can take several seconds; too tight a budget makes
    a healthy database look unreachable and skips this suite silently, which is
    worse than failing.
    """
    engine = create_engine(url, connect_args={"connect_timeout": timeout_s})
    try:
        with engine.connect() as connection:
            return connection.execute(text(sql)).scalar_one()
    finally:
        engine.dispose()


def _skip_reason() -> str | None:
    """State precisely why integration coverage is not running, if it is not."""
    candidate = _database_url()
    if candidate is None:
        return "no database URL; set CLUSTER_TEST_DATABASE_URL or DATABASE_URL"
    try:
        _probe(candidate, "SELECT 1")
    except Exception as exc:  # noqa: BLE001 - reported, not handled
        return f"database not reachable ({type(exc).__name__})"
    try:
        ready = _probe(
            candidate, "SELECT to_regclass('clustering.clusters') IS NOT NULL"
        )
    except Exception as exc:  # noqa: BLE001 - reported, not handled
        return f"could not inspect the clustering schema ({type(exc).__name__})"
    if not ready:
        return "clustering schema not migrated; run `python -m clustering migrate`"
    return None


url = _database_url()
SKIP_REASON = _skip_reason()
pytestmark = pytest.mark.skipif(SKIP_REASON is not None, reason=SKIP_REASON or "")


@pytest.fixture
def database() -> Database:
    assert url is not None
    db = Database(url, DB_CONFIG, application_name="ingestion:clustering-tests")
    try:
        yield db
    finally:
        db.close()


def _run(database: Database, *, persist: bool = True):
    detections = linear_chain_20km()
    window_start, window_end = day_bounds(date(2026, 9, 8), date(2026, 9, 9))
    run_id = new_run_id()
    writer = ClusterWriter(database, PARAMS, aoi_name="fixture", run_id=run_id)
    writer.require_schema()
    writer.start_run(
        window_start=window_start,
        window_end=window_end,
        evaluation_time=window_end,
        settled_horizon=window_end - timedelta(days=7),
        products=None,
    )
    summary = cluster_detection_stream(
        detections,
        PARAMS,
        aoi_name="fixture",
        window_start=window_start,
        window_end=window_end,
        evaluation_time=window_end,
        writer=writer if persist else None,
        run_id=run_id,
    )
    writer.finish_run(
        "succeeded",
        {
            "detections_read": summary.detections_read,
            "observations": summary.observations,
            "clusters_open": summary.clusters_open,
            "clusters_closed": summary.clusters_closed,
            "noise_count": summary.noise_count,
            "pending_count": summary.pending_count,
            "max_extent_m": summary.max_extent_m,
            "duration_s": summary.duration_s,
        },
    )
    return summary


def _count(database: Database, sql: str, **params) -> int:
    with database.engine.connect() as connection:
        return int(connection.execute(text(sql), params).scalar_one())


def test_schema_is_migrated(database: Database) -> None:
    writer = ClusterWriter(database, PARAMS, aoi_name="fixture", run_id=new_run_id())
    writer.require_schema()


def test_persisting_twice_is_idempotent(database: Database) -> None:
    first = _run(database)
    before = _count(
        database,
        "SELECT count(*) FROM clustering.clusters WHERE params_fingerprint = :fp",
        fp=PARAMS.fingerprint(),
    )
    members_before = _count(database, "SELECT count(*) FROM clustering.cluster_members")

    second = _run(database)
    after = _count(
        database,
        "SELECT count(*) FROM clustering.clusters WHERE params_fingerprint = :fp",
        fp=PARAMS.fingerprint(),
    )
    members_after = _count(database, "SELECT count(*) FROM clustering.cluster_members")

    assert first.cluster_count == second.cluster_count
    assert before == after
    assert members_before == members_after


def test_stored_extent_never_exceeds_the_limit(database: Database) -> None:
    _run(database)
    violations = _count(
        database,
        "SELECT count(*) FROM clustering.clusters WHERE extent_m > :limit",
        limit=PARAMS.max_cluster_diameter_m,
    )
    assert violations == 0


def test_clusters_as_of_matches_a_fresh_run_at_that_time(database: Database) -> None:
    _run(database)
    detections = linear_chain_20km()
    cutoff = detections[len(detections) // 2].as_of

    with database.engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT cluster_id, observation_count "
                "FROM clustering.clusters_as_of(:at, :fp) ORDER BY cluster_id"
            ),
            {"at": cutoff, "fp": PARAMS.fingerprint()},
        ).all()
    stored = {row.cluster_id: int(row.observation_count) for row in rows}

    truncated = [d for d in detections if d.as_of <= cutoff]
    window_start, window_end = day_bounds(date(2026, 9, 8), date(2026, 9, 9))
    fresh = cluster_detection_stream(
        truncated,
        PARAMS,
        aoi_name="fixture",
        window_start=window_start,
        window_end=window_end,
        evaluation_time=cutoff,
    )

    assert sum(stored.values()) == fresh.observations - fresh.pending_count


def test_extent_invariant_is_enforced_before_writing(database: Database) -> None:
    """A cluster above the limit must abort the run, not reach the table."""
    detections = linear_chain_20km()
    window_start, window_end = day_bounds(date(2026, 9, 8), date(2026, 9, 9))
    loose = validate(ClusterParams(max_cluster_diameter_m=1_000_000.0))

    run_id = new_run_id()
    writer = ClusterWriter(database, loose, aoi_name="fixture", run_id=run_id)
    writer.require_schema()
    writer.start_run(
        window_start=window_start,
        window_end=window_end,
        evaluation_time=window_end,
        settled_horizon=window_end - timedelta(days=7),
        products=None,
    )
    summary = cluster_detection_stream(
        detections,
        loose,
        aoi_name="fixture",
        window_start=window_start,
        window_end=window_end,
        evaluation_time=window_end,
        run_id=run_id,
    )
    assert summary.max_extent_m > 19_000.0

    # Re-verify that runaway cluster against the real limit.
    strict_writer = ClusterWriter(database, PARAMS, aoi_name="fixture", run_id=run_id)
    from clustering.core.geo import Hull
    from clustering.core.models import Cluster

    hull = Hull()
    hull.add(30.5, 75.0)
    hull.add(30.5, 75.3)
    oversized = Cluster(
        cluster_id="c-oversized",
        seed_key="o-seed",
        formed_at=datetime(2026, 9, 8, tzinfo=UTC),
        first_detected=datetime(2026, 9, 8, tzinfo=UTC),
        last_detected=datetime(2026, 9, 8, tzinfo=UTC),
        hull=hull,
    )
    with pytest.raises(ExtentInvariantError):
        strict_writer.write_clusters([(oversized, [], False)])

    assert (
        _count(
            database,
            "SELECT count(*) FROM clustering.clusters WHERE cluster_id = 'c-oversized'",
        )
        == 0
    )


def test_clustering_does_not_write_to_ingestion_tables(database: Database) -> None:
    before = _count(database, "SELECT count(*) FROM ingestion.fire_detections")
    _run(database)
    after = _count(database, "SELECT count(*) FROM ingestion.fire_detections")
    assert before == after
