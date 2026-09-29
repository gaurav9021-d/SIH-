"""Pooled PostGIS access, resumable state, leases, and shared FIRMS quota."""

from __future__ import annotations

import hashlib
import logging
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, time as datetime_time, timedelta
from typing import Any, TypeVar

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ingestion.core.errors import (
    ConfigurationError,
    DatabaseUnavailableError,
    LeaseLostError,
)
from ingestion.core.logging import get_logger, log_event, redact
from ingestion.core.settings import DatabaseConfig, FirmsConfig
from ingestion.firms.models import BackfillChunk, FirmsDetection

LOGGER = get_logger("db")
T = TypeVar("T")
EXPECTED_REVISION = "20260929_0001"

# Minimums rather than exact matches: the schema needs nothing newer than
# PostgreSQL 15 (generated stored columns, partitioned unique constraints,
# identity columns) and nothing newer than PostGIS 3.3. Any later release is
# fine. Local development runs 15 and 3.3 via docker compose; managed Postgres
# such as Supabase currently serves 17 with PostGIS 3.3.7 and upgrades both on
# its own schedule, so pinning an exact version breaks without warning.
MINIMUM_SERVER_VERSION_NUM = 150000
MINIMUM_POSTGIS_VERSION = (3, 3)


def parse_version(raw: str) -> tuple[int, ...]:
    """Leading numeric components of a version string, e.g. '3.3.7' -> (3, 3, 7).

    Stops at the first non-numeric component so suffixes such as '3.4.0dev' or
    '3.5.0-alpha1' compare on their release numbers.
    """
    parts: list[int] = []
    for chunk in str(raw).split("."):
        digits = ""
        for character in chunk:
            if not character.isdigit():
                break
            digits += character
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)
REQUIRED_TABLES = (
    "ingestion.job_runs",
    "ingestion.run_units",
    "ingestion.fire_detections",
    "ingestion.backfill_chunks",
    "ingestion.gaps",
    "ingestion.source_freshness",
    "ingestion.job_leases",
    "ingestion.firms_rate_limit_reservations",
    "ingestion.alembic_version",
)


class Database:
    """One bounded SQLAlchemy engine per process, with every session forced to UTC."""

    def __init__(
        self,
        database_url: str,
        config: DatabaseConfig,
        *,
        application_name: str = "ingestion:firms",
    ) -> None:
        self.config = config
        self.engine: Engine = create_engine(
            database_url,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_timeout=config.pool_timeout_s,
            pool_pre_ping=True,
            pool_recycle=config.pool_recycle_s,
            connect_args={
                "application_name": application_name,
                "options": "-c timezone=UTC",
            },
        )

    def close(self) -> None:
        self.engine.dispose()

    def check(self, *, max_concurrent_backfills: int = 1) -> dict[str, Any]:
        def operation() -> dict[str, Any]:
            with self.engine.connect() as connection:
                server_version_num = int(
                    connection.execute(text("SHOW server_version_num")).scalar_one()
                )
                max_connections = int(
                    connection.execute(text("SHOW max_connections")).scalar_one()
                )
                postgis_version = connection.execute(
                    text("SELECT extversion FROM pg_extension WHERE extname = 'postgis'")
                ).scalar_one_or_none()
                table_status = {
                    table_name: bool(
                        connection.execute(
                            text("SELECT to_regclass(:table_name) IS NOT NULL"),
                            {"table_name": table_name},
                        ).scalar_one()
                    )
                    for table_name in REQUIRED_TABLES
                }
                revision = None
                if table_status["ingestion.alembic_version"]:
                    revision = connection.execute(
                        text("SELECT version_num FROM ingestion.alembic_version")
                    ).scalar_one_or_none()
                return {
                    "server_version_num": server_version_num,
                    "postgis": postgis_version,
                    "max_connections": max_connections,
                    "tables": table_status,
                    "revision": revision,
                }

        result = self._retry("database_check", operation)
        if result["server_version_num"] < MINIMUM_SERVER_VERSION_NUM:
            raise ConfigurationError(
                f"PostgreSQL {MINIMUM_SERVER_VERSION_NUM // 10000} or newer is "
                f"required; server_version_num={result['server_version_num']}"
            )
        if not result["postgis"]:
            raise ConfigurationError("PostGIS is not installed; run the ingestion migration")
        postgis_version = parse_version(result["postgis"])
        minimum = ".".join(str(part) for part in MINIMUM_POSTGIS_VERSION)
        if not postgis_version:
            raise ConfigurationError(
                f"Could not read the PostGIS version from {result['postgis']!r}; "
                f"PostGIS {minimum} or newer is required"
            )
        if postgis_version < MINIMUM_POSTGIS_VERSION:
            raise ConfigurationError(
                f"PostGIS {minimum} or newer is required; found {result['postgis']}"
            )

        per_process = self.config.pool_size + self.config.max_overflow
        aggregate = 1 + max_concurrent_backfills * per_process
        if aggregate > self.config.connection_budget:
            raise ConfigurationError(
                "Configured FIRMS concurrency needs "
                f"{aggregate} connections (runner/status=1 + "
                f"{max_concurrent_backfills} backfills x {per_process}), above "
                f"connection_budget={self.config.connection_budget}"
            )
        if self.config.connection_budget >= result["max_connections"]:
            raise ConfigurationError(
                f"connection_budget={self.config.connection_budget} must be below "
                f"PostgreSQL max_connections={result['max_connections']}"
            )
        result["required_connections"] = aggregate
        result["schema_ready"] = (
            all(result["tables"].values()) and result["revision"] == EXPECTED_REVISION
        )
        return result

    def require_schema(self, *, max_concurrent_backfills: int) -> None:
        status = self.check(max_concurrent_backfills=max_concurrent_backfills)
        if not status["schema_ready"]:
            missing = [name for name, ready in status["tables"].items() if not ready]
            detail = f"missing tables: {', '.join(missing)}" if missing else (
                f"database revision={status['revision']!r}, expected={EXPECTED_REVISION!r}"
            )
            raise ConfigurationError(
                "The ingestion schema is not current; run `python -m ingestion migrate` "
                f"({detail})"
            )

    def acquire_backfill_lease(
        self,
        *,
        backfill_id: str,
        holder: str,
        max_concurrent: int,
        ttl_s: int,
        heartbeat_s: int,
    ) -> "BackfillLease | None":
        lease_key = f"backfill:firms:{backfill_id}"

        def operation() -> bool:
            with self.engine.begin() as connection:
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext('firms-backfill-capacity'))")
                )
                connection.execute(
                    text("DELETE FROM ingestion.job_leases WHERE expires_at <= now()")
                )
                existing = connection.execute(
                    text(
                        """
                        SELECT holder FROM ingestion.job_leases
                        WHERE lease_key = :lease_key AND expires_at > now()
                        """
                    ),
                    {"lease_key": lease_key},
                ).scalar_one_or_none()
                if existing is not None:
                    if str(existing) != holder:
                        return False
                    connection.execute(
                        text(
                            """
                            UPDATE ingestion.job_leases
                            SET expires_at = now() + (:ttl_s * interval '1 second'),
                                heartbeat_at = now()
                            WHERE lease_key = :lease_key AND holder = CAST(:holder AS uuid)
                            """
                        ),
                        {"lease_key": lease_key, "holder": holder, "ttl_s": ttl_s},
                    )
                    return True

                active = int(
                    connection.execute(
                        text(
                            """
                            SELECT count(*) FROM ingestion.job_leases
                            WHERE lease_key LIKE 'backfill:firms:%' AND expires_at > now()
                            """
                        )
                    ).scalar_one()
                )
                if active >= max_concurrent:
                    return False
                connection.execute(
                    text(
                        """
                        INSERT INTO ingestion.job_leases
                          (lease_key, holder, expires_at, heartbeat_at)
                        VALUES
                          (:lease_key, CAST(:holder AS uuid),
                           now() + (:ttl_s * interval '1 second'), now())
                        """
                    ),
                    {"lease_key": lease_key, "holder": holder, "ttl_s": ttl_s},
                )
                connection.execute(
                    text(
                        """
                        UPDATE ingestion.job_runs
                        SET status = 'failed', finished_at = now(),
                            error_summary = 'lease expired before run finalization'
                        WHERE backfill_id = :backfill_id AND status = 'running'
                          AND run_id <> CAST(:holder AS uuid)
                        """
                    ),
                    {"backfill_id": backfill_id, "holder": holder},
                )
                return True

        acquired = self._retry("acquire_backfill_lease", operation)
        if not acquired:
            return None
        return BackfillLease(
            database=self,
            lease_key=lease_key,
            holder=holder,
            ttl_s=ttl_s,
            heartbeat_s=heartbeat_s,
        )

    def heartbeat_lease(self, lease_key: str, holder: str, ttl_s: int) -> bool:
        def operation() -> bool:
            with self.engine.begin() as connection:
                result = connection.execute(
                    text(
                        """
                        UPDATE ingestion.job_leases
                        SET expires_at = now() + (:ttl_s * interval '1 second'),
                            heartbeat_at = now()
                        WHERE lease_key = :lease_key AND holder = CAST(:holder AS uuid)
                        """
                    ),
                    {"lease_key": lease_key, "holder": holder, "ttl_s": ttl_s},
                )
                return result.rowcount == 1

        return self._retry("heartbeat_backfill_lease", operation)

    def lease_is_owned(self, lease_key: str, holder: str) -> bool:
        def operation() -> bool:
            with self.engine.connect() as connection:
                return bool(
                    connection.execute(
                        text(
                            """
                            SELECT EXISTS (
                              SELECT 1 FROM ingestion.job_leases
                              WHERE lease_key = :lease_key
                                AND holder = CAST(:holder AS uuid)
                                AND expires_at > now()
                            )
                            """
                        ),
                        {"lease_key": lease_key, "holder": holder},
                    ).scalar_one()
                )

        return self._retry("verify_backfill_lease", operation)

    def release_lease(self, lease_key: str, holder: str) -> None:
        def operation() -> None:
            with self.engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        DELETE FROM ingestion.job_leases
                        WHERE lease_key = :lease_key AND holder = CAST(:holder AS uuid)
                        """
                    ),
                    {"lease_key": lease_key, "holder": holder},
                )

        self._retry("release_backfill_lease", operation)

    def create_run(
        self,
        *,
        run_id: str,
        backfill_id: str,
        aoi: str,
        start: date,
        end_inclusive: date,
        status: str = "running",
    ) -> None:
        finished_at = datetime.now(UTC) if status != "running" else None

        def operation() -> None:
            with self.engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        INSERT INTO ingestion.job_runs
                          (run_id, source, run_type, status, backfill_id, aoi,
                           window_start, window_end, finished_at)
                        VALUES
                          (CAST(:run_id AS uuid), 'firms', 'backfill', :status,
                           :backfill_id, :aoi, :window_start, :window_end, :finished_at)
                        ON CONFLICT (run_id) DO NOTHING
                        """
                    ),
                    {
                        "run_id": run_id,
                        "status": status,
                        "backfill_id": backfill_id,
                        "aoi": aoi,
                        "window_start": _midnight(start),
                        "window_end": _midnight(end_inclusive + timedelta(days=1)),
                        "finished_at": finished_at,
                    },
                )

        self._retry("create_run", operation)

    def sync_chunks(
        self,
        *,
        backfill_id: str,
        aoi: str,
        chunks: Sequence[BackfillChunk],
    ) -> None:
        """Reconcile the plan, preserving finished calendar-day coverage."""

        def operation() -> None:
            with self.engine.begin() as connection:
                existing = list(
                    connection.execute(
                        text(
                            """
                            SELECT product_family, product, window_start, window_end,
                                   status, attempts
                            FROM ingestion.backfill_chunks
                            WHERE backfill_id = :backfill_id
                            FOR UPDATE
                            """
                        ),
                        {"backfill_id": backfill_id},
                    )
                )
                done_days: dict[str, set[date]] = {}
                attempts_by_key: dict[tuple[str, datetime, datetime], int] = {}
                for row in existing:
                    start_day = _utc_date(row.window_start)
                    end_day = _utc_date(row.window_end)
                    attempts_by_key[
                        (row.product_family, _midnight(start_day), _midnight(end_day))
                    ] = int(row.attempts)
                    if row.status == "done":
                        day = start_day
                        while day < end_day:
                            done_days.setdefault(row.product_family, set()).add(day)
                            day += timedelta(days=1)

                # The backfill-level lease makes every non-done row safe to rebuild.
                connection.execute(
                    text(
                        """
                        DELETE FROM ingestion.backfill_chunks
                        WHERE backfill_id = :backfill_id AND status <> 'done'
                        """
                    ),
                    {"backfill_id": backfill_id},
                )

                reconciled: list[BackfillChunk] = []
                for chunk in chunks:
                    reconciled.extend(
                        _subtract_done_days(chunk, done_days.get(chunk.family, set()))
                    )
                rows = []
                for chunk in reconciled:
                    start_at = _midnight(chunk.start)
                    end_at = _midnight(chunk.end_exclusive)
                    rows.append(
                        {
                            "backfill_id": backfill_id,
                            "family": chunk.family,
                            "product": chunk.product,
                            "aoi": aoi,
                            "window_start": start_at,
                            "window_end": end_at,
                            "status": chunk.status,
                            "attempts": attempts_by_key.get(
                                (chunk.family, start_at, end_at), 0
                            ),
                        }
                    )
                if rows:
                    connection.execute(_BACKFILL_CHUNK_INSERT, rows)

        self._retry("sync_backfill_chunks", operation)

    def pending_chunks(self, backfill_id: str) -> tuple[BackfillChunk, ...]:
        def operation() -> tuple[BackfillChunk, ...]:
            with self.engine.connect() as connection:
                result = connection.execute(
                    text(
                        """
                        SELECT product_family, product, window_start, window_end
                        FROM ingestion.backfill_chunks
                        WHERE backfill_id = :backfill_id
                          AND status IN ('pending', 'failed')
                          AND product IS NOT NULL
                        ORDER BY window_start, product_family
                        """
                    ),
                    {"backfill_id": backfill_id},
                )
                return tuple(
                    BackfillChunk(
                        family=row.product_family,
                        product=row.product,
                        start=_utc_date(row.window_start),
                        end_exclusive=_utc_date(row.window_end),
                    )
                    for row in result
                )

        return self._retry("read_pending_chunks", operation)

    def waiting_chunks(self, backfill_id: str) -> tuple[BackfillChunk, ...]:
        def operation() -> tuple[BackfillChunk, ...]:
            with self.engine.connect() as connection:
                result = connection.execute(
                    text(
                        """
                        SELECT product_family, window_start, window_end
                        FROM ingestion.backfill_chunks
                        WHERE backfill_id = :backfill_id AND status = 'waiting'
                        ORDER BY window_start, product_family
                        """
                    ),
                    {"backfill_id": backfill_id},
                )
                return tuple(
                    BackfillChunk(
                        family=row.product_family,
                        product=None,
                        start=_utc_date(row.window_start),
                        end_exclusive=_utc_date(row.window_end),
                    )
                    for row in result
                )

        return self._retry("read_waiting_chunks", operation)

    def mark_chunk_running(
        self, backfill_id: str, chunk: BackfillChunk, *, run_id: str
    ) -> bool:
        """Atomically claim a chunk and recognize a retried claim by its owner."""

        def operation() -> bool:
            with self.engine.begin() as connection:
                claimed = connection.execute(
                    text(
                        """
                        UPDATE ingestion.backfill_chunks
                        SET status = 'running',
                            attempts = CASE
                              WHEN status = 'running' AND owner_run_id = CAST(:run_id AS uuid)
                              THEN attempts ELSE attempts + 1
                            END,
                            owner_run_id = CAST(:run_id AS uuid),
                            last_error = NULL, updated_at = now()
                        WHERE backfill_id = :backfill_id
                          AND product_family = :family
                          AND window_start = :window_start
                          AND window_end = :window_end
                          AND (
                            status IN ('pending', 'failed')
                            OR (status = 'running' AND owner_run_id = CAST(:run_id AS uuid))
                          )
                        RETURNING 1
                        """
                    ),
                    {
                        "run_id": run_id,
                        "backfill_id": backfill_id,
                        "family": chunk.family,
                        "window_start": _midnight(chunk.start),
                        "window_end": _midnight(chunk.end_exclusive),
                    },
                ).scalar_one_or_none()
                return claimed is not None

        return self._retry("claim_backfill_chunk", operation)

    def record_waiting_units(
        self,
        *,
        run_id: str,
        backfill_id: str,
        aoi: str,
        chunks: Sequence[BackfillChunk],
    ) -> None:
        if not chunks:
            return
        rows = [
            _run_unit_params(
                run_id,
                chunk,
                aoi,
                status="skipped",
                fetched=0,
                stored=0,
                rejected=0,
                retries=0,
                error="no FIRMS product currently covers this date range",
                product_override=f"UNAVAILABLE:{chunk.family}",
            )
            for chunk in chunks
        ]

        def operation() -> None:
            with self.engine.begin() as connection:
                connection.execute(_RUN_UNIT_UPSERT, rows)

        self._retry("record_waiting_units", operation)

    def record_chunk_success(
        self,
        *,
        run_id: str,
        backfill_id: str,
        aoi: str,
        chunk: BackfillChunk,
        fetched: int,
        stored: int,
        rejected: int,
        retries: int,
        latest_as_of: datetime | None,
    ) -> None:
        def operation() -> None:
            with self.engine.begin() as connection:
                self._finish_owned_chunk(
                    connection,
                    run_id=run_id,
                    backfill_id=backfill_id,
                    chunk=chunk,
                    status="done",
                    error=None,
                )
                connection.execute(
                    _RUN_UNIT_UPSERT,
                    _run_unit_params(
                        run_id,
                        chunk,
                        aoi,
                        status="succeeded",
                        fetched=fetched,
                        stored=stored,
                        rejected=rejected,
                        retries=retries,
                        error=None,
                    ),
                )
                connection.execute(
                    text(
                        """
                        INSERT INTO ingestion.source_freshness
                          (source, product, latest_as_of, last_success_at, last_status)
                        VALUES ('firms', :product, :latest_as_of, now(), 'succeeded')
                        ON CONFLICT (source, product) DO UPDATE SET
                          latest_as_of = CASE
                            WHEN EXCLUDED.latest_as_of IS NULL
                            THEN ingestion.source_freshness.latest_as_of
                            ELSE GREATEST(
                              ingestion.source_freshness.latest_as_of,
                              EXCLUDED.latest_as_of
                            )
                          END,
                          last_success_at = now(), last_status = 'succeeded'
                        """
                    ),
                    {"product": chunk.product, "latest_as_of": latest_as_of},
                )
                connection.execute(
                    text(
                        """
                        UPDATE ingestion.gaps SET resolved_at = now()
                        WHERE source = 'firms' AND product = :product AND aoi = :aoi
                          AND window_start = :window_start AND window_end = :window_end
                          AND resolved_at IS NULL
                        """
                    ),
                    {
                        "product": chunk.product,
                        "aoi": aoi,
                        "window_start": _midnight(chunk.start),
                        "window_end": _midnight(chunk.end_exclusive),
                    },
                )

        self._retry("record_chunk_success", operation)

    def record_chunk_failure(
        self,
        *,
        run_id: str,
        backfill_id: str,
        aoi: str,
        chunk: BackfillChunk,
        retries: int,
        error: str,
    ) -> None:
        safe_error = redact(error)[:2000]

        def operation() -> None:
            with self.engine.begin() as connection:
                attempts = self._finish_owned_chunk(
                    connection,
                    run_id=run_id,
                    backfill_id=backfill_id,
                    chunk=chunk,
                    status="failed",
                    error=safe_error,
                )
                connection.execute(
                    _RUN_UNIT_UPSERT,
                    _run_unit_params(
                        run_id,
                        chunk,
                        aoi,
                        status="failed",
                        fetched=0,
                        stored=0,
                        rejected=0,
                        retries=retries,
                        error=safe_error,
                    ),
                )
                connection.execute(
                    text(
                        """
                        INSERT INTO ingestion.gaps
                          (source, product, aoi, window_start, window_end, reason, attempts)
                        VALUES
                          ('firms', :product, :aoi, :window_start, :window_end,
                           :reason, :attempts)
                        ON CONFLICT (source, product, aoi, window_start, window_end)
                        DO UPDATE SET
                          reason = EXCLUDED.reason,
                          attempts = GREATEST(ingestion.gaps.attempts, EXCLUDED.attempts),
                          last_attempt_at = now(), resolved_at = NULL
                        """
                    ),
                    {
                        "product": chunk.product,
                        "aoi": aoi,
                        "window_start": _midnight(chunk.start),
                        "window_end": _midnight(chunk.end_exclusive),
                        "reason": safe_error,
                        "attempts": attempts,
                    },
                )

        self._retry("record_chunk_failure", operation)

    @staticmethod
    def _finish_owned_chunk(
        connection: Any,
        *,
        run_id: str,
        backfill_id: str,
        chunk: BackfillChunk,
        status: str,
        error: str | None,
    ) -> int:
        row = connection.execute(
            text(
                """
                UPDATE ingestion.backfill_chunks
                SET status = :status, last_error = :error, updated_at = now()
                WHERE backfill_id = :backfill_id
                  AND product_family = :family
                  AND window_start = :window_start
                  AND window_end = :window_end
                  AND owner_run_id = CAST(:run_id AS uuid)
                  AND (status = 'running' OR status = :status)
                RETURNING attempts
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "error": error,
                "backfill_id": backfill_id,
                "family": chunk.family,
                "window_start": _midnight(chunk.start),
                "window_end": _midnight(chunk.end_exclusive),
            },
        ).scalar_one_or_none()
        if row is None:
            raise LeaseLostError(
                f"run {run_id} no longer owns {chunk.family} {chunk.start}"
            )
        return int(row)

    def finish_run(self, run_id: str) -> dict[str, int | str]:
        def operation() -> dict[str, int | str]:
            with self.engine.begin() as connection:
                row = connection.execute(
                    text(
                        """
                        SELECT
                          count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
                          count(*) FILTER (WHERE status = 'failed') AS failed,
                          count(*) FILTER (WHERE status = 'skipped') AS skipped,
                          COALESCE(sum(fetched), 0) AS fetched,
                          COALESCE(sum(stored), 0) AS stored,
                          COALESCE(sum(rejected), 0) AS rejected,
                          COALESCE(sum(retries), 0) AS retries
                        FROM ingestion.run_units
                        WHERE run_id = CAST(:run_id AS uuid)
                        """
                    ),
                    {"run_id": run_id},
                ).one()
                succeeded, failed, skipped = (
                    int(row.succeeded), int(row.failed), int(row.skipped)
                )
                if failed and (succeeded or skipped):
                    status = "partial"
                elif failed:
                    status = "failed"
                elif skipped and succeeded:
                    status = "partial"
                elif skipped:
                    status = "skipped"
                elif succeeded:
                    status = "succeeded"
                else:
                    status = "skipped"
                connection.execute(
                    text(
                        """
                        UPDATE ingestion.job_runs
                        SET status = :status, finished_at = now(), fetched = :fetched,
                            stored = :stored, rejected = :rejected, retries = :retries
                        WHERE run_id = CAST(:run_id AS uuid)
                        """
                    ),
                    {
                        "run_id": run_id,
                        "status": status,
                        "fetched": int(row.fetched),
                        "stored": int(row.stored),
                        "rejected": int(row.rejected),
                        "retries": int(row.retries),
                    },
                )
                return {
                    "status": status,
                    "succeeded": succeeded,
                    "failed": failed,
                    "skipped": skipped,
                    "fetched": int(row.fetched),
                    "stored": int(row.stored),
                    "rejected": int(row.rejected),
                    "retries": int(row.retries),
                }

        return self._retry("finish_run", operation)

    def fail_run(self, run_id: str, error: str) -> None:
        safe_error = redact(error)[:2000]

        def operation() -> None:
            with self.engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        UPDATE ingestion.job_runs
                        SET status = 'failed', finished_at = now(), error_summary = :error
                        WHERE run_id = CAST(:run_id AS uuid) AND status = 'running'
                        """
                    ),
                    {"run_id": run_id, "error": safe_error},
                )

        self._retry("fail_run", operation)

    def insert_detections(
        self,
        detections: Sequence[FirmsDetection],
        *,
        run_id: str,
        backfill_id: str,
        retrieved_at: datetime,
    ) -> int:
        if not detections:
            return 0
        for month in sorted({_month_start(item.as_of) for item in detections}):
            self._ensure_fire_partition(month)

        total = 0
        for offset in range(0, len(detections), self.config.batch_size):
            batch = detections[offset : offset + self.config.batch_size]
            rows = [
                item.as_record(
                    run_id=run_id,
                    backfill_id=backfill_id,
                    retrieved_at=retrieved_at,
                )
                for item in batch
            ]

            def operation(rows: list[dict[str, Any]] = rows) -> int:
                with self.engine.begin() as connection:
                    result = connection.execute(_FIRE_INSERT, rows)
                    return max(0, int(result.rowcount or 0))

            total += self._retry("insert_firms_batch", operation)
        return total

    def _ensure_fire_partition(self, month: date) -> None:
        next_month = _next_month(month)
        partition_name = f"fire_detections_y{month.year:04d}m{month.month:02d}"
        lower = f"{month.isoformat()} 00:00:00+00"
        upper = f"{next_month.isoformat()} 00:00:00+00"
        ddl = text(
            f"""
            CREATE TABLE IF NOT EXISTS ingestion.{partition_name}
            PARTITION OF ingestion.fire_detections
            FOR VALUES FROM (TIMESTAMPTZ '{lower}') TO (TIMESTAMPTZ '{upper}')
            """
        )

        def operation() -> None:
            with self.engine.begin() as connection:
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                    {"key": "ingestion.fire_detections.partition"},
                )
                connection.execute(ddl)

        self._retry("ensure_fire_partition", operation)

    def _retry(self, event_name: str, operation: Callable[[], T]) -> T:
        def before_sleep(state: RetryCallState) -> None:
            error = state.outcome.exception() if state.outcome else None
            log_event(
                LOGGER,
                logging.WARNING,
                "database_retry",
                operation=event_name,
                attempt=state.attempt_number,
                error_type=type(error).__name__ if error else None,
                wait_s=round(float(state.next_action.sleep), 3)
                if state.next_action
                else None,
            )

        retryer = Retrying(
            retry=retry_if_exception_type((OperationalError, SQLAlchemyTimeoutError)),
            wait=wait_exponential_jitter(initial=1, max=30),
            stop=stop_after_attempt(self.config.operation_max_attempts),
            before_sleep=before_sleep,
            reraise=True,
        )
        try:
            return retryer(operation)
        except (OperationalError, SQLAlchemyTimeoutError) as exc:
            raise DatabaseUnavailableError(
                f"database operation {event_name} exhausted retries: {redact(str(exc))}"
            ) from exc


class BackfillLease:
    """DB-backed expiring lease; heartbeat checks out a connection only briefly."""

    def __init__(
        self,
        *,
        database: Database,
        lease_key: str,
        holder: str,
        ttl_s: int,
        heartbeat_s: int,
    ) -> None:
        self.database = database
        self.lease_key = lease_key
        self.holder = holder
        self.ttl_s = ttl_s
        self.heartbeat_s = heartbeat_s
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "BackfillLease":
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"lease-{self.holder[:8]}",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def assert_owned(self) -> None:
        if self._lost.is_set() or not self.database.lease_is_owned(
            self.lease_key, self.holder
        ):
            self._lost.set()
            raise LeaseLostError(f"backfill lease {self.lease_key} is no longer owned")

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.heartbeat_s + 1)
        try:
            self.database.release_lease(self.lease_key, self.holder)
        except DatabaseUnavailableError:
            log_event(
                LOGGER,
                logging.WARNING,
                "lease_release_failed",
                lease_key=self.lease_key,
            )

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_s):
            try:
                if not self.database.heartbeat_lease(
                    self.lease_key, self.holder, self.ttl_s
                ):
                    self._lost.set()
                    return
            except DatabaseUnavailableError:
                self._lost.set()
                return


class PostgresFirmsQuota:
    """Rolling ten-minute MAP_KEY allowance coordinated across processes."""

    WINDOW = timedelta(minutes=10)

    def __init__(self, database: Database, firms: FirmsConfig, map_key: str) -> None:
        self.database = database
        self.limit = firms.transactions_per_10_minutes
        self.key_fingerprint = hashlib.sha256(map_key.encode("utf-8")).hexdigest()[:16]

    def reserve(self, cost: int) -> None:
        if cost < 1 or cost > self.limit:
            raise ConfigurationError(
                f"FIRMS quota cost {cost} is outside the permitted range"
            )
        reservation_id = str(uuid.uuid4())
        while True:
            accepted, wait_s = self._try_reserve(reservation_id, cost)
            if accepted:
                return
            log_event(
                LOGGER,
                logging.INFO,
                "firms_quota_wait",
                wait_s=round(wait_s, 3),
                requested_tokens=cost,
                limit=self.limit,
            )
            time.sleep(wait_s)  # No connection is held while waiting.

    def _try_reserve(self, reservation_id: str, cost: int) -> tuple[bool, float]:
        def operation() -> tuple[bool, float]:
            with self.database.engine.begin() as connection:
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                    {"key": f"firms-quota:{self.key_fingerprint}"},
                )
                already_reserved = connection.execute(
                    text(
                        """
                        SELECT EXISTS (
                          SELECT 1 FROM ingestion.firms_rate_limit_reservations
                          WHERE reservation_id = CAST(:reservation_id AS uuid)
                        )
                        """
                    ),
                    {"reservation_id": reservation_id},
                ).scalar_one()
                if already_reserved:
                    return True, 0.0
                connection.execute(
                    text(
                        """
                        DELETE FROM ingestion.firms_rate_limit_reservations
                        WHERE reserved_at <= clock_timestamp() - interval '10 minutes'
                        """
                    )
                )
                row = connection.execute(
                    text(
                        """
                        SELECT COALESCE(sum(cost), 0) AS used,
                               COALESCE(
                                 EXTRACT(EPOCH FROM (
                                   min(reserved_at) + interval '10 minutes' - clock_timestamp()
                                 )), 0
                               ) AS wait_s
                        FROM ingestion.firms_rate_limit_reservations
                        WHERE key_fingerprint = :fingerprint
                        """
                    ),
                    {"fingerprint": self.key_fingerprint},
                ).one()
                if int(row.used) + cost > self.limit:
                    return False, max(0.1, float(row.wait_s) + 0.1)
                connection.execute(
                    text(
                        """
                        INSERT INTO ingestion.firms_rate_limit_reservations
                          (reservation_id, key_fingerprint, cost)
                        VALUES (CAST(:reservation_id AS uuid), :fingerprint, :cost)
                        """
                    ),
                    {
                        "reservation_id": reservation_id,
                        "fingerprint": self.key_fingerprint,
                        "cost": cost,
                    },
                )
                return True, 0.0

        return self.database._retry("reserve_firms_quota", operation)


_BACKFILL_CHUNK_INSERT = text(
    """
    INSERT INTO ingestion.backfill_chunks
      (backfill_id, source, product_family, product, aoi,
       window_start, window_end, status, attempts)
    VALUES
      (:backfill_id, 'firms', :family, :product, :aoi,
       :window_start, :window_end, :status, :attempts)
    ON CONFLICT (backfill_id, product_family, window_start, window_end)
    DO NOTHING
    """
)

_FIRE_INSERT = text(
    """
    INSERT INTO ingestion.fire_detections
      (product, satellite, lat, lon, as_of, acq_date, acq_time,
       brightness_temp, frp, day_night, instrument, confidence,
       scan, track, version, run_id, backfill_id, source_retrieved_at)
    VALUES
      (:product, :satellite, :lat, :lon, :as_of, :acq_date, :acq_time,
       :brightness_temp, :frp, :day_night, :instrument, :confidence,
       :scan, :track, :version, CAST(:run_id AS uuid), :backfill_id,
       :retrieved_at)
    ON CONFLICT (product, satellite, as_of, lat, lon) DO NOTHING
    """
)

_RUN_UNIT_UPSERT = text(
    """
    INSERT INTO ingestion.run_units
      (run_id, product, product_family, aoi, window_start, window_end,
       status, fetched, stored, rejected, retries, error_summary)
    VALUES
      (CAST(:run_id AS uuid), :product, :family, :aoi, :window_start, :window_end,
       :status, :fetched, :stored, :rejected, :retries, :error)
    ON CONFLICT (run_id, product_family, window_start, window_end)
    DO UPDATE SET
      product = EXCLUDED.product, status = EXCLUDED.status,
      fetched = EXCLUDED.fetched, stored = EXCLUDED.stored,
      rejected = EXCLUDED.rejected, retries = EXCLUDED.retries,
      error_summary = EXCLUDED.error_summary
    """
)


def _run_unit_params(
    run_id: str,
    chunk: BackfillChunk,
    aoi: str,
    *,
    status: str,
    fetched: int,
    stored: int,
    rejected: int,
    retries: int,
    error: str | None,
    product_override: str | None = None,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "product": product_override or chunk.product,
        "family": chunk.family,
        "aoi": aoi,
        "window_start": _midnight(chunk.start),
        "window_end": _midnight(chunk.end_exclusive),
        "status": status,
        "fetched": fetched,
        "stored": stored,
        "rejected": rejected,
        "retries": retries,
        "error": error,
    }


def _subtract_done_days(
    chunk: BackfillChunk, done_days: set[date]
) -> list[BackfillChunk]:
    results: list[BackfillChunk] = []
    cursor = chunk.start
    while cursor < chunk.end_exclusive:
        while cursor < chunk.end_exclusive and cursor in done_days:
            cursor += timedelta(days=1)
        if cursor >= chunk.end_exclusive:
            break
        segment_start = cursor
        while cursor < chunk.end_exclusive and cursor not in done_days:
            cursor += timedelta(days=1)
        results.append(
            BackfillChunk(
                family=chunk.family,
                product=chunk.product,
                start=segment_start,
                end_exclusive=cursor,
            )
        )
    return results


def _midnight(value: date) -> datetime:
    return datetime.combine(value, datetime_time.min, tzinfo=UTC)


def _utc_date(value: datetime) -> date:
    return value.astimezone(UTC).date()


def _month_start(value: datetime) -> date:
    utc_value = value.astimezone(UTC)
    return date(utc_value.year, utc_value.month, 1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)
