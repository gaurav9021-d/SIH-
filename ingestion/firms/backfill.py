"""Resumable FIRMS historical backfill orchestration."""

from __future__ import annotations

import logging
import uuid
from datetime import date
from typing import Sequence

from ingestion.core.db import BackfillLease, Database, PostgresFirmsQuota
from ingestion.core.errors import LeaseLostError, UpstreamFormatError
from ingestion.core.http import ResilientHttpClient
from ingestion.core.logging import (
    configure_logging,
    get_logger,
    log_context,
    log_event,
    redact,
)
from ingestion.core.settings import Aoi, load_config, load_secrets
from ingestion.firms.client import FirmsClient
from ingestion.firms.models import BackfillChunk, ParseResult
from ingestion.firms.parser import parse_firms_csv
from ingestion.firms.planner import (
    build_backfill_id,
    explicit_product_families,
    plan_chunks,
)

LOGGER = get_logger("firms.backfill")


def run_firms_backfill(
    *,
    start: date,
    end_inclusive: date,
    aoi_name: str | None = None,
    products: Sequence[str] | None = None,
    config_path: str | None = None,
    dry_run: bool = False,
) -> int:
    """Backfill a calendar-date range; both CLI date bounds are inclusive."""

    config = load_config(config_path)
    secrets = load_secrets()
    map_key = (
        secrets.firms_map_key.get_secret_value()
        if secrets.firms_map_key is not None
        else None
    )
    database_secret = (
        secrets.database_url.get_secret_value()
        if secrets.database_url is not None
        else None
    )
    configure_logging(map_key, database_secret)

    aoi = config.get_aoi(aoi_name)
    families = (
        explicit_product_families(list(products))
        if products
        else config.firms.product_families
    )
    backfill_id = build_backfill_id(
        aoi=aoi,
        start=start,
        end_inclusive=end_inclusive,
        product_families=families,
        chunk_days=config.firms.chunk_days,
    )

    if dry_run:
        chunks = plan_chunks(
            start=start,
            end_inclusive=end_inclusive,
            product_families=families,
            availability=None,
            chunk_days=config.firms.chunk_days,
        )
        log_event(
            LOGGER,
            logging.WARNING if map_key is None else logging.INFO,
            "firms_backfill_dry_run",
            backfill_id=backfill_id,
            aoi=aoi.name,
            bounds=aoi.bounds,
            start=start,
            end_inclusive=end_inclusive,
            note="Preferred products are assumed; a live run refreshes FIRMS availability.",
            chunk_count=len(chunks),
        )
        for chunk in chunks:
            log_event(LOGGER, logging.INFO, "firms_backfill_chunk", **chunk.to_json())
        return 0

    map_key = secrets.require_firms_key()
    database_url = secrets.require_database_url()
    database = Database(database_url, config.database)
    run_id = str(uuid.uuid4())
    try:
        database.require_schema(
            max_concurrent_backfills=config.firms.max_concurrent_backfills
        )
        lease = database.acquire_backfill_lease(
            backfill_id=backfill_id,
            holder=run_id,
            max_concurrent=config.firms.max_concurrent_backfills,
            ttl_s=config.firms.lease_ttl_s,
            heartbeat_s=config.firms.lease_heartbeat_s,
        )
        if lease is None:
            database.create_run(
                run_id=run_id,
                backfill_id=backfill_id,
                aoi=aoi.name,
                start=start,
                end_inclusive=end_inclusive,
                status="skipped",
            )
            log_event(
                LOGGER,
                logging.INFO,
                "firms_backfill_skipped",
                backfill_id=backfill_id,
                reason="an identical backfill is active or capacity is full",
            )
            return 0

        with lease, log_context(
            source="firms",
            run_id=run_id,
            backfill_id=backfill_id,
            aoi=aoi.name,
        ):
            database.create_run(
                run_id=run_id,
                backfill_id=backfill_id,
                aoi=aoi.name,
                start=start,
                end_inclusive=end_inclusive,
            )
            log_event(
                LOGGER,
                logging.INFO,
                "firms_backfill_started",
                start=start,
                end_inclusive=end_inclusive,
                bounds=aoi.bounds,
            )
            try:
                quota = PostgresFirmsQuota(database, config.firms, map_key)
                with ResilientHttpClient(config.http) as http:
                    client = FirmsClient(
                        http=http,
                        config=config.firms,
                        map_key=map_key,
                        quota=quota,
                    )
                    availability = client.availability()
                    chunks = plan_chunks(
                        start=start,
                        end_inclusive=end_inclusive,
                        product_families=families,
                        availability=availability,
                        chunk_days=config.firms.chunk_days,
                    )
                    lease.assert_owned()
                    database.sync_chunks(
                        backfill_id=backfill_id,
                        aoi=aoi.name,
                        chunks=chunks,
                    )
                    waiting = list(database.waiting_chunks(backfill_id))
                    database.record_waiting_units(
                        run_id=run_id,
                        backfill_id=backfill_id,
                        aoi=aoi.name,
                        chunks=waiting,
                    )
                    for chunk in waiting:
                        log_event(
                            LOGGER,
                            logging.WARNING,
                            "firms_dates_not_available",
                            family=chunk.family,
                            start=chunk.start,
                            end_inclusive=chunk.to_json()["end_inclusive"],
                            action="will recheck on the next identical invocation",
                        )

                    pending = database.pending_chunks(backfill_id)
                    for chunk in pending:
                        lease.assert_owned()
                        _run_chunk(
                            database=database,
                            client=client,
                            lease=lease,
                            run_id=run_id,
                            backfill_id=backfill_id,
                            aoi=aoi,
                            chunk=chunk,
                        )
                lease.assert_owned()
                summary = database.finish_run(run_id)
            except KeyboardInterrupt as exc:
                _best_effort_fail_run(database, run_id, exc)
                raise
            except Exception as exc:
                _best_effort_fail_run(database, run_id, exc)
                LOGGER.error(
                    "firms_backfill_failed",
                    exc_info=True,
                    extra={"event": "firms_backfill_failed"},
                )
                return 1

            log_event(
                LOGGER,
                logging.INFO if summary["status"] != "failed" else logging.ERROR,
                "firms_backfill_finished",
                **summary,
                waiting_chunks=len(waiting),
            )
            return 1 if summary["status"] == "failed" else 0
    finally:
        database.close()


def _run_chunk(
    *,
    database: Database,
    client: FirmsClient,
    lease: BackfillLease,
    run_id: str,
    backfill_id: str,
    aoi: Aoi,
    chunk: BackfillChunk,
) -> None:
    if chunk.product is None:
        return
    retries = 0
    if not database.mark_chunk_running(backfill_id, chunk, run_id=run_id):
        log_event(
            LOGGER,
            logging.INFO,
            "firms_chunk_already_claimed",
            family=chunk.family,
            product=chunk.product,
            start=chunk.start,
            end_exclusive=chunk.end_exclusive,
        )
        return
    with log_context(
        product=chunk.product,
        window={
            "start": chunk.start.isoformat(),
            "end_exclusive": chunk.end_exclusive.isoformat(),
        },
    ):
        try:
            response = client.area(
                product=chunk.product,
                aoi=aoi,
                start_date=chunk.start.isoformat(),
                day_count=chunk.day_count,
            )
            retries = response.retries
            parsed = _filter_to_requested_scope(
                parse_firms_csv(response.text, chunk.product), chunk=chunk, aoi=aoi
            )
            lease.assert_owned()
            stored = database.insert_detections(
                parsed.detections,
                run_id=run_id,
                backfill_id=backfill_id,
                retrieved_at=response.retrieved_at,
            )
            lease.assert_owned()
            latest_as_of = max(
                (detection.as_of for detection in parsed.detections), default=None
            )
            fetched = len(parsed.detections) + parsed.rejected
            database.record_chunk_success(
                run_id=run_id,
                backfill_id=backfill_id,
                aoi=aoi.name,
                chunk=chunk,
                fetched=fetched,
                stored=stored,
                rejected=parsed.rejected,
                retries=retries,
                latest_as_of=latest_as_of,
            )
            log_event(
                LOGGER,
                logging.INFO,
                "firms_chunk_succeeded",
                fetched=fetched,
                stored=stored,
                duplicates=fetched - parsed.rejected - stored,
                rejected=parsed.rejected,
                retries=retries,
            )
        except LeaseLostError:
            raise
        except Exception as exc:
            retries = max(retries, int(getattr(exc, "retries", 0)))
            safe_error = f"{type(exc).__name__}: {redact(str(exc))}"
            database.record_chunk_failure(
                run_id=run_id,
                backfill_id=backfill_id,
                aoi=aoi.name,
                chunk=chunk,
                retries=retries,
                error=safe_error,
            )
            LOGGER.error(
                "firms_chunk_failed",
                exc_info=True,
                extra={
                    "event": "firms_chunk_failed",
                    "fields": {"error": safe_error, "retries": retries},
                },
            )


def _filter_to_requested_scope(
    parsed: ParseResult, *, chunk: BackfillChunk, aoi: Aoi
) -> ParseResult:
    in_scope = tuple(
        detection
        for detection in parsed.detections
        if chunk.start <= detection.acq_date < chunk.end_exclusive
        and aoi.south <= detection.lat <= aoi.north
        and aoi.west <= detection.lon <= aoi.east
    )
    scope_rejected = len(parsed.detections) - len(in_scope)
    if parsed.detections and not in_scope:
        raise UpstreamFormatError(
            "every FIRMS row fell outside the requested date window or AOI"
        )
    return ParseResult(
        detections=in_scope,
        rejected=parsed.rejected + scope_rejected,
    )


def _best_effort_fail_run(database: Database, run_id: str, error: BaseException) -> None:
    try:
        database.fail_run(run_id, f"{type(error).__name__}: {redact(str(error))}")
    except Exception as bookkeeping_error:
        log_event(
            LOGGER,
            logging.ERROR,
            "run_finalization_failed",
            original_error=f"{type(error).__name__}: {redact(str(error))}",
            bookkeeping_error=(
                f"{type(bookkeeping_error).__name__}: {redact(str(bookkeeping_error))}"
            ),
        )
