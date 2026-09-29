"""Availability-aware FIRMS backfill planning."""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta

from ingestion.core.errors import ConfigurationError
from ingestion.core.settings import Aoi
from ingestion.firms.models import BackfillChunk, ProductAvailability

PLANNER_VERSION = 1


def plan_chunks(
    *,
    start: date,
    end_inclusive: date,
    product_families: dict[str, tuple[str, ...]],
    availability: tuple[ProductAvailability, ...] | None,
    chunk_days: int,
) -> tuple[BackfillChunk, ...]:
    if end_inclusive < start:
        raise ConfigurationError("--end must be on or after --start")
    if not 1 <= chunk_days <= 5:
        raise ConfigurationError("FIRMS chunks must contain between 1 and 5 days")

    availability_by_product: dict[str, list[ProductAvailability]] = {}
    for item in availability or ():
        availability_by_product.setdefault(item.product.upper(), []).append(item)

    planned: list[BackfillChunk] = []
    final_exclusive = end_inclusive + timedelta(days=1)
    for family, candidates in product_families.items():
        cursor = start
        while cursor < final_exclusive:
            selected = _select_product(
                cursor,
                candidates,
                availability_by_product,
                assume_preferred=availability is None,
            )
            segment_end = cursor + timedelta(days=1)
            while segment_end < final_exclusive:
                next_product = _select_product(
                    segment_end,
                    candidates,
                    availability_by_product,
                    assume_preferred=availability is None,
                )
                if next_product != selected:
                    break
                segment_end += timedelta(days=1)

            chunk_start = cursor
            while chunk_start < segment_end:
                chunk_end = min(segment_end, chunk_start + timedelta(days=chunk_days))
                planned.append(
                    BackfillChunk(
                        family=family,
                        product=selected,
                        start=chunk_start,
                        end_exclusive=chunk_end,
                    )
                )
                chunk_start = chunk_end
            cursor = segment_end

    return tuple(sorted(planned, key=lambda chunk: (chunk.start, chunk.family)))


def build_backfill_id(
    *,
    aoi: Aoi,
    start: date,
    end_inclusive: date,
    product_families: dict[str, tuple[str, ...]],
    chunk_days: int,
) -> str:
    identity = {
        "planner_version": PLANNER_VERSION,
        "aoi": {"name": aoi.name, "bounds": aoi.bounds},
        "start": start.isoformat(),
        "end_inclusive": end_inclusive.isoformat(),
        "product_families": product_families,
        "chunk_days": chunk_days,
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"firms-{digest[:20]}"


def explicit_product_families(products: list[str]) -> dict[str, tuple[str, ...]]:
    cleaned = [product.strip().upper() for product in products if product.strip()]
    if not cleaned:
        raise ConfigurationError("--products did not contain a product name")
    return {product: (product,) for product in dict.fromkeys(cleaned)}


def _select_product(
    day: date,
    candidates: tuple[str, ...],
    availability_by_product: dict[str, list[ProductAvailability]],
    *,
    assume_preferred: bool,
) -> str | None:
    if assume_preferred:
        return candidates[0]
    for candidate in candidates:
        if any(
            period.covers(day)
            for period in availability_by_product.get(candidate.upper(), ())
        ):
            return candidate
    return None
