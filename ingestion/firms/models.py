"""Typed values shared by FIRMS planning, parsing, and storage."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(frozen=True)
class ProductAvailability:
    product: str
    start: date
    end: date  # inclusive

    def covers(self, day: date) -> bool:
        return self.start <= day <= self.end


@dataclass(frozen=True)
class BackfillChunk:
    family: str
    product: str | None
    start: date
    end_exclusive: date

    @property
    def day_count(self) -> int:
        return (self.end_exclusive - self.start).days

    @property
    def status(self) -> str:
        return "pending" if self.product else "waiting"

    def to_json(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "product": self.product,
            "start": self.start.isoformat(),
            "end_inclusive": date.fromordinal(self.end_exclusive.toordinal() - 1).isoformat(),
            "day_count": self.day_count,
            "status": self.status,
        }


@dataclass(frozen=True)
class FirmsDetection:
    product: str
    satellite: str
    lat: float
    lon: float
    as_of: datetime
    acq_date: date
    acq_time: int
    brightness_temp: float
    frp: float
    day_night: str
    instrument: str | None
    confidence: str | None
    scan: float | None
    track: float | None
    version: str | None

    def as_record(
        self,
        *,
        run_id: str,
        backfill_id: str,
        retrieved_at: datetime,
    ) -> dict[str, Any]:
        record = asdict(self)
        record.update(
            run_id=run_id,
            backfill_id=backfill_id,
            retrieved_at=retrieved_at,
        )
        return record


@dataclass(frozen=True)
class ParseResult:
    detections: tuple[FirmsDetection, ...]
    rejected: int


@dataclass(frozen=True)
class HttpTextResult:
    text: str
    retries: int
    retrieved_at: datetime
