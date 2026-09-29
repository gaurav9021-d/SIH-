"""Strict, row-tolerant parser for FIRMS VIIRS and MODIS CSV."""

from __future__ import annotations

import csv
import io
import math
from datetime import UTC, date, datetime

from ingestion.core.errors import UpstreamFormatError
from ingestion.firms.models import FirmsDetection, ParseResult, ProductAvailability


def parse_firms_csv(payload: str, product: str) -> ParseResult:
    text = payload.lstrip("\ufeff\r\n ")
    if not text or text.startswith("<"):
        raise UpstreamFormatError(f"{product} did not return FIRMS CSV")

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise UpstreamFormatError(f"{product} response has no CSV header")

    normalized_fields = {field.strip().lower() for field in reader.fieldnames if field}
    required = {
        "latitude",
        "longitude",
        "acq_date",
        "acq_time",
        "satellite",
        "frp",
        "daynight",
    }
    if not required.issubset(normalized_fields):
        missing = ",".join(sorted(required - normalized_fields))
        raise UpstreamFormatError(f"{product} response is missing columns: {missing}")
    if not ({"bright_ti4", "brightness"} & normalized_fields):
        raise UpstreamFormatError(f"{product} response has no brightness column")

    detections: list[FirmsDetection] = []
    rejected = 0
    for source_row in reader:
        row = {
            str(key).strip().lower(): (value.strip() if value else "")
            for key, value in source_row.items()
            if key is not None
        }
        try:
            detections.append(_parse_row(row, product))
        except (KeyError, TypeError, ValueError, OverflowError):
            rejected += 1

    return ParseResult(detections=tuple(detections), rejected=rejected)


def parse_availability_csv(payload: str) -> tuple[ProductAvailability, ...]:
    text = payload.lstrip("\ufeff\r\n ")
    if not text or text.startswith("<"):
        raise UpstreamFormatError("FIRMS availability endpoint did not return CSV")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise UpstreamFormatError("FIRMS availability response has no CSV header")

    aliases = {field.strip().lower(): field for field in reader.fieldnames if field}
    product_column = _first_column(aliases, "data_id", "source", "product")
    start_column = _first_column(aliases, "min_date", "start_date", "date_start")
    end_column = _first_column(aliases, "max_date", "end_date", "date_end")
    if not all((product_column, start_column, end_column)):
        raise UpstreamFormatError("FIRMS availability CSV has an unexpected schema")

    results: list[ProductAvailability] = []
    for row in reader:
        try:
            product = str(row[product_column]).strip()
            start = date.fromisoformat(str(row[start_column]).strip())
            end = date.fromisoformat(str(row[end_column]).strip())
            if not product or end < start:
                raise ValueError("invalid availability row")
            results.append(ProductAvailability(product=product, start=start, end=end))
        except (KeyError, TypeError, ValueError) as exc:
            raise UpstreamFormatError(
                "FIRMS availability CSV contains an invalid product row"
            ) from exc
    if not results:
        raise UpstreamFormatError("FIRMS availability CSV contained no valid products")
    return tuple(results)


def _first_column(aliases: dict[str, str], *names: str) -> str | None:
    for name in names:
        if name in aliases:
            return aliases[name]
    return None


def _parse_row(row: dict[str, str], product: str) -> FirmsDetection:
    lat = _finite_float(row["latitude"])
    lon = _finite_float(row["longitude"])
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError("coordinate out of range")

    frp = _finite_float(row["frp"])
    if frp < 0:
        raise ValueError("FRP cannot be negative")

    brightness_raw = row.get("bright_ti4") or row.get("brightness")
    brightness = _finite_float(brightness_raw)

    acquisition_date = date.fromisoformat(row["acq_date"])
    acquisition_time = _parse_hhmm(row["acq_time"])
    hour, minute = divmod(acquisition_time, 100)
    as_of = datetime(
        acquisition_date.year,
        acquisition_date.month,
        acquisition_date.day,
        hour,
        minute,
        tzinfo=UTC,
    )

    day_night = row["daynight"].upper()
    if day_night not in {"D", "N"}:
        raise ValueError("daynight must be D or N")
    satellite = row["satellite"].strip()
    if not satellite:
        raise ValueError("satellite is required")

    return FirmsDetection(
        product=product,
        satellite=satellite,
        lat=lat,
        lon=lon,
        as_of=as_of,
        acq_date=acquisition_date,
        acq_time=acquisition_time,
        brightness_temp=brightness,
        frp=frp,
        day_night=day_night,
        instrument=_optional_text(row.get("instrument")),
        confidence=_optional_text(row.get("confidence")),
        scan=_optional_float(row.get("scan")),
        track=_optional_float(row.get("track")),
        version=_optional_text(row.get("version")),
    )


def _parse_hhmm(raw: str) -> int:
    value = raw.strip()
    if not value.isdigit() or len(value) > 4:
        raise ValueError("invalid FIRMS acquisition time")
    padded = value.zfill(4)
    hour = int(padded[:2])
    minute = int(padded[2:])
    if hour > 23 or minute > 59:
        raise ValueError("invalid FIRMS acquisition time")
    return int(padded)


def _finite_float(raw: str | None) -> float:
    if raw is None or not str(raw).strip():
        raise ValueError("missing numeric value")
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("numeric value is not finite")
    return value


def _optional_float(raw: str | None) -> float | None:
    if raw is None or not str(raw).strip():
        return None
    return _finite_float(raw)


def _optional_text(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    return value or None
