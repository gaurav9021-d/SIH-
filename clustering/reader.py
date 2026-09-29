"""Stream FIRMS detections from the ingestion store in causal order.

Keyset pagination on `(as_of, detection_id)` rather than OFFSET: the order is
stable across a partitioned table and the cost does not grow with the offset. A
connection is held only while a batch is being fetched, never while the engine is
computing on one.

This module reads `ingestion.fire_detections` and nothing else. Cluster formation
must not see land cover, OSM, NO2, optical or wind data.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import datetime

from sqlalchemy import text

from ingestion.core.db import Database
from ingestion.core.settings import Aoi

from clustering.core.models import Detection

DETECTION_TABLE = "ingestion.fire_detections"

_QUERY = text(
    f"""
    SELECT detection_id, as_of, lat, lon, frp, day_night, product, satellite
    FROM {DETECTION_TABLE}
    WHERE as_of >= :window_start
      AND as_of < :window_end
      AND as_of <= :evaluation_time
      AND lat BETWEEN :south AND :north
      AND lon BETWEEN :west AND :east
      AND (:all_products OR product = ANY(:products))
      AND (as_of, detection_id) > (:last_as_of, :last_id)
    ORDER BY as_of, detection_id
    LIMIT :batch_size
    """
)

_COUNT_QUERY = text(
    f"""
    SELECT count(*)
    FROM {DETECTION_TABLE}
    WHERE as_of >= :window_start
      AND as_of < :window_end
      AND as_of <= :evaluation_time
      AND lat BETWEEN :south AND :north
      AND lon BETWEEN :west AND :east
      AND (:all_products OR product = ANY(:products))
    """
)


def _bind(
    *,
    aoi: Aoi,
    window_start: datetime,
    window_end: datetime,
    evaluation_time: datetime,
    products: Sequence[str] | None,
) -> dict:
    return {
        "window_start": window_start,
        "window_end": window_end,
        "evaluation_time": evaluation_time,
        "south": aoi.south,
        "north": aoi.north,
        "west": aoi.west,
        "east": aoi.east,
        "all_products": products is None,
        "products": list(products) if products else [],
    }


def count_detections(
    database: Database,
    *,
    aoi: Aoi,
    window_start: datetime,
    window_end: datetime,
    evaluation_time: datetime,
    products: Sequence[str] | None = None,
) -> int:
    params = _bind(
        aoi=aoi,
        window_start=window_start,
        window_end=window_end,
        evaluation_time=evaluation_time,
        products=products,
    )

    def operation() -> int:
        with database.engine.connect() as connection:
            return int(connection.execute(_COUNT_QUERY, params).scalar_one())

    return database._retry("count_detections", operation)


def stream_detections(
    database: Database,
    *,
    aoi: Aoi,
    window_start: datetime,
    window_end: datetime,
    evaluation_time: datetime,
    products: Sequence[str] | None = None,
    batch_size: int = 20_000,
) -> Iterator[Detection]:
    """Yield detections ordered by acquisition time, in bounded batches."""
    base = _bind(
        aoi=aoi,
        window_start=window_start,
        window_end=window_end,
        evaluation_time=evaluation_time,
        products=products,
    )
    last_as_of = window_start
    last_id = -1

    while True:
        params = dict(base, last_as_of=last_as_of, last_id=last_id, batch_size=batch_size)

        def operation(params: dict = params) -> list:
            with database.engine.connect() as connection:
                return list(connection.execute(_QUERY, params))

        rows = database._retry("stream_detections", operation)
        if not rows:
            return

        for row in rows:
            yield Detection(
                detection_id=int(row.detection_id),
                as_of=row.as_of,
                lat=float(row.lat),
                lon=float(row.lon),
                frp=float(row.frp),
                day_night=str(row.day_night),
                product=str(row.product),
                satellite=str(row.satellite),
            )

        last_as_of = rows[-1].as_of
        last_id = int(rows[-1].detection_id)
        if len(rows) < batch_size:
            return
