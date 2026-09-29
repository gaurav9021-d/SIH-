# SIH26162 FIRMS Backfill

This is the first runnable ingestion vertical slice. It retrieves NASA FIRMS VIIRS and MODIS detections, parses their acquisition times in UTC, and writes lightweight, idempotent point records into pooled PostGIS storage.

## Target dataset

The requested India backfill is **2025-11-01 through 2025-11-20, inclusive**. Internally it is represented as the half-open UTC interval `[2025-11-01T00:00:00Z, 2025-11-21T00:00:00Z)` so November 20 cannot be dropped accidentally.

The default India AOI is the rectangular regional envelope:

```text
west=68.0, south=6.5, east=97.5, north=37.5
```

The backfill handles four sensor families independently:

- VIIRS Suomi NPP: prefer `VIIRS_SNPP_SP`, otherwise use `VIIRS_SNPP_NRT`
- VIIRS NOAA-20: prefer `VIIRS_NOAA20_SP`, otherwise use `VIIRS_NOAA20_NRT`
- VIIRS NOAA-21: `VIIRS_NOAA21_NRT`
- MODIS Terra/Aqua: prefer `MODIS_SP`, otherwise use `MODIS_NRT`

Before planning, a live run queries FIRMS data availability. It chooses the first candidate that covers each day, splits at product changes, and caps every Area API request at five days. Products fail independently. Re-running the same command refreshes availability and resumes unfinished chunks without re-fetching completed chunks.

> FIRMS detections are an observational reference for tuning spatiotemporal clustering density; they are not verified agricultural-burning class labels. Keep supervised label construction separate and follow `.kiro/steering/ml-methodology.md`.

## 1. Install

Python 3.12 through 3.14 is supported. From the repository root:

```powershell
python -m pip install -e .
```

Dependencies are pinned in `pyproject.toml`.

## 2. Configure secrets

Copy the template, then edit the copy locally:

```powershell
Copy-Item .env.example .env
```

Set:

- `FIRMS_MAP_KEY`: a free NASA FIRMS MAP_KEY
- `POSTGRES_PASSWORD`: a local database password
- `DATABASE_URL`: the matching `postgresql+psycopg://...` URL

`MAP_KEY` is accepted as a legacy alias, but `FIRMS_MAP_KEY` takes precedence. `.env` is ignored by git. Never paste a key into source code or command arguments.

## 3. Start and migrate PostGIS

With Docker installed:

```powershell
docker compose --env-file .env -f docker/docker-compose.yml up -d postgis
python -m ingestion migrate
python -m ingestion check
```

You may instead point `DATABASE_URL` at any PostgreSQL 15 or newer server with PostGIS 3.3. Managed Postgres works: Supabase currently serves PostgreSQL 17 with PostGIS 3.3.7. Note that hosted plans often cap `max_connections` well below a local default, so keep `database.connection_budget` under that cap.

## 4. Inspect the exact plan without credentials

```powershell
python -m ingestion backfill firms --start 2025-11-01 --end 2025-11-20 --aoi india --dry-run
```

Dry-run performs no network or database operations and assumes each family's preferred product. The live run refreshes actual FIRMS availability before storing a plan.

## 5. Run or resume the backfill

```powershell
python -m ingestion backfill firms --start 2025-11-01 --end 2025-11-20 --aoi india
```

The equivalent convenience wrapper is:

```powershell
python scripts/backfill_partition.py --start 2025-11-01 --end 2025-11-20 --aoi india
```

To request explicit products instead of default family routing:

```powershell
python -m ingestion backfill firms --start 2025-11-01 --end 2025-11-20 --aoi india --products VIIRS_SNPP_SP,MODIS_SP
```

Every invocation has a stable `backfill_id` based on the dates, AOI coordinates, product families and planner version. Completed chunks remain `done`; failed chunks are retried. Dates missing from current FIRMS availability remain `waiting` and are reconsidered on the next identical invocation.

FIRMS quota reservations are coordinated through PostGIS by a non-secret MAP_KEY fingerprint. Backfill is capped at 3,000 transactions per 10 minutes, leaving 2,000 of the documented 5,000 limit for live polling. Network waits never hold a database connection.

## Inspect results

```sql
SELECT product, count(*) AS detections,
       min(as_of) AS first_detection,
       max(as_of) AS last_detection
FROM ingestion.fire_detections
WHERE as_of >= TIMESTAMPTZ '2025-11-01 00:00:00+00'
  AND as_of <  TIMESTAMPTZ '2025-11-21 00:00:00+00'
GROUP BY product
ORDER BY product;
```

Approximate Punjab, Haryana and western Uttar Pradesh density (use administrative polygons for analysis, not these coarse bounds):

```sql
SELECT date_trunc('day', as_of) AS day, count(*) AS detections
FROM ingestion.fire_detections
WHERE as_of >= TIMESTAMPTZ '2025-11-01 00:00:00+00'
  AND as_of <  TIMESTAMPTZ '2025-11-21 00:00:00+00'
  AND geom && ST_MakeEnvelope(73.5, 25.8, 81.0, 32.7, 4326)
GROUP BY day
ORDER BY day;
```

## Failure behavior

HTTP connection failures, timeouts, 429 responses and 5xx responses retry with tenacity exponential backoff and jitter. `Retry-After` on 429 or 503 sets a minimum wait. Other 4xx responses fail only their product chunk. Failures, gaps, per-product freshness and chunk progress are stored in the `ingestion` schema, while logs are JSON lines with the MAP_KEY redacted.

## Upstream references

- [NASA FIRMS Area API](https://firms.modaps.eosdis.nasa.gov/api/area/)
- [NASA FIRMS MAP_KEY and transaction limit](https://firms2.modaps.eosdis.nasa.gov/api/map_key/)

Upstream documentation details are paraphrased for licensing compliance.
