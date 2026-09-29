# Design Document: Data Ingestion

## Overview

The ingestion layer is a Python package at `/ingestion/` with one job per source. A supervisor process, the runner, starts each job run as its own OS process on that job's schedule. A crash, hang or memory blowup in one source therefore can't reach the others.

Every job runs the same three stages:

1. **Fetch** (network only): every HTTP call goes through one shared client with timeouts and tenacity retries.
2. **Parse** (no I/O): pure functions turn upstream payloads into validated rows and count rejected ones.
3. **Store** (database only): batched, idempotent upserts into lightweight tables in the `ingestion` schema.

Fetching finishes before storing starts, so a job never holds a database connection while it waits on an upstream (Requirement 4.4).

Decisions from the requirements review:

- Tables and code are global. The default AOI is India: 68.0°E to 97.5°E, 6.5°N to 37.5°N.
- NOAA HMS/GOES stays in the design. Over India its runs are recorded as `no_coverage` without sending a request. No India geostationary source is added.
- Point staging tables stay lightweight: lat, lon, timestamp and raw values, plus provenance. Clustering and the features layer turn them into the cluster object in `data-contracts.md`.

Also out of scope here: H3 indexing (owned by `/clustering/h3_partitioner`), DuckDB cold storage, and feature computation.

## Architecture

```mermaid
flowchart LR
    subgraph runner["Runner process"]
        sched["APScheduler<br/>one trigger per job"]
    end
    subgraph jobs["One OS process per run"]
        firms["firms"]
        hms["geostationary (HMS)"]
        osm["osm"]
        tropomi["tropomi"]
        optical["optical"]
        wind["weather (wind)"]
        lc["landcover"]
    end
    bf["backfill CLI<br/>scripts/backfill_partition.py"]
    sched -->|spawn, wait, kill on timeout| jobs
    jobs --> http["core.http<br/>httpx + tenacity"]
    bf --> http
    http --> up[("FIRMS, HMS, Geofabrik, Overpass,<br/>CDSE, USGS STAC, NCMRWF,<br/>NOMADS / NODD, WorldCover")]
    jobs --> db[("PostGIS<br/>schema: ingestion")]
    bf --> db
    db --> down["clustering, features, api"]
```

### Processes and entry points

| Command | What it does |
|---|---|
| `python -m ingestion runner` | Long-running supervisor. Validates config and the connection budget, then schedules every enabled job. |
| `python -m ingestion run <source>` | One scheduled run of one job. The runner calls this, and you can run it by hand. |
| `python -m ingestion backfill <source> --start YYYY-MM-DD --end YYYY-MM-DD [--aoi NAME] [--products A,B]` | Resumable backfill in its own process. `scripts/backfill_partition.py` is a thin wrapper. |
| `python -m ingestion migrate` | Applies Alembic migrations. |
| `python -m ingestion check` | Validates config, the secrets each enabled job needs, and the connection budget, without running anything. |

How the runner works:

- It registers one APScheduler interval trigger per enabled job (`max_instances=1`, `coalesce=True`, small start jitter). The thread pool has one thread per job, so a long run never delays another job's trigger.
- Each trigger creates a run ID and spawns `python -m ingestion run <source> --run-id <id>` with `subprocess.Popen`, which behaves the same on Windows and Linux. It then waits up to the job's `max_runtime_minutes`.
- On timeout it kills the child. If the child is killed, or exits non-zero while its run row still says `running`, the runner marks the run `failed` with the reason. (R1.4)
- The runner holds a one-connection engine for startup checks and those status updates. It never fetches data.

### Independence guarantees (Requirement 1)

| Failure | What contains it |
|---|---|
| A job crashes or runs out of memory | Separate OS process per run. The runner only sees an exit code. |
| An upstream hangs | Connect and read timeouts on every request, plus the per-run `max_runtime` kill. |
| An upstream is down | Retries end in a gap for that product and window. Other products and jobs carry on. |
| Two copies of the same job | `max_instances=1` in the scheduler, plus a lease row per job for manual runs. A run that finds the lease held records `skipped`. |
| Waiting on another source | No job reads another job's tables or leases. The only shared resources are the host network and the database connection budget, which is split up front. |
| The database is slow or down | Pool timeouts and bounded retries. The failure stays in the run that hit it. |

## Components and Interfaces

### Package layout

```
/ingestion/
  __main__.py              CLI: runner, run, backfill, migrate, check
  config/ingestion.yaml    non-secret defaults (AOIs, schedules, retries, pools)
  core/
    settings.py            secrets from env/.env, YAML config, env overrides
    logging.py             JSON logs, context fields, redaction
    http.py                shared client, retry policy, downloads, tokens, rate limits
    db.py                  engine factory, batched upserts, DB retry, partitions
    jobs.py                SourceJob base class, work units, run orchestration
    runs.py                job_runs, run_units, gaps, freshness, leases
    backfill.py            chunk planning and resume
    aoi.py                 AOI model and coverage checks
    errors.py              exception taxonomy
  runner.py                APScheduler supervisor
  firms/                   client.py, parse.py, job.py
  geostationary/           hms.py, job.py
  osm/                     extracts.py, parse.py, overpass.py, lookup.py, job.py
  tropomi/                 cdse.py, read.py, job.py
  optical/                 stac.py, job.py
  weather/                 gfs.py, ncmrwf.py, grib.py, wind.py, job.py
  landcover/               worldcover.py, job.py
  migrations/              Alembic env and versions
  tests/                   unit/, integration/, fixtures/
  Dockerfile
/scripts/backfill_partition.py
/docker/docker-compose.yml
/pyproject.toml            dependencies with exact pins
/environment.yml           conda-forge environment for Windows development
/.env.example
```

`tech.md` lists `sentinel2` and `weather` under `/ingestion/`. This design uses `optical`, since the module also covers Landsat, and adds `geostationary` for HMS. `weather` holds the wind job.

Runtime: Python 3.12. The Docker image is the reference runtime. For Windows development, use the conda-forge environment, since pyrosm, GDAL (through rasterio) and ecCodes (through cfgrib) have reliable Windows builds there.

Main dependencies: httpx, tenacity, SQLAlchemy 2.x, psycopg 3, Alembic, pydantic-settings, PyYAML, APScheduler 3.x, python-json-logger, pyrosm, shapely 2, numpy, xarray, netCDF4, cfgrib and rasterio. For tests: pytest, respx and hypothesis. Exact versions are pinned when the project is set up.

### Settings and secrets (core/settings.py)

- `Secrets` is a pydantic-settings `BaseSettings` that reads environment variables and `.env`. Every field is a `SecretStr`, so values are masked when printed: `FIRMS_MAP_KEY`, `CDSE_USERNAME`, `CDSE_PASSWORD`, `NCMRWF_USERNAME`, `NCMRWF_PASSWORD` and `DATABASE_URL`. (R3.1)
- `AppConfig` loads `ingestion/config/ingestion.yaml`. An environment variable with the `INGEST__` prefix can override any key, with `__` for nesting, for example `INGEST__JOBS__FIRMS__SCHEDULE_MINUTES=30`. (R3.6)
- Each job declares `required_secrets`. `run` checks them before anything else and exits with code 2, naming the missing variables. The run is recorded as failed and nothing else is affected. (R3.4)
- At startup every secret value is registered with the log redaction filter.
- The default AOI is `india: [68.0, 6.5, 97.5, 37.5]` (west, south, east, north). AOIs are plain bounding boxes, so any region works, including the whole world. (R3.7)

### Logging and redaction (core/logging.py)

- The standard `logging` module with a JSON formatter writes one line per event to stdout. Each line has `ts`, `level`, `event`, `source`, `product`, `run_id` and `window`, plus event fields. The orchestrator sets the context fields through `contextvars`. (R6.6)
- A `RedactingFilter` on every handler scrubs the final formatted text, so messages, arguments and tracebacks are all covered. It replaces every registered secret with `***`. It also masks the key segment of FIRMS URLs (`/csv/<key>/`) even when the key wasn't registered. (R3.5)
- The `httpx` and `httpcore` loggers are set to WARNING, because httpx logs full request URLs at INFO.
- Run records store error summaries produced by the same redaction function.

### Shared HTTP client (core/http.py)

```python
def make_client(cfg: HttpConfig) -> httpx.Client
def request(client: httpx.Client, method: str, url: str, *, source: str, product: str,
            params=None, headers=None, json=None, data=None,
            token: TokenProvider | None = None,
            limiter: RateLimiter | None = None) -> httpx.Response
def download(client: httpx.Client, url: str, dest: Path, *, source: str, product: str,
             token: TokenProvider | None = None, expected_md5: str | None = None) -> Path
```

- Timeouts: `httpx.Timeout(connect=..., read=..., write=..., pool=...)` from config. (R1.6)
- The retry policy is shared by every source through tenacity `Retrying`: (R2.1 to R2.4)
  - It retries on `httpx.TransportError` (connect errors, timeouts, dropped connections) and on HTTP 429 and 5xx, which `request` raises as `RetryableHTTPError`.
  - It waits `max(retry_after, exponential_jitter)`. `retry_after` comes from the `Retry-After` header on 429 and 503, in seconds or as an HTTP date. `exponential_jitter` is tenacity's `wait_exponential_jitter(initial=2, max=120)`.
  - It stops at `stop_after_attempt(max_attempts) | stop_after_delay(max_total_wait_s)`. Both have global defaults with per-source overrides.
  - Before each sleep it logs an `http_retry` event with source, product, attempt, status or error, and the wait.
- Other 4xx responses raise `NonRetryableHTTPError` straight away. A 404 raises its subclass `NotFoundError`, so HMS and NCMRWF can read it as "not published yet". (R2.5)
- For a 401 with a `TokenProvider`, the client invalidates the token, fetches a new one and retries once. A second 401 raises `AuthError`. (R2.7)
- When retries run out it raises `RetryExhaustedError` with the last status or error, redacted. The orchestrator turns this into a gap. (R2.6)
- `download` streams to a temp file under `INGEST_TMP_DIR` and renames it when complete. If the server accepts ranges, a retried download resumes with `Range`. It verifies the MD5 when one is given, as with Geofabrik.
- `RateLimiter` is a token bucket per host. Overpass and NOMADS use it, so we rarely hit their 429s at all.
- Requests carry a descriptive `User-Agent` from config.

### Database (core/db.py)

- `get_engine()` lazily creates one SQLAlchemy 2.x engine per process, using psycopg 3 (`postgresql+psycopg://`) and `QueuePool` with `pool_size`, `max_overflow`, `pool_timeout`, `pool_pre_ping=True`, `pool_recycle=1800` and `application_name=ingestion:<source>`. (R4.1, R4.2)
- `check_connection_budget()` runs at runner start, in `check`, and when a backfill starts. It adds up `pool_size + max_overflow` for the runner, each enabled job and `backfill.max_concurrent` backfill processes. If the total exceeds `database.connection_budget`, or the budget isn't below the server's `max_connections`, it refuses to start and prints each share. (R4.3)
- `bulk_upsert(table, columns, rows, conflict_cols, batch_size)` handles each batch in one short transaction. It COPYs the rows into a temporary table, then runs `INSERT INTO target SELECT ... ON CONFLICT (conflict_cols) DO NOTHING`, and returns the number of new rows. A connection is checked out only for the batch. (R4.4, R4.5, R5.6)
- On database errors (`OperationalError`, pool `TimeoutError`) it retries the whole batch with backoff. That's safe because batches are idempotent. If the retries run out it raises `DatabaseUnavailableError`. (R4.6)
- `ensure_month_partitions(table, months)` creates missing monthly partitions such as `fire_detections_y2026m09`, under a `pg_advisory_xact_lock` on the table name. Only processes writing that table take this lock, meaning the same source's live and backfill processes. The store step calls it before the first batch for each month.

### Jobs and orchestration (core/jobs.py, core/runs.py)

```python
@dataclass(frozen=True)
class Window:
    start: datetime   # inclusive, UTC
    end: datetime     # exclusive, UTC

@dataclass(frozen=True)
class WorkUnit:
    product: str
    aoi: Aoi
    window: Window

@dataclass
class ParseResult:
    rows: Iterable[dict]   # may be a generator for large payloads
    rejected: int = 0

class SourceJob(ABC):
    source: ClassVar[str]
    required_secrets: ClassVar[tuple[str, ...]] = ()

    def scheduled_units(self, now: datetime) -> list[WorkUnit]: ...
    def backfill_units(self, start: date, end: date, aoi: Aoi,
                       products: list[str] | None) -> list[WorkUnit]: ...
    def coverage(self, product: str) -> BaseGeometry | None: ...   # None means global
    def fetch(self, unit: WorkUnit) -> Any: ...                     # network only
    def parse(self, unit: WorkUnit, raw: Any) -> ParseResult: ...   # no I/O
    def store(self, unit: WorkUnit, result: ParseResult) -> StoreStats: ...  # database only
```

`execute(job, run_type, units)` drives every run, scheduled or backfill:

1. Acquire the lease (`scheduled:<source>` or `backfill:<backfill_id>`). If it's held, record a `skipped` run and exit 0. (R1.7)
2. Insert a `job_runs` row with status `running`. (R6.1)
3. For scheduled runs, append up to `gap_retry_limit` open gaps as extra units, oldest first. (R6.5)
4. Process each unit inside its own `try` block, so one product's failure can't stop the rest: (R1.8)
   - If the AOI doesn't intersect the product's coverage, record `no_coverage` without making a request. (R9.4)
   - Otherwise fetch, parse and store. Zero rows still counts as `succeeded`. (R6.4)
   - `NotPublishedYet` records a gap at INFO level and marks the unit `skipped`. (R9.5)
   - Any other error records a gap, marks the unit `failed` and logs at ERROR. That covers exhausted retries, non-retryable HTTP errors, format errors, database errors and unexpected exceptions. (R2.6)
   - Update `run_units`, advance `source_freshness`, and resolve the gap if this unit was a gap retry. (R6.3)
5. Close the run and release the lease. The run's status is:
   - `no_coverage` if every unit had no coverage,
   - `succeeded` if every unit either succeeded or had no coverage,
   - `failed` if every unit failed,
   - `partial` otherwise.

Leases are rows in `ingestion.job_leases`. A run acquires one with `INSERT ... ON CONFLICT DO UPDATE ... WHERE expires_at < now()`, and a heartbeat thread renews it every 30 s over a short-lived connection. The TTL is 120 s, so a killed run's lease expires on its own and no connection is held for the whole run.

Exit codes: 0 for a completed run (including partial, no_coverage and skipped), 1 for an unexpected crash, and 2 for configuration or secret errors.

### Backfill (core/backfill.py)

- `backfill_id` is a hash of the source, AOI, start, end and products, so running the same command again resumes the same backfill. (R7.5)
- The first invocation writes one `backfill_chunks` row per unit. Later invocations process chunks that are `pending` or `failed`, in time order, through the same `execute` path. Each chunk ends as `done`, `failed` (with attempts incremented) or `skipped`.
- Each backfill runs in its own process with its own lease and engine, so it never blocks scheduled jobs. (R7.6)

| Source | Chunk | Notes |
|---|---|---|
| FIRMS | 5 days per product | The Area API maximum. (R7.4) |
| HMS | 1 day | One file per day. `no_coverage` over India. |
| TROPOMI | 1 day | OFFL and RPRO streams by default. |
| Optical | 7 days | STAC search with paging. |
| Wind | 1 day | Four GFS cycles per day. |

- FIRMS product choice (R7.8, R7.9): the backfill reads `/api/data_availability/csv/<key>/ALL` once and maps each chunk to a product that covers it. Where both cover a date, it prefers standard processing (`*_SP`), the reprocessed science product, over NRT. Chunks that cross an availability boundary are split there. Dates no product covers are marked `skipped` and logged.
- FIRMS pacing (R7.7): a token bucket allows `transactions_per_10min` (default 3,000). That leaves 2,000 of the 5,000 MAP_KEY limit for live polling, which uses about 8 transactions every 15 minutes. Each request costs `day_range` tokens, because FIRMS counts a multi-day request as several transactions.

### Source adapters

#### FIRMS (firms/)

- Request: `GET https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{product}/{west},{south},{east},{north}/{day_range}[/{date}]`.
- Scheduled every 15 minutes, with one unit per product and AOI. Each unit requests `day_range = trailing_days` (default 2) with no date, which returns today and yesterday. (R8.1, R8.5)
- Parse with `csv.DictReader`, driven by the header row. If the first line doesn't contain `latitude` and `longitude`, it raises `UpstreamFormatError`. That catches "Invalid MAP_KEY" messages and HTML error pages, and nothing is stored. (R8.7)
- Column mapping: (R8.2 to R8.4)
  - `latitude` → `lat` and `longitude` → `lon`.
  - `bright_ti4` (VIIRS) or `brightness` (MODIS) → `brightness_temp`.
  - `daynight` → `day_night`.
  - `frp`, `acq_date`, `acq_time` (HHMM), `satellite`, `instrument`, `confidence`, `scan`, `track` and `version` are kept as they are.
  - `as_of` is `acq_date` plus HH:MM, in UTC.
- A row is rejected and counted if its lat or lon is out of range, its FRP is negative, its date or time can't be parsed, or `day_night` isn't D or N. (R8.6)
- Store in `fire_detections`, keyed on `(product, satellite, as_of, lat, lon)`.

#### Geostationary: NOAA HMS / GOES (geostationary/)

- Request: `https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Fire_Points/Text/{YYYY}/{MM}/hms_fire{YYYYMMDD}.txt`.
- Coverage is a configured polygon. The default is North America, 170°W to 50°W and 14.6°N to 72°N. With the India AOI every unit is `no_coverage` and no request is made. (R9.3, R9.4)
- Scheduled every 15 minutes, with units for today and yesterday (UTC). A 404 means the file isn't published yet, which records a gap at INFO level. (R9.1, R9.5)
- Parse as CSV driven by the header (`Lon, Lat, YearDay, Time, Satellite, Method, Ecosystem, FRP`). `as_of` combines `YearDay` (YYYYDDD) with `Time` (HHMM). An FRP of -999 becomes null. (R9.2)
- Only rows whose satellite matches `include_satellites` are kept, which is GOES by default. HMS also carries VIIRS and MODIS points that would duplicate FIRMS.
- Store in `geo_detections` with `product = 'HMS'`. Any future geostationary product implements the same `GeoProduct` interface (`coverage`, `fetch_day`, `parse`) and writes to the same table. (R9.7)

#### OpenStreetMap (osm/)

- Extract registry: Geofabrik's `index-v1.json` provides region IDs, `.osm.pbf` URLs and boundary polygons. The default config lists the India zone extracts rather than the single India file, because smaller files keep pyrosm's memory use manageable.
- Refreshed weekly. A region is skipped if its extract timestamp matches the active load. (R10.1)
- Fetch downloads the `.osm.pbf` and its `.md5`, and verifies the checksum. (R10.1)
- Parse: (R10.2, R10.3)
  - Call `pyrosm.OSM(path).get_data_by_custom_criteria(custom_filter=tag_filter, filter_type="keep", keep_nodes=True, keep_ways=True, keep_relations=True)`.
  - For each feature, the first key and value that match, in filter order, become `tag_key` and `tag_value`.
  - `lat` and `lon` come from shapely's `representative_point()`.
  - `as_of` is the extract's replication timestamp.
- Default tag filter:
  - `landuse`: industrial, quarry, farmland, farmyard, orchard
  - `man_made`: works, chimney, kiln, flare, petroleum_well, mineshaft, adit
  - `industrial`: any value (covers brickyard, refinery, mine and so on)
  - `power`: plant
- Versioned loads (R10.4):
  1. Insert an `osm_loads` row with status `loading`, using the region's boundary as its coverage.
  2. Bulk insert the features.
  3. In one transaction, mark the region's previous active load `superseded`, then mark the new load `active`.

  Readers use the view `osm_features_current`, which only shows active loads. They see either the old version or the new one, never a mix. Superseded loads are deleted in batches, keeping one for rollback.
- Overpass fallback, in `osm/lookup.py`, is called by the features layer. `features_near(lat, lon, radius_m)` works like this: (R10.5, R10.6)
  1. If an active extract covers the point, query the store.
  2. Otherwise, if a fresh Overpass load covers it (default TTL 30 days), query the store.
  3. Otherwise, send one Overpass QL query built from the same tag filter, with `out geom`. Its bbox is centered on the point and capped at `overpass.max_bbox_deg` (default 0.1°, about 11 km). Store the result as an `overpass` load, keyed by the bbox snapped to a 0.1° grid, then query the store.

  Overpass calls go through the shared client, so they back off on 429 and 504. A rate limit of one request per 10 seconds also applies. There's no bulk mode.

#### Sentinel-5P TROPOMI NO2 (tropomi/)

- Search uses CDSE OData `https://catalogue.dataspace.copernicus.eu/odata/v1/Products` with these filters, following `@odata.nextLink` for more pages: (R11.1)
  - collection `SENTINEL-5P` and `productType` `L2__NO2___`,
  - `OData.CSC.Intersects` with the AOI polygon,
  - `ContentDate/Start` inside the window.
- Stream, orbit, collection and processor version are read from the product name (`S5P_OFFL_L2__NO2____...`).
- Auth: `TokenProvider` gets a password-grant token from `https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token` with `client_id=cdse-public`. It caches the token until 60 s before expiry and uses the refresh token after that. When the refresh token expires too, it logs in again.
- Scheduled daily over the last 3 days. Products already in `no2_products` are skipped. The default streams are NRTI and OFFL, each stored separately. (R11.4)
- Fetch streams `https://download.dataspace.copernicus.eu/odata/v1/Products({id})/$value` to a temp file, about 450 MB per orbit. It checks for free disk space first.
- Parse uses xarray with the netCDF4 engine on group `PRODUCT`, reading `latitude`, `longitude`, `nitrogendioxide_tropospheric_column` (mol/m²), `qa_value` and `time_utc`. A pixel is kept only if: (R11.2, R11.3)
  - its value is finite,
  - its `qa_value` is at least `qa_min` (0.75),
  - its center is inside the AOI.

  `as_of` is the scanline time.
- Store a `no2_products` row, then `no2_pixels` in batches. The file is deleted once the load commits, or after a failed run. (R11.5)
- Bandwidth: whole orbit files are downloaded, and 2 to 4 a day intersect the India bbox. If that becomes a bottleneck, the CDSE Sentinel Hub Process API can return AOI subsets instead. Only `fetch` would change.

#### Optical: Sentinel-2 and Landsat (optical/)

- A small STAC `POST /search` client runs on the shared HTTP client and follows `next` links. It doesn't use pystac-client, so retries stay uniform. (R12.1)
- Sentinel-2: CDSE STAC `https://stac.dataspace.copernicus.eu/v1`, collection `sentinel-2-l2a`.
- Landsat: USGS LandsatLook STAC `https://landsatlook.usgs.gov/stac-server`, collection `landsat-c2l2-sr`, platforms LANDSAT_8 and LANDSAT_9.
- Endpoints and collection IDs are config values.
- Scheduled daily over the last 5 days, to allow for catalog delays. Backfill uses 7-day chunks.
- Parse: (R12.2, R12.3)
  - `scene_id` from `item.id`, plus `platform`.
  - `as_of` from `properties.datetime`.
  - `cloud_cover_pct` from `eo:cloud_cover`. Missing or negative values are stored as null and logged. Landsat night scenes report -1.
  - The processing level.
  - `tile` from the MGRS tile or the WRS path/row.
  - `footprint` from the item geometry.
  - `item_url` from the item's `self` link, which lists every asset link.
- Metadata only. No pixels are downloaded. (R12.4)

#### Wind: NCMRWF primary, GFS fallback (weather/)

GFS is always ingested for the whole AOI as the baseline, and NCMRWF is added on top inside its domain. `source` is part of the key, so the two never overwrite each other. The lookup `wind_at` returns NCMRWF wherever it has a value for that place and time, and GFS otherwise. The fallback is automatic, and the returned `source` says which model answered. (R13.2, R13.4, R5.7)

- GFS 0.25°:
  - Cycles run at 00, 06, 12 and 18 UTC. By default the job takes leads 0 and 3 from each cycle, which gives 3-hourly valid times.
  - Recent cycles, the last 7 days by default, come from the NOMADS grib filter `filter_gfs_0p25.pl`. The request asks for `var_UGRD`, `var_VGRD` and `lev_10_m_above_ground` over a `subregion` bbox, and returns a small GRIB2 file per lead. NOMADS retired OpenDAP in February 2026, so it isn't used.
  - Older cycles come from the NOAA open-data archive on AWS (`noaa-gfs-bdp-pds`). The adapter reads the `.idx` inventory and fetches only the 10 m UGRD and VGRD messages with HTTP Range requests. URL templates are configurable, since the archive layout has changed over the years. (R13.8)
- NCMRWF NCUM-R (4 km):
  - NCMRWF doesn't publish a public API, so the adapter is driven by config: `url_template`, `auth` (basic or token), `format` (GRIB2 or NetCDF), `cycles`, `leads` and `domain`. These get filled in once you have access. Until then credentials are unset, and each cycle records `unavailable: credentials_not_configured`.
  - NCMRWF counts as unavailable in three cases. The reason is logged and stored in `wind_cycles`: (R13.3)
    - credentials are missing,
    - retries run out or authentication fails,
    - a cycle isn't published within `publish_wait_h` (default 8 h) of its cycle time.
  - It's stored at `grid_stride` 3 by default, about 12 km. A 4 km model resolves features several grid lengths wide, so this keeps most of the real detail while cutting rows ninefold.
- Decoding uses xarray with cfgrib (ecCodes) for GRIB2, or plain xarray for NetCDF, then crops to the AOI. It computes `speed = hypot(u, v)` and `direction = (degrees(atan2(-u, -v)) + 360) % 360`, the meteorological "blowing from" direction. (R13.6, R13.7)
- There's no HRRR adapter, and the config schema rejects `hrrr` as a source. (R13.5)
- Lookup: `ingestion.wind_at(lat, lon, t, known_at DEFAULT NULL)` returns `speed_mps`, `direction_deg`, `source` and `as_of`. (R13.9)
  - It tries NCMRWF first, then GFS. For each source it takes the valid time nearest `t` within ±3 h, and the shortest lead for that valid time.
  - Within that time slice it takes the nearest grid point, but only within 1.5 grid spacings. That stops points outside NCMRWF's domain from snapping to its edge.
  - `known_at` limits results to rows with `ingested_at <= known_at`, for causal replays.

#### Land cover (landcover/)

- The default dataset is ESA WorldCover 2021 v200, 10 m, published as public 3°×3° COG tiles (`https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map/ESA_WorldCover_10m_2021_v200_{tile}_Map.tif`). (R14.1)
- Storage is a PostGIS raster table rather than point rows. Land cover is a dense static grid. At 0.0025° (about 275 m, finer than a VIIRS pixel), a one-byte raster takes roughly 150 MB for the India bbox. The same grid as rows would take several GB.
- Load:
  1. Work out which source tiles intersect the AOI. Tiles that don't exist (open ocean) return 404 and are skipped.
  2. Read each tile through GDAL's `/vsicurl/` at the target resolution with mode (majority) resampling. GDAL uses the COG's internal overviews, so only a small part of each tile is downloaded. Each source-tile read is wrapped in tenacity, and GDAL's own HTTP retries are also turned on.
  3. Write output tiles of 240×240 px. That divides the 1,200 px per 3° evenly, so each output tile comes from exactly one source tile.
  4. Insert each tile with `ST_FromGDALRaster`. This needs `postgis.gdal_enabled_drivers` to include `GTiff`, which the migration sets for the database.
- Each dataset version gets a `land_cover_versions` row (`as_of` = 2021-01-01), then its tiles. New versions load alongside old ones. (R14.2, R14.4)
- `land_cover_classes` maps codes to names:

  | Code | Class | Code | Class |
  |---|---|---|---|
  | 10 | tree cover | 60 | bare or sparse vegetation |
  | 20 | shrubland | 70 | snow and ice |
  | 30 | grassland | 80 | permanent water |
  | 40 | cropland | 90 | herbaceous wetland |
  | 50 | built-up | 95 | mangroves |
  | | | 100 | moss and lichen |
- Lookup: `ingestion.land_cover_at(lat, lon, dataset DEFAULT 'esa_worldcover', version DEFAULT NULL)` returns `class_code`, `class_name` and `as_of`. A NULL `version` means the latest ready one. It calls `ST_Value` on the tile that intersects the point. (R14.3)
- Runs on demand. `schedule_days` is off by default. (R14.5)

## Data Models

All tables live in the `ingestion` schema. It has its own Alembic history (`version_table_schema='ingestion'`), so other components can keep separate migrations.

Point tables store `lat` and `lon`, and derive `geom` as a generated column. That keeps inserts small and geometry always consistent.

The four time-series tables are range-partitioned by month on `as_of`. That keeps global volumes, per-partition backfills and later cold-data exports manageable.

```sql
-- Point staging tables ------------------------------------------------------

CREATE TABLE ingestion.fire_detections (             -- FIRMS VIIRS + MODIS
  detection_id    bigserial,
  product         text             NOT NULL,         -- VIIRS_SNPP_NRT, MODIS_SP, ...
  satellite       text             NOT NULL,
  lat             double precision NOT NULL,
  lon             double precision NOT NULL,
  geom            geometry(Point, 4326)
                  GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED,
  as_of           timestamptz      NOT NULL,         -- acq_date + acq_time, UTC
  acq_date        date             NOT NULL,
  acq_time        smallint         NOT NULL,         -- HHMM, UTC
  brightness_temp real,                              -- kelvin
  frp             real,                              -- MW
  day_night       char(1)          NOT NULL,         -- D | N
  instrument      text,
  confidence      text,                              -- VIIRS l/n/h, MODIS 0-100
  scan            real,
  track           real,
  version         text,
  ingested_at     timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (detection_id, as_of),
  UNIQUE (product, satellite, as_of, lat, lon)
) PARTITION BY RANGE (as_of);

CREATE TABLE ingestion.geo_detections (              -- HMS/GOES, future geostationary products
  detection_id    bigserial,
  product         text             NOT NULL,         -- HMS
  satellite       text             NOT NULL,
  lat             double precision NOT NULL,
  lon             double precision NOT NULL,
  geom            geometry(Point, 4326)
                  GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED,
  as_of           timestamptz      NOT NULL,
  frp             real,                              -- null where HMS reports -999
  method          text,
  ecosystem       smallint,
  ingested_at     timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (detection_id, as_of),
  UNIQUE (product, satellite, as_of, lat, lon)
) PARTITION BY RANGE (as_of);

CREATE TABLE ingestion.no2_products (
  product_key       serial      PRIMARY KEY,
  product_name      text        NOT NULL UNIQUE,     -- S5P_OFFL_L2__NO2____...
  cdse_id           uuid        NOT NULL,
  orbit             integer     NOT NULL,
  stream            text        NOT NULL,            -- NRTI | OFFL | RPRO
  processor_version text        NOT NULL,
  sensing_start     timestamptz NOT NULL,
  sensing_end       timestamptz NOT NULL,
  pixel_count       integer,
  ingested_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ingestion.no2_pixels (
  product_key  integer          NOT NULL REFERENCES ingestion.no2_products,
  scanline     smallint         NOT NULL,
  ground_pixel smallint         NOT NULL,
  lat          double precision NOT NULL,            -- pixel center
  lon          double precision NOT NULL,
  geom         geometry(Point, 4326)
               GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED,
  as_of        timestamptz      NOT NULL,            -- scanline time
  no2_trop     real             NOT NULL,            -- mol/m2, tropospheric column
  qa_value     real             NOT NULL,
  ingested_at  timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (product_key, scanline, ground_pixel, as_of)
) PARTITION BY RANGE (as_of);

CREATE TABLE ingestion.wind_grid (
  source        text             NOT NULL,           -- NCMRWF | GFS
  ref_time      timestamptz      NOT NULL,           -- forecast cycle
  lead_h        smallint         NOT NULL,
  as_of         timestamptz      NOT NULL,           -- valid time = ref_time + lead
  lat           double precision NOT NULL,
  lon           double precision NOT NULL,
  geom          geometry(Point, 4326)
                GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED,
  speed_mps     real             NOT NULL,
  direction_deg real             NOT NULL,           -- meteorological, blowing from
  ingested_at   timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (source, as_of, ref_time, lat, lon)
) PARTITION BY RANGE (as_of);

CREATE TABLE ingestion.wind_cycles (
  source     text        NOT NULL,
  ref_time   timestamptz NOT NULL,
  status     text        NOT NULL,                   -- ingested | unavailable
  reason     text,                                   -- credentials_not_configured, not_published, ...
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, ref_time)
);

-- Each point table also gets a GiST index on geom and a B-tree index on as_of.

-- Coverage and context tables -------------------------------------------------

CREATE TABLE ingestion.optical_scenes (
  scene_id         text        PRIMARY KEY,
  platform         text        NOT NULL,             -- sentinel-2a, landsat-9, ...
  provider         text        NOT NULL,             -- cdse | usgs
  as_of            timestamptz NOT NULL,             -- sensing time
  cloud_cover_pct  real,                             -- 0-100, null if unknown
  processing_level text        NOT NULL,             -- L2A | L2SP
  tile             text,                             -- MGRS tile or WRS path/row
  footprint        geometry(MultiPolygon, 4326) NOT NULL,
  item_url         text        NOT NULL,
  ingested_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ingestion.osm_loads (
  load_id       bigserial   PRIMARY KEY,
  kind          text        NOT NULL,                -- extract | overpass
  coverage_key  text        NOT NULL,                -- Geofabrik region id or snapped bbox
  coverage      geometry(MultiPolygon, 4326) NOT NULL,
  as_of         timestamptz NOT NULL,                -- extract or Overpass data timestamp
  status        text        NOT NULL,                -- loading | active | superseded | failed
  feature_count integer,
  started_at    timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz
);
CREATE UNIQUE INDEX osm_one_active_load
  ON ingestion.osm_loads (coverage_key) WHERE status = 'active';

CREATE TABLE ingestion.osm_features (
  load_id     bigint           NOT NULL REFERENCES ingestion.osm_loads ON DELETE CASCADE,
  osm_type    char(1)          NOT NULL,             -- n | w | r
  osm_id      bigint           NOT NULL,
  tag_key     text             NOT NULL,
  tag_value   text             NOT NULL,
  name        text,
  lat         double precision NOT NULL,             -- representative point
  lon         double precision NOT NULL,
  geom        geometry(Geometry, 4326) NOT NULL,
  as_of       timestamptz      NOT NULL,
  ingested_at timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (load_id, osm_type, osm_id, tag_key)
);

CREATE VIEW ingestion.osm_features_current AS
  SELECT f.* FROM ingestion.osm_features f
  JOIN ingestion.osm_loads l USING (load_id)
  WHERE l.status = 'active';

CREATE TABLE ingestion.land_cover_versions (
  version_id    smallserial      PRIMARY KEY,
  dataset       text             NOT NULL,           -- esa_worldcover
  version       text             NOT NULL,           -- v200
  cell_size_deg double precision NOT NULL,
  as_of         timestamptz      NOT NULL,           -- start of the reference year
  status        text             NOT NULL,           -- loading | ready
  UNIQUE (dataset, version, cell_size_deg)
);

CREATE TABLE ingestion.land_cover_tiles (
  tile_id     bigserial   PRIMARY KEY,
  version_id  smallint    NOT NULL REFERENCES ingestion.land_cover_versions,
  rast        raster      NOT NULL,                  -- 1 band, 8BUI, SRID 4326, nodata 0
  as_of       timestamptz NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON ingestion.land_cover_tiles USING gist (ST_ConvexHull(rast));

CREATE TABLE ingestion.land_cover_classes (
  dataset text     NOT NULL,
  code    smallint NOT NULL,
  name    text     NOT NULL,
  PRIMARY KEY (dataset, code)
);

-- Run tracking -------------------------------------------------------------------

CREATE TABLE ingestion.job_runs (
  run_id        uuid        PRIMARY KEY,
  source        text        NOT NULL,
  run_type      text        NOT NULL,                -- scheduled | backfill | manual
  status        text        NOT NULL,                -- running | succeeded | partial | failed | skipped | no_coverage
  started_at    timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz,
  error_summary text                                 -- redacted
);

CREATE TABLE ingestion.run_units (
  run_id        uuid        NOT NULL REFERENCES ingestion.job_runs,
  product       text        NOT NULL,
  aoi           text        NOT NULL,
  window_start  timestamptz NOT NULL,
  window_end    timestamptz NOT NULL,
  status        text        NOT NULL,                -- succeeded | failed | skipped | no_coverage
  fetched       integer     NOT NULL DEFAULT 0,
  stored        integer     NOT NULL DEFAULT 0,
  rejected      integer     NOT NULL DEFAULT 0,
  retries       integer     NOT NULL DEFAULT 0,
  error_summary text,
  PRIMARY KEY (run_id, product, aoi, window_start)
);

CREATE TABLE ingestion.gaps (
  source          text        NOT NULL,
  product         text        NOT NULL,
  aoi             text        NOT NULL,
  window_start    timestamptz NOT NULL,
  window_end      timestamptz NOT NULL,
  reason          text        NOT NULL,              -- not_published | retries_exhausted | http_4xx | format | database
  attempts        integer     NOT NULL DEFAULT 1,
  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  last_attempt_at timestamptz NOT NULL DEFAULT now(),
  resolved_at     timestamptz,
  PRIMARY KEY (source, product, aoi, window_start, window_end)
);

CREATE TABLE ingestion.source_freshness (
  source          text NOT NULL,
  product         text NOT NULL,
  latest_as_of    timestamptz,
  last_success_at timestamptz,
  last_status     text,
  PRIMARY KEY (source, product)
);

CREATE TABLE ingestion.job_leases (
  lease_key  text        PRIMARY KEY,                -- scheduled:firms, backfill:<id>
  holder     uuid        NOT NULL,
  expires_at timestamptz NOT NULL
);

CREATE TABLE ingestion.backfill_chunks (
  backfill_id  text        NOT NULL,
  source       text        NOT NULL,
  product      text        NOT NULL,
  aoi          text        NOT NULL,
  window_start timestamptz NOT NULL,
  window_end   timestamptz NOT NULL,
  status       text        NOT NULL DEFAULT 'pending', -- pending | done | failed | skipped
  attempts     integer     NOT NULL DEFAULT 0,
  last_error   text,
  updated_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (backfill_id, product, window_start)
);
```

Notes:

- NO2 pixel rows reference their product row for orbit, stream and version. Land-cover tiles carry `as_of` per tile. (R5.3, R5.5)
- Natural keys make every write idempotent. Different products never share a key, so NRT and SP detections, or NCMRWF and GFS wind, sit side by side. (R5.6, R5.7)
- Only migrations run DDL, apart from monthly partition creation. Partition DDL uses `IF NOT EXISTS`, so reruns are safe. (R5.8)

### Mapping to the cluster contract

| `data-contracts.md` field | Built from |
|---|---|
| `features.firms` (persistence_days, frp_mean, frp_var, day_night_ratio) | `fire_detections.as_of`, `frp`, `day_night` |
| `features.land_cover.class`, `as_of` | `land_cover_at()` |
| `features.osm.tag`, `distance_m`, `as_of` | `osm_features_current.tag_value`, `geom`, `as_of` |
| `features.no2.column_density`, `as_of` | `no2_pixels.no2_trop`, `as_of` |
| `features.optical.cloud_cover_pct`, `as_of` | `optical_scenes.cloud_cover_pct`, `as_of` |
| `features.wind.speed_mps`, `direction_deg`, `source`, `as_of` | `wind_at()` |

### Rough storage for the India bbox

| Table | Volume | Notes |
|---|---|---|
| `fire_detections` | 1k to 20k rows a day | Seasonal, peaking in the post-harvest burning season. |
| `geo_detections` | 0 | No HMS coverage over India. |
| `no2_pixels` | 200k to 350k rows a day | After QA filtering. The bbox includes neighboring land and sea. |
| `wind_grid` | about 120k rows a day (GFS), plus about 630k (NCMRWF at stride 3) | 3-hourly valid times. |
| `optical_scenes` | 100 to 300 rows a day | |
| `osm_features` | low millions per load | Mostly farmland polygons. |
| `land_cover_tiles` | about 2,500 tiles, about 150 MB | One-off per dataset version. |

## Configuration

Non-secret defaults live in `ingestion/config/ingestion.yaml`. An excerpt:

```yaml
aois:
  india: [68.0, 6.5, 97.5, 37.5]          # west, south, east, north
default_aoi: india

http:
  connect_timeout_s: 10
  read_timeout_s: 60
  max_attempts: 6
  max_total_wait_s: 900
  backoff_initial_s: 2
  backoff_max_s: 120
  rate_limits_per_minute: {overpass-api.de: 6, nomads.ncep.noaa.gov: 30}

database:
  pool_size: 2
  max_overflow: 1
  pool_timeout_s: 30
  batch_size: 5000
  connection_budget: 40                   # runner 1 + 7 jobs x 3 + 2 backfills x 3 = 28

runs: {gap_retry_limit: 3, lease_ttl_s: 120}

jobs:
  firms:
    schedule_minutes: 15
    max_runtime_minutes: 20
    trailing_days: 2
    products: [VIIRS_SNPP_NRT, VIIRS_NOAA20_NRT, VIIRS_NOAA21_NRT, MODIS_NRT]
  hms:
    schedule_minutes: 15
    coverage: [-170.0, 14.6, -50.0, 72.0]
    include_satellites: "(?i)^goes"
  tropomi: {schedule_hours: 24, lookback_days: 3, streams: [NRTI, OFFL], qa_min: 0.75}
  optical: {schedule_hours: 24, lookback_days: 5}
  wind:
    schedule_minutes: 60
    gfs: {leads: [0, 3], nomads_window_days: 7}
    ncmrwf: {url_template: null, cycles: ["00"], leads: [0, 3, 6, 9, 12, 15, 18, 21],
             grid_stride: 3, publish_wait_h: 8}
  osm: {schedule_days: 7, overpass: {max_bbox_deg: 0.1, ttl_days: 30}}
  landcover: {schedule_days: null, dataset: esa_worldcover, version: v200, cell_size_deg: 0.0025}

backfill:
  max_concurrent: 2
  firms: {chunk_days: 5, transactions_per_10min: 3000, prefer: SP}
```

`.env.example` lists every variable with placeholders only (R3.3):

```
FIRMS_MAP_KEY=your-firms-map-key
CDSE_USERNAME=you@example.com
CDSE_PASSWORD=change-me
NCMRWF_USERNAME=
NCMRWF_PASSWORD=
DATABASE_URL=postgresql+psycopg://sih:change-me@127.0.0.1:5432/sih
POSTGRES_USER=sih
POSTGRES_PASSWORD=change-me
POSTGRES_DB=sih
INGEST_TMP_DIR=./.ingest_tmp
```

## Correctness Properties

A property is a statement that should hold for every valid input, not just for hand-picked examples. Each property below becomes a Hypothesis property test, except Property 15, which is an integration test.

**Property 1: Idempotent storage.** For any batch of valid rows, storing it twice, or storing overlapping batches in any order, leaves the table exactly as if each row were stored once. Repeated rows count as zero new rows.
*Validates: Requirements 5.6, 7.5, 8.5*

**Property 2: Chunks tile the range.** For any start ≤ end and chunk limit L, the planned chunks are contiguous and don't overlap. Together they cover the range exactly, and each spans at most L days. This still holds after chunks are split at product-availability boundaries.
*Validates: Requirements 7.4, 7.8*

**Property 3: Resume skips finished work.** For any plan and any subset of chunks already marked done or skipped, resuming processes exactly the remaining chunks.
*Validates: Requirement 7.5*

**Property 4: FIRMS rows round-trip.** For any valid VIIRS or MODIS row, parsing returns the same lat, lon, brightness_temp, frp, day_night and satellite, with `as_of` equal to acq_date plus acq_time in UTC. For any row with one invalid field, that row is rejected and counted once, and every other row still parses.
*Validates: Requirements 8.2, 8.4, 8.6*

**Property 5: Non-CSV stores nothing.** For any response whose first line lacks the FIRMS header, parsing raises `UpstreamFormatError` and no row reaches the store.
*Validates: Requirement 8.7*

**Property 6: Retry bounds.** For any sequence of simulated responses:
- the number of attempts never exceeds `max_attempts`,
- each wait after a 429 or 503 carrying `Retry-After: r` is at least r,
- a non-retryable 4xx is attempted exactly once,
- a 401 with a token provider triggers exactly one refresh and retry.

*Validates: Requirements 2.1, 2.2, 2.3, 2.5, 2.7*

**Property 7: Secrets never reach output.** For any secret value, and any log call, exception or run error summary that contains it (including inside a FIRMS URL), the emitted text doesn't contain the secret.
*Validates: Requirement 3.5*

**Property 8: Connection budget.** For any pool settings, startup succeeds exactly when the summed maximum connections fit the budget and the budget is below `max_connections`.
*Validates: Requirement 4.3*

**Property 9: No coverage, no request.** For any AOI disjoint from a product's coverage, the unit is recorded as `no_coverage` and the mock transport receives no request.
*Validates: Requirement 9.4*

**Property 10: Products fail independently.** For any split of a run's units into failing and succeeding ones:
- every succeeding unit's rows are stored,
- every failing unit has a gap,
- the run status matches the split: succeeded, partial or failed.

*Validates: Requirements 1.8, 2.6, 6.1*

**Property 11: Wind conversion.** For any (u, v) with nonzero speed:
- `speed = hypot(u, v)`,
- `0 ≤ direction < 360`,
- rebuilding (u, v) from speed and direction matches within 1e-4,
- wind blowing from the north (u = 0, v < 0) gives a direction of 0.

*Validates: Requirements 13.6, 13.7*

**Property 12: Wind source preference.** For any location and time, `wind_at` returns NCMRWF when an NCMRWF value exists within the distance and time limits, and GFS otherwise. The returned `source` always matches the row it chose.
*Validates: Requirements 13.2, 13.4, 13.9*

**Property 13: NO2 filter.** For any pixel arrays, the stored pixels are exactly those with a finite column value, `qa_value ≥ qa_min` and a center inside the AOI.
*Validates: Requirements 11.2, 11.3*

**Property 14: Land-cover lookup.** For any point inside a loaded AOI, `land_cover_at` returns the value of the raster cell that contains the point.
*Validates: Requirement 14.3*

**Property 15: OSM refresh is atomic.** For any reader running during a refresh, `osm_features_current` returns either the complete previous load or the complete new load for that region, never a mix.
*Validates: Requirement 10.4*

## Error Handling

| Condition | Raised in | Unit status | Gap | Log level |
|---|---|---|---|---|
| Retryable HTTP error with attempts left | `http.request` | retried | no | WARNING per retry |
| Retries exhausted | `http.request` | failed | yes | ERROR |
| Non-retryable 4xx (400, 403, ...) | `http.request` | failed | yes | ERROR |
| Daily file or cycle not published yet (404) | adapter | skipped | yes | INFO |
| 401 after a token refresh | `http.request` | failed | yes | ERROR |
| Non-CSV, HTML or unexpected schema | parse | failed | yes | ERROR |
| One invalid row | parse | row rejected and counted | no | DEBUG |
| AOI outside coverage | orchestrator | no_coverage | no | INFO |
| Missing credential | job startup | run failed, exit code 2 | no | ERROR |
| Pool timeout or transient DB error, retries exhausted | `db` | failed | yes | ERROR |
| Unexpected exception in job code | orchestrator, caught per unit | failed | yes | ERROR with traceback |
| Crash, out of memory, or `max_runtime` exceeded | runner | run failed | no, the next trailing window and gaps cover it | ERROR |
| NCMRWF unavailable | wind job | recorded in `wind_cycles`, GFS continues | no | WARNING |

Every error summary written to the database or logs passes through the redaction function first.

## Testing Strategy

- **Unit tests** need no network and no database.
  - Parsers, with small fixtures:
    - FIRMS: VIIRS and MODIS CSV, an "Invalid MAP_KEY" message, an HTML error page.
    - HMS: a day file that includes -999 values.
    - OData and STAC: JSON pages.
    - TROPOMI: a synthetic netCDF built inside the test with the real group and variable names.
    - Wind: a tiny public GFS GRIB2 subset.
  - The HTTP policy through respx: 429 with `Retry-After`, 503 then success, 404, and 401 then refresh.
  - Redaction, chunk planning, and config and budget validation.
- **Property tests** with Hypothesis cover Properties 1 to 14. Those that touch the database (1, 12, 14) run in the integration suite.
- **Integration tests** run against PostGIS from docker compose, using a test database from `TEST_DATABASE_URL`. Migrations are applied once per session, and the suite is skipped when the database isn't reachable. They cover:
  - idempotent upserts and partition creation,
  - leases and OSM swap atomicity (Property 15),
  - `wind_at` and `land_cover_at`,
  - run, gap and freshness bookkeeping.
- **Isolation tests** run the runner with fake jobs that crash, hang past `max_runtime`, or fail every request, next to a healthy fake job. The healthy job must keep succeeding on schedule. (R1.3, R1.4)
- **Live smoke tests** are opt-in with `pytest -m live`, one per source, using real credentials from `.env`. The default test run makes no live upstream calls.
- Fixtures use placeholder keys only. (R3.2)

## Deployment and Security

- The store requires PostgreSQL 15 or newer with PostGIS 3.3 or newer. Both are checked as minimums, not exact versions: managed Postgres upgrades on its own schedule, so pinning an exact release breaks without warning. `Database.check()` enforces the floors and reports the versions it found.
- `/docker/docker-compose.yml` runs `postgis/postgis:15-3.3` with a named volume and credentials from `.env`. Its port is bound to `127.0.0.1` only, so the database isn't reachable from other machines. That image pins the floor versions deliberately, so local development exercises the oldest supported combination.
- Managed Postgres is supported as an alternative to the compose service by pointing `DATABASE_URL` at it. Hosted plans usually cap `max_connections` well below a local default, so `database.connection_budget` must stay under that cap; the startup check enforces this.
- An optional `ingestion` service, behind a compose profile, builds `/ingestion/Dockerfile` and runs the runner.
- The migration enables GDAL's `GTiff` driver so rasters can be built from bytes inside the database. Out-of-database rasters stay disabled.
- This spec exposes no network service. The runner has no HTTP endpoint.
- `.env` is listed in `.gitignore`, and `.env.example` holds placeholders only. Logs and run records are redacted.
- One database role is enough for the hackathon. Later, jobs can use a narrower writer role while migrations run as the owner.

## Risks and Open Items

1. **NCMRWF access.** Its API and file format aren't known until access is granted, so the adapter is driven by templates. GFS covers India until then.
2. **pyrosm memory.** Large extracts can need several GB of RAM. The zone extracts, a long `max_runtime` and the Docker or conda runtime keep this manageable.
3. **TROPOMI bandwidth.** Expect roughly 1 to 2 GB of downloads a day for India. The fallback plan is to switch `fetch` to the Sentinel Hub Process API.
4. **HMS format changes.** The parser reads column names from the header. A change in the file layout shows up as `UpstreamFormatError`, not as bad rows.
5. **Wind volume at global scale.** Stride and lead settings control it. Storing wind as PostGIS rasters is the upgrade path if needed.
6. **FIRMS standard-processing lag.** SP data trails real time by months. Because backfills pick products by availability, recent dates use NRT automatically.
7. **Land-cover accuracy.** The majority class is computed from the COG's overviews, not every 10 m pixel. A config flag can force full-resolution reads, at the cost of several GB of downloads for India.
