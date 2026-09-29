# Requirements Document

## Introduction

SIH26162 ingests satellite thermal detections, groups them into persistent sites, and classifies each site as industrial_fire, gas_flare, agricultural_burning, mining_activity or wildfire. This spec covers the ingestion layer under `/ingestion/`. It pulls seven sources into a shared PostgreSQL 15 or newer + PostGIS 3.3 or newer store:

1. NASA FIRMS active fires (VIIRS and MODIS)
2. NOAA HMS geostationary fire detections (GOES)
3. OpenStreetMap industrial, energy, mining and agricultural features
4. Sentinel-5P TROPOMI NO2
5. Sentinel-2 and Landsat scene metadata
6. Wind from the NCMRWF 4 km regional model, with GFS 0.25° as fallback
7. Land cover

Each source runs as an independent job, and no job waits on another. Every external call retries transient failures with exponential backoff. Every time-series source supports backfilling historical ranges.

Out of scope: clustering, feature engineering (OSM distances, dispersion cones, optical structure classes), the model, the API, the dashboard, and downloading optical image pixels. Those components read what this layer stores.

The steering documents in `.kiro/steering/` apply to this spec: `data-contracts.md` (the cluster object this layer ultimately feeds), `ml-methodology.md` (causal time, Tier 1 and Tier 2 data, weather routing), `structure.md` (tech stack) and `tech.md` (repository layout).

Scope decisions from review: tables and code are global, and the default AOI is India. NOAA HMS/GOES stays in scope even though it has no coverage over India, and no India geostationary source is added.

## Glossary

- **Source**: one of the seven upstream datasets listed above.
- **Product**: a specific feed within a source, such as `VIIRS_SNPP_NRT` in FIRMS, GFS in wind, or Landsat in optical.
- **Job**: the independently runnable unit that ingests one source.
- **Run**: one execution of a job over one window. A run is either scheduled or part of a backfill.
- **Window**: the UTC time range a run covers.
- **AOI**: an area of interest, defined in configuration as a bounding box. There can be several.
- **as_of**: the UTC time a stored value describes, meaning its observation or validity time, as data-contracts.md requires. It is not the time the row was written.
- **ingested_at**: the UTC time a record was written to the store.
- **Retryable error**: an HTTP 429 or 5xx response, a connection error, or a timeout.
- **Gap**: a window that failed or wasn't published yet, recorded so a later run can retry it.

## Requirements

### Requirement 1: Independent job execution

**User Story:** As a pipeline operator, I want each source to run as its own job, so that a slow or failing source never delays or breaks the others.

#### Acceptance Criteria

1. The system SHALL provide one job per source under `/ingestion/`, and each job SHALL be runnable on its own from the command line.
2. The system SHALL run jobs concurrently, each on its own configurable schedule.
3. WHEN an external API is unreachable, THEN the system SHALL log the failure and continue running all other ingestion jobs unaffected.
4. WHEN a job crashes, raises an unhandled error, or exceeds its configured maximum run time, THEN the system SHALL end only that job's run, record it as failed, and keep all other jobs running.
5. The system SHALL NOT make any job wait for another job's data, locks or completion.
6. The system SHALL apply a configurable connect timeout and read timeout to every external request.
7. IF a scheduled run starts while the previous scheduled run of the same job is still in progress, THEN the system SHALL skip the new run and log that it was skipped.
8. WHEN one product within a source fails, THEN the system SHALL keep ingesting that source's other products.

### Requirement 2: Retries with exponential backoff

**User Story:** As a pipeline operator, I want every external call to retry transient failures with exponential backoff, so that rate limits and short outages don't lose data.

#### Acceptance Criteria

1. WHEN any external request fails with a retryable error, THEN the system SHALL retry it with exponential backoff and random jitter, implemented with tenacity. This SHALL apply to every upstream, including FIRMS, NOAA HMS/GOES, Copernicus Data Space, USGS, Overpass, NCMRWF, GFS, and the hosts that serve OSM extracts and land-cover files.
2. WHEN a 429 or 503 response includes a `Retry-After` header, THEN the system SHALL wait at least that long before the next attempt.
3. The system SHALL cap retries for each request at a configurable number of attempts and a configurable total wait.
4. The system SHALL log each retry with the source, product, attempt number, status or error, and wait time.
5. The system SHALL treat 4xx responses other than 401 and 429 as non-retryable.
6. IF a request fails with a non-retryable error, or still fails after its last allowed attempt, THEN the system SHALL log the failure, record the affected window as a gap, and end that product's run without affecting other products or jobs.
7. WHEN a request fails with HTTP 401 because an access token expired, THEN the system SHALL get a new token and retry the request once.

### Requirement 3: Secrets and configuration

**User Story:** As a developer, I want keys and credentials loaded from environment variables, so that no secret ends up in the code or the git history.

#### Acceptance Criteria

1. The system SHALL read MAP_KEY, Copernicus Data Space credentials, NCMRWF credentials, any other upstream credentials, and the database connection settings from environment variables, loading a local `.env` file when one exists.
2. The system SHALL NOT contain keys, passwords or tokens in source code, configuration files, tests or fixtures.
3. The repository SHALL include a `.env.example` file that lists every variable with a placeholder value, and SHALL exclude `.env` from git.
4. IF a credential that an enabled job needs is missing, THEN the system SHALL fail that job at startup with an error naming the missing variable, and SHALL keep all other jobs running.
5. The system SHALL redact secret values from logs, error messages and stored run records, including the MAP_KEY embedded in FIRMS request URLs.
6. The system SHALL read non-secret settings (AOIs, schedules, retry limits, timeouts, pool sizes, QA thresholds, OSM tag filters and enabled sources) from a configuration file, and SHALL let environment variables override them.
7. The system SHALL use India's bounding box as the default AOI, and SHALL support any AOI worldwide without schema or code changes.

### Requirement 4: Database connection management

**User Story:** As a pipeline operator, I want pooled, bounded database connections, so that parallel jobs don't exhaust PostGIS connections.

#### Acceptance Criteria

1. The system SHALL connect to PostGIS through SQLAlchemy with connection pooling enabled, using configurable `pool_size`, `max_overflow` and `pool_timeout`, with pre-ping to replace stale connections.
2. The system SHALL create one engine per job process and reuse it for the whole run.
3. IF the combined maximum connections (`pool_size` plus `max_overflow`) of all jobs and backfills that can run at once exceed the configured connection budget, or the budget isn't lower than the database's `max_connections`, THEN the system SHALL refuse to start and report each job's share.
4. The system SHALL NOT hold a database connection while waiting on an external request.
5. The system SHALL write records in batches of configurable size, each in its own short transaction.
6. WHEN a job can't get a connection within `pool_timeout`, or the database is briefly unreachable, THEN the system SHALL retry the write with exponential backoff and, if it still fails, fail only that run.

### Requirement 5: Shared store and data contract alignment

**User Story:** As a developer working on clustering, features or the model, I want every source stored with consistent geometry, time and freshness fields, so that I can build the features in data-contracts.md without guessing units or timestamps.

#### Acceptance Criteria

1. The system SHALL store each source in its own tables in the shared PostGIS store, with geometries in EPSG:4326 and a spatial index on every geometry column.
2. The system SHALL store all timestamps in UTC.
3. The system SHALL store `as_of` and `ingested_at` on every record.
4. The system SHALL store, for each source, the fields needed to derive that source's features in data-contracts.md, using the contract's field names and units where they match (for example `frp`, `cloud_cover_pct`, `speed_mps`, `direction_deg` and `source`).
5. The system SHALL record the upstream product and its version or processing level for every record, either on the record itself or through a reference to a product row, for example `VIIRS_SNPP_NRT` or `VIIRS_SNPP_SP`, TROPOMI `NRTI` or `OFFL`, or the GFS cycle and lead time.
6. WHEN a run ingests a window that was already ingested, THEN the system SHALL NOT create duplicate records.
7. The system SHALL keep records from different products side by side, and SHALL NOT overwrite or delete one product's records with another's (for example FIRMS NRT with standard-processing data, or GFS fallback wind with later NCMRWF wind), so that downstream code can reconstruct what was known at any time T.
8. The system SHALL create and upgrade its tables through versioned migrations that are safe to re-run.
9. The system SHALL keep point staging tables lightweight: location (lat, lon), timestamp, the raw measured values, and the provenance fields above.

### Requirement 6: Run tracking and sensor freshness

**User Story:** As a dashboard user, I want to know when each sensor was last updated and which windows are missing, so that I can judge how fresh each feature is.

#### Acceptance Criteria

1. WHEN a run starts or finishes, THEN the system SHALL record its source, product, run type (scheduled or backfill), window, status, counts of fetched, stored and rejected records, number of retries, and an error summary.
2. The system SHALL give each run one of these statuses: running, succeeded, partial, failed, skipped or no_coverage.
3. The system SHALL keep, for each source and product, the latest stored `as_of` and the time of the last successful run, to back the dashboard's per-sensor freshness timestamps.
4. WHEN a source returns no data for a window it covers (for example no fires, or every TROPOMI pixel fails QA), THEN the system SHALL record the run as succeeded with zero records, not as failed.
5. WHEN a scheduled run starts, THEN the system SHALL retry up to a configurable number of that source's recorded gaps in addition to its current window.
6. The system SHALL write logs as structured JSON lines that include the source, product, run ID and window.

### Requirement 7: Historical backfill

**User Story:** As a data scientist, I want to backfill any historical date range, so that I can build training data and spatial-block evaluations over past seasons.

#### Acceptance Criteria

1. The system SHALL support backfilling FIRMS data for an arbitrary historical date range.
2. The system SHALL support backfilling HMS fire points, TROPOMI NO2, optical scene metadata and wind for any date range their upstream archives cover.
3. The system SHALL start a backfill from the command line, given a source, a start date, an end date and an optional AOI.
4. WHEN a backfill range is longer than one upstream request allows, THEN the system SHALL split it into chunks that fit the limit, such as at most 5 days per FIRMS Area API request.
5. WHEN a backfill is interrupted or a chunk fails, THEN the system SHALL resume from the incomplete chunks on the next invocation without re-fetching completed ones.
6. WHILE a backfill is running, the system SHALL keep all scheduled jobs running on schedule.
7. The system SHALL pace FIRMS backfill requests so that backfill and live polling together stay under the MAP_KEY transaction limit, leaving configurable headroom for live polling.
8. WHEN a backfill range includes dates a product doesn't cover, THEN the system SHALL use a product that does, such as `VIIRS_SNPP_SP` instead of `VIIRS_SNPP_NRT` for older dates.
9. IF no product covers some dates in a backfill range, THEN the system SHALL skip those dates and log which dates were skipped.
10. The system SHALL store backfilled records with the same fields as live records.

### Requirement 8: NASA FIRMS active fires (VIIRS and MODIS)

**User Story:** As a data scientist, I want VIIRS and MODIS active fire detections from FIRMS, so that clustering has its primary thermal signal.

#### Acceptance Criteria

1. The system SHALL poll the FIRMS Area API on a configurable schedule for each configured product and AOI. The default products SHALL be `VIIRS_SNPP_NRT`, `VIIRS_NOAA20_NRT`, `VIIRS_NOAA21_NRT` and `MODIS_NRT`.
2. The system SHALL parse and store lat, lon, brightness_temp, frp, acq_date, acq_time, day_night and satellite for each detection, mapping VIIRS `bright_ti4` and MODIS `brightness` to brightness_temp in kelvin, and `daynight` to day_night.
3. The system SHALL also store each detection's product, and its confidence, scan, track, instrument and version where FIRMS provides them.
4. The system SHALL combine acq_date and acq_time (UTC, HHMM) into one UTC timestamp and use it as the record's `as_of`.
5. WHEN a scheduled FIRMS run starts, THEN the system SHALL request a trailing window of configurable length (2 days by default) so that late-arriving detections are captured.
6. IF a row fails validation (latitude or longitude out of range, negative FRP, unparseable date or time, or an unknown day_night value), THEN the system SHALL skip that row, count it as rejected, and keep processing the rest of the response.
7. IF FIRMS returns something other than the expected CSV, such as an invalid MAP_KEY message or an HTML error page, THEN the system SHALL treat it as a failure for that product and window and SHALL NOT store any of it.

### Requirement 9: Geostationary fire detections (NOAA HMS / GOES)

**User Story:** As a data scientist, I want geostationary fire detections between polar-orbit passes, so that short-lived burns aren't missed.

#### Acceptance Criteria

1. The system SHALL poll NOAA HMS fire points on a configurable schedule (every 15 minutes by default) for the current and previous UTC day.
2. The system SHALL store each detection's lat, lon, detection time as `as_of`, satellite, detection method, ecosystem code and FRP, storing FRP as null where HMS reports -999.
3. The system SHALL record the geographic coverage of each geostationary product.
4. WHEN an AOI lies entirely outside a geostationary product's coverage, THEN the system SHALL record that run as `no_coverage` rather than failed.
5. WHEN the file for a requested day isn't published yet, THEN the system SHALL record the window as a gap for a later run to retry, and SHALL log it as informational rather than as an error.
6. The system SHALL support backfilling HMS fire points for past dates from the HMS archive.
7. WHERE another geostationary fire product is configured, the system SHALL ingest it through the same interface into the same table, tagged with its product name.

### Requirement 10: OpenStreetMap features

**User Story:** As a data scientist, I want industrial, energy, mining and agricultural features from OpenStreetMap, so that detections can be placed in infrastructure context.

#### Acceptance Criteria

1. The system SHALL download the regional OSM extracts (`.osm.pbf`) listed in configuration on a configurable refresh interval, verify each file's checksum, and parse it offline with pyrosm.
2. The system SHALL extract features matching a configurable tag filter that covers at least industrial sites, oil and gas facilities including flares, power plants, mines and quarries, and agricultural land use.
3. The system SHALL store each feature's OSM type and ID, geometry, matched tag key and value, and the extract's timestamp as `as_of`.
4. WHEN a refreshed extract is loaded, THEN the system SHALL make the new version visible only once it has fully loaded, so readers never see a partial load.
5. WHEN a lookup is requested for a location outside every loaded extract, THEN the system SHALL query the Overpass API for a bounding box around that location no larger than a configured maximum size, and SHALL store the results tagged as Overpass data.
6. The system SHALL NOT use the Overpass API for bulk or region-wide downloads.

### Requirement 11: Sentinel-5P TROPOMI NO2

**User Story:** As a data scientist, I want daily TROPOMI tropospheric NO2 over each AOI, so that the features layer can check for NO2 downwind of a detection.

#### Acceptance Criteria

1. The system SHALL search the Copernicus Data Space Ecosystem on a configurable schedule (daily by default) for Sentinel-5P L2 NO2 products (`L2__NO2___`) that intersect each AOI, and download those not yet ingested.
2. The system SHALL store, for each pixel inside the AOI, the tropospheric NO2 column in mol/m², its qa_value, its center location, its measurement time as `as_of`, the orbit number, the processing stream (NRTI, OFFL or RPRO) and the product version.
3. The system SHALL discard pixels whose qa_value is below a configurable threshold (0.75 by default).
4. WHEN a later processing stream is published for an orbit that was already ingested, such as OFFL after NRTI, THEN the system SHALL store it as separate records tagged with that stream.
5. WHEN a product has been parsed and stored, THEN the system SHALL delete its downloaded file.

### Requirement 12: Sentinel-2 and Landsat scene metadata

**User Story:** As a data scientist, I want scene metadata and cloud cover for Sentinel-2 and Landsat, so that the model knows when a clear optical spot-check is possible.

#### Acceptance Criteria

1. The system SHALL search on a configurable schedule (daily by default) for Sentinel-2 L2A and Landsat 8/9 Collection 2 Level-2 scenes that intersect each AOI.
2. The system SHALL store each scene's ID, platform, sensing time as `as_of`, footprint, cloud_cover_pct (0 to 100), processing level, tile or path/row, and links to its assets.
3. IF a scene has no cloud cover value, THEN the system SHALL store cloud_cover_pct as null and log it.
4. The system SHALL ingest scene metadata only, without downloading image pixels.

### Requirement 13: Wind (NCMRWF primary, GFS fallback)

**User Story:** As a data scientist, I want 10 m wind for every AOI, using NCMRWF's 4 km model over India and GFS elsewhere, so that dispersion features use the best wind available.

#### Acceptance Criteria

1. The system SHALL ingest 10 m u and v wind from the NCMRWF 4 km regional model (NCUM-R) for each new forecast cycle, for the parts of each AOI inside the NCMRWF domain.
2. WHEN NCMRWF access is unavailable, THEN the system SHALL fall back to GFS automatically and mark the wind record's source field.
3. IF NCMRWF credentials aren't configured, requests still fail after retries (including authentication failures), or an expected cycle isn't published within a configurable wait, THEN the system SHALL treat NCMRWF as unavailable and log which condition applied.
4. The system SHALL use GFS 0.25° for the parts of each AOI outside the NCMRWF domain.
5. The system SHALL NOT use HRRR.
6. The system SHALL store, for each grid cell and valid time, speed_mps and direction_deg derived from u and v, the `source` field (`NCMRWF` or `GFS`), the forecast reference time, the lead time, and the valid time as `as_of`.
7. The system SHALL express direction_deg in the meteorological convention: the direction the wind blows from, in degrees clockwise from true north.
8. WHEN a wind backfill covers times older than the GFS real-time service keeps, THEN the system SHALL read GFS from a historical archive.
9. The system SHALL store wind so that speed, direction, source and `as_of` can be looked up for any location and time within an AOI.

### Requirement 14: Land cover

**User Story:** As a data scientist, I want a land-cover class for any location, so that Tier 1 classification has land-use context.

#### Acceptance Criteria

1. The system SHALL load a configured land-cover dataset and version (ESA WorldCover by default) for each AOI, recording the dataset name and version, and using the dataset's reference year as `as_of`.
2. The system SHALL store each dataset's native class codes with readable class names.
3. The system SHALL allow the land-cover class at any point within an AOI to be looked up.
4. WHEN a new dataset version is configured, THEN the system SHALL load it alongside earlier versions and keep those queryable.
5. The system SHALL run the land-cover job on demand, and optionally on a configurable interval.

## Upstream references

- FIRMS Area API (products, 1 to 5 days per request, data availability): https://firms.modaps.eosdis.nasa.gov/api/area/
- FIRMS MAP_KEY limits (5,000 transactions per 10 minutes): https://firms.modaps.eosdis.nasa.gov/api/map_key/
- NOAA HMS fire point archive: https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Fire_Points/Text/
- Copernicus Data Space access tokens: https://documentation.dataspace.copernicus.eu/APIs/Token.html
