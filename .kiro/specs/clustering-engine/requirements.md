# Requirements Document

## Introduction

SIH26162 classifies persistent thermal sites, not individual satellite pixels. This spec covers the clustering stage under `/clustering/`. It reads raw FIRMS detections from the PostGIS store built by the `data-ingestion` spec and emits persistent cluster objects, each representing one facility or event rather than one pixel ping.

The algorithm is spatiotemporal DBSCAN applied **only** to FIRMS VIIRS and MODIS detections. No other source participates in cluster formation. Land cover, OSM, TROPOMI NO2, optical scenes and wind are enrichment inputs for later stages and must not influence cluster geometry or membership.

Two properties dominate this design:

1. **Causality.** At any evaluation timestamp T, the engine may read only detections whose acquisition time is at or before T. A detection acquired after T can never alter a cluster that existed at T. This is directly testable by replaying the same detection stream to different cutoffs.
2. **Bounded extent.** A cluster must stay at facility or field scale. A runaway cluster spanning many kilometres is a failure, not a large site.

### Correction carried into the requirements

The request asks that the epsilon bound prevent adjacent fires from merging into runaway clusters above 2 km. Epsilon alone cannot do that. DBSCAN forms clusters by transitive density connectivity: if A is within epsilon of B and B is within epsilon of C, all three join one cluster even when A and C are far apart. With epsilon at 750 m, a chain of detections spaced 700 m apart merges into a single cluster of unbounded length. Punjab and Haryana stubble burning in November produces exactly that geometry, so an epsilon-only rule would produce super-clusters tens of kilometres across.

Epsilon is therefore kept as the neighbourhood bound, and a separate hard extent limit (Requirement 4) enforces the 2 km ceiling. Because that limit constrains cluster growth, the algorithm is diameter-bounded DBSCAN rather than textbook DBSCAN, and the deviation is stated explicitly in the output metadata.

### Scope boundaries

In scope: reading FIRMS detections, cross-satellite observation reconciliation, causal spatiotemporal clustering, cluster lifecycle and persistence, the FIRMS feature block of the cluster contract, a CLI entrypoint, and a stress-test harness.

Out of scope: enrichment from any non-FIRMS source, dispersion or glint filtering, classification and calibration, anomaly detection, the API and the dashboard. Those stages read what this stage persists.

The steering documents in `.kiro/steering/` apply: `data-contracts.md` (cluster object shape), `ml-methodology.md` (causal integrity, Tier 1 and Tier 2 separation, labelling constraints), `structure.md` (PostGIS, H3, DuckDB, in-database or scikit-learn clustering) and `tech.md` (`/clustering/` holds `dbscan`, `h3_partitioner` and `spatial_dedup`).

The upstream detection table is `ingestion.fire_detections`, monthly range-partitioned on `as_of`, with `lat`, `lon`, a generated EPSG:4326 `geom`, `as_of` (UTC acquisition time), `acq_date`, `acq_time`, `brightness_temp`, `frp`, `day_night`, `product`, `satellite`, `confidence`, and the provenance fields `source_retrieved_at` and `ingested_at`.

## Glossary

- **Detection**: one row in `ingestion.fire_detections`, meaning one satellite pixel flagged as fire at one acquisition time.
- **Observation**: one physical fire sighting after reconciling near-duplicate detections of the same fire by different satellites or products at nearly the same moment.
- **Cluster**: a persistent group of observations treated as one facility or event.
- **Evaluation time (T)**: the simulated "now" a run is evaluated at. Only detections with `as_of <= T` may be read.
- **Trailing window**: the causal span ending at T that the engine inspects.
- **Linkage gap**: the time difference between two observations being tested for density connectivity.
- **Inactivity**: the time since a cluster last admitted an observation.
- **Open cluster**: a cluster still eligible to admit new observations.
- **Closed cluster**: a cluster whose inactivity exceeded the configured cutoff. It is immutable.
- **Extent (diameter)**: the greatest distance between any two member observations of a cluster.
- **Noise**: an observation that meets no density criterion and belongs to no cluster.
- **Run**: one execution of the clustering engine over a date range at a given evaluation time.

## Requirements

### Requirement 1: FIRMS-only clustering input

**User Story:** As a data scientist, I want clusters formed from thermal detections alone, so that infrastructure context can never bias which detections group together.

#### Acceptance Criteria

1. The system SHALL read cluster input exclusively from `ingestion.fire_detections`.
2. The system SHALL NOT read, join or otherwise consult OSM, land cover, TROPOMI NO2, optical scene or wind data while forming clusters.
3. The system SHALL include every configured FIRMS product and satellite in one clustering pass, so a single fire observed by several satellites yields one cluster rather than one cluster per satellite.
4. The system SHALL support restricting a run to an explicit product list for diagnostics, and SHALL record the product set used by the run.
5. The system SHALL filter input to a configurable area of interest, defaulting to the same India bounding box as ingestion.
6. IF the detection table is empty for the requested range and area, THEN the system SHALL complete successfully, report zero clusters, and persist nothing.

### Requirement 2: Spatiotemporal DBSCAN parameters

**User Story:** As a data scientist, I want the neighbourhood and time parameters expressed as validated configuration, so that tuning never requires editing code.

#### Acceptance Criteria

1. The system SHALL read the spatial epsilon from configuration, defaulting to 750 metres.
2. The system SHALL reject any configuration whose spatial epsilon exceeds 750 metres, and SHALL refuse to start rather than clustering with a wider neighbourhood.
3. The system SHALL reject a spatial epsilon that is not greater than zero.
4. The system SHALL measure spatial distance in metres on the WGS 84 ellipsoid or an equivalent geodesic measure, and SHALL NOT treat degrees of latitude and longitude as interchangeable distances.
5. The system SHALL read the minimum-neighbours parameter from configuration and SHALL record the value used by every run.
6. The system SHALL read the inactivity cutoff from configuration, defaulting to 7 days, and SHALL close a cluster once its inactivity exceeds that cutoff.
7. The system SHALL read the maximum linkage gap from configuration, defaulting to the inactivity cutoff, and SHALL treat two observations as density-connected only when they are within both the spatial epsilon and that gap.
8. The system SHALL persist, with every cluster, the parameter set and algorithm version that produced it.
9. WHEN any clustering parameter changes, THEN the system SHALL NOT silently mix clusters produced under different parameters in one result set.

### Requirement 3: Causal temporal integrity and replay determinism

**User Story:** As a data scientist, I want clustering to depend only on the past, so that evaluation at a historical timestamp reflects what was genuinely knowable then.

#### Acceptance Criteria

1. The system SHALL accept an evaluation timestamp T and SHALL read only detections whose `as_of` is at or before T.
2. The system SHALL process observations in non-decreasing acquisition-time order, using a deterministic tiebreak for identical timestamps.
3. The system SHALL NOT allow any observation acquired after a cluster's closure to modify that cluster's identity, geometry, membership, timing or FIRMS features.
4. The system SHALL produce byte-identical cluster output for repeated runs over the same input set, evaluation time and configuration.
5. WHEN the same detection stream is replayed to two evaluation times T1 and T2 where T1 is earlier than T2, THEN every cluster state the T2 run reports as of T1 SHALL equal the corresponding state from the T1 run.
6. The system SHALL expose the incremental clustering step as a pure function of prior cluster state plus the next observation, so that the property in criterion 5 can be unit-tested without a database.
7. The system SHALL apply event-time causality only, using acquisition time as the single causal clock, and SHALL NOT require a knowledge-time bound.
8. The system SHALL record the evaluation time on every run, and SHALL document that a replay over a range whose standard-processing data has since been backfilled can legitimately yield different clusters, because the input set itself changed.
9. The system SHALL derive cluster identifiers deterministically from causal inputs, so that a replay reproduces the same identifiers rather than assigning new random or sequential values.

### Requirement 4: Bounded cluster extent

**User Story:** As a dashboard user, I want every cluster to stay at site scale, so that a single row describes one facility or burn event instead of a merged regional smear.

#### Acceptance Criteria

1. The system SHALL read a maximum cluster diameter from configuration, defaulting to 2000 metres.
2. The system SHALL ensure that no persisted cluster has an extent exceeding the configured maximum diameter.
3. WHEN admitting an observation to a cluster would push that cluster's extent beyond the maximum diameter, THEN the system SHALL NOT admit it, and SHALL instead assign it to another eligible cluster or start a new cluster.
4. The system SHALL apply the extent rule deterministically, so that two runs over the same ordered input partition observations identically.
5. The system SHALL verify the extent invariant for every cluster it persists, and SHALL fail the run rather than persist a violating cluster.
6. The system SHALL report, for each run, the distribution of cluster extents including the maximum observed value.
7. The system SHALL record in output metadata that clustering is diameter-bounded, so downstream consumers do not assume textbook DBSCAN connectivity.
8. WHERE a dense region yields many adjacent bounded clusters, the system SHALL keep them as separate clusters and SHALL NOT merge them in a later pass.

### Requirement 5: Cross-satellite observation reconciliation

**User Story:** As a data scientist, I want near-simultaneous detections of one fire by different satellites treated as one sighting, so that density thresholds and detection counts are not inflated by sensor overlap.

#### Acceptance Criteria

1. The system SHALL reconcile detections that represent the same physical fire sighting across different products or satellites within a configurable space and time tolerance.
2. The system SHALL keep reconciliation tolerances configurable and SHALL default the spatial tolerance to no more than the spatial epsilon and the temporal tolerance to a small fraction of the inactivity cutoff.
3. The system SHALL record, for each observation, which detections it represents, including their products and satellites.
4. The system SHALL apply density and minimum-neighbour tests to reconciled observations rather than to raw detections.
5. The system SHALL report both the raw detection count and the reconciled observation count for every cluster.
6. The system SHALL perform reconciliation deterministically and causally, so it never depends on data acquired after the observation being reconciled.
7. IF reconciliation is disabled in configuration, THEN the system SHALL cluster raw detections and SHALL record that reconciliation was off.

### Requirement 6: Cluster identity, lifecycle and persistence

**User Story:** As a pipeline operator, I want clusters to persist and accumulate across runs, so that a site keeps one identity as new detections arrive over days.

#### Acceptance Criteria

1. The system SHALL persist clusters in the shared PostGIS store, in its own tables, with geometries in EPSG:4326 and spatial indexes on geometry columns.
2. The system SHALL store all timestamps in UTC.
3. The system SHALL give each cluster a stable identifier that persists as the cluster admits further observations.
4. The system SHALL maintain, for every cluster, an open or closed state derived from the inactivity cutoff.
5. WHEN an observation falls within the spatial epsilon and linkage gap of an open cluster and respects the extent limit, THEN the system SHALL admit it to that cluster and advance the cluster's last-detected time.
6. WHEN a cluster's inactivity exceeds the cutoff, THEN the system SHALL close it, and SHALL NOT admit later observations into it.
7. WHEN a new observation appears near a closed cluster, THEN the system SHALL start a new cluster rather than reopening the closed one.
8. The system SHALL persist cluster membership so that every cluster's constituent detections can be listed.
9. WHEN a run reprocesses a range that was already clustered, THEN the system SHALL converge on the same clusters rather than duplicating them.
10. The system SHALL create and upgrade its tables through versioned migrations that are safe to re-run.
11. The system SHALL record observations classified as noise, separately from clusters, so that later stages can inspect isolated detections without those detections forming clusters.

### Requirement 7: Cluster object output conforming to the data contract

**User Story:** As a developer of the features, model and API stages, I want clusters emitted in the documented contract shape, so that downstream code needs no translation layer.

#### Acceptance Criteria

1. The system SHALL emit each cluster in the JSON shape defined by `data-contracts.md`, including `cluster_id`, `centroid`, `bounding_box`, `first_detected`, `last_detected` and `detection_count`.
2. The system SHALL populate the `features.firms` block with `persistence_days`, `frp_mean`, `frp_var`, `day_night_ratio` and an `as_of` freshness timestamp.
3. The system SHALL derive `persistence_days` from the span between the cluster's first and last detection, and SHALL define the units and the single-detection case explicitly.
4. The system SHALL derive `frp_mean` and `frp_var` from member FRP values, and SHALL define the variance convention and the single-detection case explicitly.
5. The system SHALL derive `day_night_ratio` from member day and night flags, and SHALL define its orientation and the zero-denominator case explicitly.
6. The system SHALL set the `features.firms.as_of` timestamp to the latest member acquisition time, never to the time the row was written.
7. The system SHALL leave the `land_cover`, `osm`, `no2`, `optical` and `wind` feature blocks, the `classification` block and the `anomaly` block unset or null, because those belong to later stages.
8. The system SHALL compute the centroid and bounding box from member observation coordinates, and SHALL keep the bounding box consistent with the persisted extent.
9. The system SHALL NOT emit raw latitude and longitude as model features, in line with the steering constraint, while still exposing them as cluster geometry.

### Requirement 8: Command-line entrypoint

**User Story:** As a data scientist, I want to run clustering over a date range from the command line and immediately see how many clusters formed and how large they are, so that I can tune parameters quickly.

#### Acceptance Criteria

1. The system SHALL provide a command-line entrypoint that accepts a start date, an end date, an optional area of interest and optional parameter overrides.
2. The system SHALL treat the command-line date bounds as inclusive calendar dates and SHALL state the resulting half-open UTC interval in its output.
3. The system SHALL default the evaluation time to the end of the requested range, and SHALL accept an explicit evaluation time for causal replay.
4. The system SHALL print the total cluster count for the run.
5. The system SHALL print the cluster size distribution, including minimum, median, 90th percentile, 99th percentile and maximum observation counts per cluster, plus a histogram of sizes.
6. The system SHALL print the cluster extent distribution, including the maximum observed extent, alongside the configured limit.
7. The system SHALL print the effective parameter set, the input detection count, the reconciled observation count, the noise count and the open and closed cluster counts.
8. The system SHALL print the same summary without persisting anything when asked for a preview.
9. The system SHALL exit non-zero when a run fails or an invariant is violated.
10. The system SHALL emit machine-readable output in addition to the human-readable summary, so results can be compared across parameter sweeps.

### Requirement 9: Stress-test validation against identified extremes

**User Story:** As a data scientist, I want the entrypoint to validate the parameters against the two extreme regimes we identified, so that epsilon and the extent limit are proven before the model depends on them.

#### Acceptance Criteria

1. The system SHALL provide a stress-test mode that runs the named scenarios and reports a pass or fail verdict per scenario.
2. The system SHALL include a dense multi-event scenario covering the November 2025 India stubble-burning backfill range.
3. The system SHALL include a late-monsoon scenario for September 8 that exercises both sparse Indian detections and high-density savanna burning.
4. The system SHALL keep each scenario's date range, area of interest and expectation thresholds in configuration rather than in code.
5. The system SHALL assert, in every scenario, that no cluster exceeds the configured maximum diameter.
6. The system SHALL assert, in the dense scenario, that clusters remain at site scale rather than forming a small number of very large clusters spanning a burning belt.
7. The system SHALL assert, in the sparse scenario, that isolated detections do not merge into large clusters and SHALL report the noise fraction.
8. The system SHALL report, per scenario, the cluster count, size distribution, extent distribution and the observed maximum extent.
9. IF any asserted expectation fails, THEN the system SHALL exit non-zero so the failure can gate a build.
10. The system SHALL be able to run its causality, determinism and extent assertions against deterministic fixtures, without network access, credentials or a populated database.
11. WHERE a scenario's detections are not present in the store, the system SHALL report that scenario as skipped with the reason, rather than passing silently or reporting a false failure.

### Requirement 10: Performance and partitioning

**User Story:** As a pipeline operator, I want clustering to stay within predictable time and memory on a laptop-scale database, so that a twenty-day dense backfill is practical to iterate on.

#### Acceptance Criteria

1. The system SHALL use a spatial index or H3 cell partitioning to avoid comparing every detection against every other detection.
2. The system SHALL produce results independent of partitioning, so that enabling or disabling partitioning does not change cluster membership.
3. WHERE H3 partitioning is used, the system SHALL include a halo wide enough that the spatial epsilon cannot be truncated at a cell boundary.
4. The system SHALL stream input in bounded batches rather than loading an entire multi-week range into memory at once.
5. The system SHALL read and write through SQLAlchemy with connection pooling, reusing the ingestion layer's engine configuration and connection budget.
6. The system SHALL NOT hold a database connection idle while performing in-process computation on a batch.
7. The system SHALL report the wall-clock duration and the input, observation and cluster counts of every run.

### Requirement 11: Run tracking, configuration and observability

**User Story:** As a pipeline operator, I want every clustering run recorded with its parameters and outcome, so that a result can be traced to the exact inputs and settings that produced it.

#### Acceptance Criteria

1. WHEN a run starts or finishes, THEN the system SHALL record its date range, evaluation time, knowledge-time bound, area of interest, parameter set, algorithm version, status and counts.
2. The system SHALL give each run one of these statuses: running, succeeded, failed or skipped.
3. The system SHALL write logs as structured JSON lines including the run identifier, date range and evaluation time.
4. The system SHALL read non-secret settings from a configuration file and SHALL let environment variables override them, following the ingestion layer's convention.
5. The system SHALL read database credentials only from environment variables and SHALL NOT contain credentials in source, configuration or fixtures.
6. The system SHALL redact secrets from logs, error messages and stored run records.
7. WHEN the clustering stage fails, THEN the system SHALL leave previously persisted clusters intact and SHALL record the failure against the run.
8. The system SHALL NOT modify `ingestion.fire_detections` or any other ingestion-owned table.

## Decisions from review

These answers are settled and bind the design.

1. **September 8 scenario**: the year is 2026. The sparse Indian case reads the live PostGIS store. The high-density savanna case uses a deterministic fixture rather than a live African dataset, so the ingestion area of interest stays unchanged. The fixture is clearly labelled synthetic and is never presented as observed data.
2. **Minimum neighbours**: 2. This keeps persistent industrial flares while rejecting isolated single-pixel sensor noise.
3. **Noise handling**: settled noise observations are persisted in their own table, visible to the anomaly stage, and excluded from the cluster tables.
4. **Reconciliation tolerances**: 375 m and 15 minutes, so overlapping satellite passes over one fire collapse into a single observation.
5. **Causality mode**: event-time only, keyed on acquisition time. Knowledge-time bounding is deliberately not implemented. The accepted consequence is that replaying a historical range after standard-processing backfill can shift results, because the input set changed rather than because the algorithm looked forward.
6. **Extent limit**: 2000 m. An oversized group is split into several bounded clusters rather than flagged for review.
