# Implementation Plan: Clustering Engine

This plan implements the approved requirements and design under `/clustering/`. Complete tasks in order: the pure step function and its causality proofs come before any database wiring, so the algorithm is proven correct before persistence can obscure it.

Automated tests **are** in scope here, unlike the data-ingestion plan. The requirements themselves demand them: Requirement 3.6 asks for the incremental step to be unit-testable without a database, and Requirement 9.10 asks for the causality, determinism and extent assertions to run without network access, credentials or a populated database. The anti-runaway guard is only meaningful if a test proves it.

Prerequisites: the ingestion package supplies `ingestion.core.settings`, `ingestion.core.logging` and `ingestion.core.db`. Store-backed work additionally needs the ingestion migration applied and the November 2025 India backfill ingested. Every fixture-backed task runs without a database or credentials.

- [x] 1. Bootstrap the clustering package and shared infrastructure reuse
  - [x] 1.1 Create the package layout from `design.md`
    - Add `clustering/__init__.py`, `__main__.py`, `core/`, `fixtures/`, `migrations/` and `tests/`.
    - Add placeholder CLI subcommands `run`, `preview`, `stress` and `migrate`, and confirm `python -m clustering --help` works.
    - _Requirements: 8.1, 11.3_
  - [x] 1.2 Add dependencies with exact pins
    - Add `h3` and `numpy` as runtime dependencies, and `geographiclib`, `pytest` and `hypothesis` as development dependencies.
    - Pin the H3 major version explicitly and note in the README which API generation is targeted, because the v3 and v4 function names differ.
    - _Requirements: 10.1, 10.3_
  - [x] 1.3 Add configuration and reuse the ingestion settings loader
    - Add `clustering/config/clustering.yaml` with the AOI list, the parameter block and the four stress scenarios from the design.
    - Load it through `ingestion.core.settings` so the same nested environment-override convention applies.
    - Read `DATABASE_URL` only from the environment, and register it with the existing redaction filter.
    - _Requirements: 11.4, 11.5, 11.6_
  - [ ] 1.4 Wire structured logging
    - Emit JSON lines through `ingestion.core.logging`, with run identifier, date range and evaluation time in the log context.
    - _Requirements: 11.3, 11.6_
  - [ ] 1.5 Validate the foundation
    - Confirm `python -m clustering --help` runs with no import errors and no database connection.
    - Confirm a deliberately planted canary secret never appears in log output.
    - _Requirements: 11.3, 11.5, 11.6_

- [x] 2. Implement validated parameters
  - [x] 2.1 Define the parameter model
    - Implement `ClusterParams` with `eps_m`, `min_samples`, `inactivity_days`, `max_linkage_gap_days`, `max_cluster_diameter_m`, the two reconciliation tolerances, `reconcile_enabled`, the H3 resolution and ring, and `algorithm_version`.
    - Default `max_linkage_gap_days` to `inactivity_days` when it is not set explicitly.
    - _Requirements: 2.1, 2.5, 2.6, 2.7_
  - [x] 2.2 Enforce the epsilon ceiling and the other bounds
    - Reject `eps_m` above 750.0 and `eps_m` at or below zero, refusing to start rather than widening the neighbourhood.
    - Reject `min_samples` below 1, non-positive time bounds, `max_cluster_diameter_m` below `eps_m`, and `reconcile_spatial_m` above `eps_m`.
    - _Requirements: 2.2, 2.3, 4.1_
  - [x] 2.3 Implement the computed H3 halo check
    - Derive the hexagon inradius for the configured resolution from the smallest edge length the H3 library reports, and require `h3_ring * inradius >= eps_m`.
    - Treat a failing check as a configuration error rather than a warning, because a truncated neighbourhood silently fragments clusters.
    - _Requirements: 10.3_
  - [x] 2.4 Implement the parameter fingerprint
    - Hash every field into a short stable fingerprint, and use it to scope results so runs under different parameters never mix.
    - _Requirements: 2.8, 2.9_
  - [x] 2.5 Test parameter validation
    - Cover the epsilon ceiling, each rejected bound, the halo check at a safe and a marginal resolution, and fingerprint stability and sensitivity.
    - _Requirements: 2.2, 2.3, 2.8, 2.9, 10.3_

- [x] 3. Implement geometry primitives
  - [x] 3.1 Implement fast metric distance
    - Implement `distance_m` using a local tangent-plane approximation with WGS 84 meridional and parallel radii of curvature at the reference latitude, so longitude is scaled by latitude and degrees are never treated as metres.
    - Keep it vectorisable, because it runs on the hot path for every candidate comparison.
    - _Requirements: 2.4_
  - [x] 3.2 Prove the distance approximation against an exact geodesic
    - Compare `distance_m` with `geographiclib` across latitudes from the equator to high latitude and separations up to the maximum cluster diameter.
    - Assert a documented error bound well below the tolerance that epsilon and the extent limit require, and record that bound in the module docstring.
    - _Requirements: 2.4, 4.2_
  - [x] 3.3 Implement the incremental convex hull and exact extent
    - Implement a hull that accepts points one at a time and recomputes by monotone chain over `hull ∪ {p}`.
    - Implement `extent_m` as the maximum distance between hull vertices, which equals the point set's diameter.
    - Implement `extent_after_adding` so admission can be tested without mutating the cluster.
    - _Requirements: 4.2, 4.3, 10.4_
  - [x] 3.4 Implement centroid and bounding box
    - Compute both from member coordinates and keep the bounding box consistent with the persisted extent.
    - _Requirements: 7.8_
  - [x] 3.5 Test geometry
    - Verify hull extent against brute-force maximum pairwise distance on random point sets.
    - Verify that hull memory stays small while the member count grows, confirming the bounded-memory claim.
    - _Requirements: 4.2, 10.4_

- [x] 4. Implement cross-satellite reconciliation
  - [x] 4.1 Implement the causal reconciler
    - Consume detections in acquisition-time order and let each open observation absorb later detections within `reconcile_spatial_m` and `reconcile_temporal_minutes` of its anchor.
    - Emit an observation only once its temporal tolerance has elapsed, so it is final before it reaches the clustering step.
    - _Requirements: 5.1, 5.2, 5.6_
  - [x] 4.2 Define observation identity and fields
    - Use the earliest detection as the anchor, with the detection identifier as the tiebreak, and take the observation's position from the anchor rather than a moving centroid.
    - Record mean FRP over absorbed detections, the anchor's day or night flag, the absorbed detection identifiers, and the contributing products and satellites.
    - Derive `observation_key` as a deterministic hash of the anchor's natural key.
    - _Requirements: 5.3, 5.5, 3.9_
  - [x] 4.3 Support disabling reconciliation
    - When disabled, map each detection to one observation and record on the run that reconciliation was off.
    - _Requirements: 5.7_
  - [x] 4.4 Test reconciliation
    - Verify that VIIRS and MODIS views of one fire inside both tolerances collapse to a single observation whose detection count equals the number of detections.
    - Verify that detections outside either tolerance stay separate, that ordering is respected, and that results are deterministic across repeated runs.
    - _Requirements: 5.1, 5.4, 5.5, 5.6_

- [x] 5. Implement the neighbour index
  - [x] 5.1 Build the H3-keyed index
    - Map H3 cell to the observations currently relevant for linkage, each tagged with its owner cluster or marked pending.
    - Look up a candidate across its own cell plus the configured ring.
    - _Requirements: 10.1, 10.3_
  - [x] 5.2 Implement eviction
    - Remove a cluster's members when it closes and a pending observation when it settles as noise, so the index tracks the active set rather than all history.
    - _Requirements: 10.4_
  - [x] 5.3 Provide a brute-force reference implementation
    - Add an exhaustive neighbour search behind the same interface, for equivalence testing only.
    - _Requirements: 10.2_
  - [x] 5.4 Test partitioning neutrality and halo sufficiency
    - Assert identical cluster memberships from the indexed and brute-force searches over the same input.
    - Assert that no observation within epsilon of a candidate falls outside the searched cells, including for candidates placed deliberately near cell boundaries.
    - _Requirements: 10.2, 10.3_

- [x] 6. Implement the pure step function
  - [x] 6.1 Define the state and effect types
    - Implement `ClusterState` holding open clusters, pending observations, the neighbour index and the last processed timestamp.
    - Implement `StepEffect` reporting clusters opened, observations admitted, clusters closed and observations settled as noise.
    - Keep `step` free of all I/O so causality can be tested without a database.
    - _Requirements: 3.6_
  - [x] 6.2 Enforce causal ordering
    - Require non-decreasing acquisition time, with a deterministic tiebreak for identical timestamps, and abort on out-of-order input rather than silently accepting it.
    - _Requirements: 3.1, 3.2_
  - [x] 6.3 Implement closure before candidate selection
    - Close every cluster whose inactivity exceeds the cutoff before evaluating the incoming observation, so a closed cluster can never be a candidate.
    - Make closure immutable: no later observation may alter a closed cluster's identity, geometry, membership, timing or features.
    - _Requirements: 3.3, 6.4, 6.6, 6.7_
  - [x] 6.4 Implement pending settlement
    - Settle a pending observation as noise once its linkage window has closed, and record the settlement time on the acquisition clock.
    - _Requirements: 6.11_
  - [x] 6.5 Implement candidate selection with the diameter guard
    - Measure reachability to cluster members rather than to the centroid, and require both epsilon and the linkage gap.
    - Reject any candidate cluster whose extent after admission would exceed `max_cluster_diameter_m`.
    - _Requirements: 4.3, 2.7_
  - [x] 6.6 Implement nearest-cluster admission with merging disabled
    - Admit to the single nearest eligible cluster, breaking ties on cluster identifier, and never merge two clusters.
    - Advance the cluster's last-detected time and update its incremental aggregates and hull.
    - _Requirements: 4.4, 4.8, 6.5_
  - [x] 6.7 Implement cluster formation from pending neighbours
    - Group the incoming observation with pending neighbours inside epsilon and the linkage gap, adding them in ascending distance and key order while the extent stays inside the bound.
    - Open a cluster only when the group reaches `min_samples`, and set `formed_at` to the acquisition time of the observation that completed the threshold.
    - _Requirements: 4.3, 4.4, 6.3_
  - [x] 6.8 Implement deterministic cluster identity
    - Derive `cluster_id` from the algorithm version, the parameter fingerprint and the seed observation key, so a replay reproduces the same identifiers.
    - _Requirements: 3.9, 6.3, 2.9_
  - [x] 6.9 Maintain incremental aggregates
    - Track count, FRP sum and sum of squares, day and night counts, first and last detected, and the hull, all in constant time per admission.
    - Keep only members within the linkage gap available for reachability, so a long-lived cluster does not retain an unbounded member list.
    - _Requirements: 10.4_
  - [x] 6.10 Implement the settled horizon and the as-of view
    - Compute `settled_horizon(T)` from the linkage gap and the reconciliation tolerance.
    - Implement an in-memory as-of view returning clusters with `formed_at` at or before T with members restricted to T, settled noise at or before the horizon, and provisional pending observations between the horizon and T.
    - _Requirements: 3.5_

- [x] 7. Prove causality and determinism
  - [x] 7.1 Test run determinism
    - Assert that repeated runs over the same input set, evaluation time and parameters produce identical identifiers, memberships and aggregates.
    - _Requirements: 3.4_
  - [x] 7.2 Test the replay prefix property
    - Replay one detection stream to several evaluation times and assert that the later run's view as of each earlier time equals the earlier run's output, with noise restricted to the settled horizon.
    - _Requirements: 3.5_
  - [x] 7.3 Test the absence of forward influence
    - Assert that truncating every observation after T leaves the state at T unchanged.
    - _Requirements: 3.1, 3.3_
  - [x] 7.4 Test closure finality
    - Assert that after a gap exceeding the inactivity cutoff a later nearby observation opens a new cluster, and that the closed cluster is untouched.
    - _Requirements: 6.6, 6.7_
  - [x] 7.5 Add property-based coverage
    - Generate observation streams with Hypothesis and assert the ordering, determinism, closure and extent invariants hold across them.
    - _Requirements: 3.2, 3.4, 4.2_

- [x] 8. Prove the anti-runaway guard
  - [x] 8.1 Build the linear chain fixture
    - Generate detections every 700 metres along a 20 kilometre line inside one linkage window, deterministically.
    - _Requirements: 9.4, 9.10_
  - [x] 8.2 Assert chain resistance
    - Assert that the engine returns more than one cluster and that every cluster respects the 2000 metre bound, documenting in the test that textbook DBSCAN returns a single cluster roughly 20 kilometres across for the same input.
    - _Requirements: 4.2, 4.3, 9.6_
  - [x] 8.3 Build the dense savanna fixture
    - Generate a seeded pseudo-random grid at roughly 400 to 700 metre spacing across about 30 by 30 kilometres, reproducible across runs.
    - Label the fixture synthetic in its output and in the run record so it is never mistaken for observed savanna data.
    - _Requirements: 9.3, 9.4, 9.10_
  - [x] 8.4 Assert dense-regime tiling
    - Assert that the dense block yields many bounded clusters rather than one large one, and that no cluster exceeds the configured maximum diameter.
    - _Requirements: 4.2, 4.8, 9.5_
  - [x] 8.5 Add the persist-time extent verification
    - Verify the extent invariant for every cluster before it is written, and fail the run rather than persist a violating cluster.
    - _Requirements: 4.5_

- [x] 9. Create the clustering schema
  - [x] 9.1 Initialise Alembic for the clustering schema
    - Keep the version table inside the `clustering` schema so ingestion and clustering migrate independently.
    - Create the schema if absent and make the migration safe to re-run.
    - _Requirements: 6.1, 6.10, 11.8_
  - [x] 9.2 Create the tables in dependency order
    - Create `runs` first, then `clusters`, `observations`, `cluster_members`, `observation_detections` and `noise_observations`, exactly as specified in the design.
    - Reference detections by the logical `(detection_id, as_of)` pair with no foreign key, so ingestion partition maintenance stays independent.
    - Add the GiST geometry indexes, the fingerprint and status indexes, and the `formed_at` index.
    - _Requirements: 6.1, 6.2, 6.8, 6.11, 11.8_
  - [x] 9.3 Add the as-of replay function
    - Add `clustering.clusters_as_of` returning cluster state at a timestamp for a parameter fingerprint, filtering on `formed_at` and member acquisition time.
    - _Requirements: 3.5_
  - [ ] 9.4 Validate the migration
    - Apply it to a clean database, apply it again, and confirm no duplicate objects or errors.
    - Confirm every geometry uses SRID 4326 with a usable spatial index, and that no ingestion-owned object was modified.
    - _Requirements: 6.1, 6.10, 11.8_

- [x] 10. Implement the detection reader
  - [x] 10.1 Stream detections in causal order
    - Read from `ingestion.fire_detections` in `(as_of, detection_id)` keyset order, filtered by AOI bounding box, date range and `as_of <= T`.
    - Use keyset pagination rather than offsets, for a stable order across a partitioned table.
    - _Requirements: 1.1, 1.5, 3.1, 3.2_
  - [x] 10.2 Enforce the FIRMS-only boundary
    - Read no table other than the detection table, and support restricting a run to an explicit product list while recording the product set used.
    - _Requirements: 1.2, 1.3, 1.4_
  - [x] 10.3 Keep reads bounded and pooled
    - Fetch configurable batches through the pooled engine, holding a connection only while fetching, never during computation.
    - Declare the clustering process's pool share so it counts against the shared connection budget.
    - _Requirements: 10.4, 10.5, 10.6_
  - [x] 10.4 Handle the empty range
    - Complete successfully with zero clusters and persist nothing when no detections match.
    - _Requirements: 1.6_

- [x] 11. Implement persistence and contract emission
  - [x] 11.1 Implement idempotent writes
    - Upsert clusters on `cluster_id` with recomputed aggregates, insert members and observation-detection links without duplicating, and upsert noise observations on `observation_key`.
    - _Requirements: 6.9_
  - [x] 11.2 Flush closed clusters during the pass
    - Write and evict each cluster as it closes, and write open clusters and pending observations at the end marked provisional, so memory does not grow with range length.
    - _Requirements: 6.4, 10.4_
  - [x] 11.3 Implement the feature calculations
    - Compute `persistence_days` as fractional days between first and last detection, zero for a single observation.
    - Compute `frp_mean` and `frp_var` over reconciled observations, using population variance and zero variance for a single observation.
    - Compute `day_night_ratio` as the day fraction of observations, and set `features.firms.as_of` to the latest member acquisition time.
    - _Requirements: 7.2, 7.3, 7.4, 7.5, 7.6_
  - [x] 11.4 Emit the contract shape
    - Emit `cluster_id`, `centroid`, `bounding_box`, `first_detected`, `last_detected`, `detection_count` and the populated `features.firms` block.
    - Emit the raw detection count as `detection_count` and the reconciled count as `observation_count`.
    - Emit the other feature blocks, `classification` and `anomaly` as null, and add the `algorithm` block recording the diameter-bounded and merging-disabled deviations plus the parameter fingerprint and extent.
    - Keep raw coordinates as cluster geometry only, never as model features.
    - _Requirements: 4.7, 5.5, 7.1, 7.7, 7.9_
  - [x] 11.5 Test features and contract output
    - Verify each formula including its single-observation case, and verify that every emitted cluster carries the required fields with the correct blocks left null.
    - _Requirements: 7.1 to 7.7_

- [x] 12. Implement the engine driver and run tracking
  - [x] 12.1 Wire the pipeline
    - Compose reader, reconciler, step function and writer into one causal pass, defaulting the evaluation time to the end of the requested interval.
    - _Requirements: 3.1, 8.3_
  - [x] 12.2 Record runs
    - Record the date range, evaluation time, settled horizon, AOI, parameters, fingerprint, algorithm version, product set, status and counts at start and finish.
    - Use the statuses running, succeeded, failed and skipped, and record duration and maximum observed extent.
    - _Requirements: 11.1, 11.2, 10.7_
  - [x] 12.3 Document event-time replay drift
    - Record the evaluation time on every run and note in the README that replaying a range after standard-processing backfill can change results because the input changed, not because the algorithm looked forward.
    - _Requirements: 3.7, 3.8_
  - [x] 12.4 Handle failures without damaging prior state
    - Leave previously persisted clusters intact on failure, record the failure against the run, and pass every error summary through redaction.
    - Retry transient database errors with bounded backoff through the ingestion helper.
    - _Requirements: 11.6, 11.7_
  - [x] 12.5 Compute distributions
    - Compute cluster count, the size distribution with minimum, median, p90, p99, maximum and a histogram, the extent distribution, and the noise, pending, open and closed counts.
    - _Requirements: 8.5, 8.6, 8.7, 4.6_

- [x] 13. Implement the command-line entrypoint
  - [x] 13.1 Implement `run` and `preview`
    - Accept inclusive start and end dates, an optional AOI, an optional evaluation time, an optional product list and per-parameter overrides.
    - Print the resulting half-open UTC interval, and make `preview` compute and report without persisting.
    - _Requirements: 8.1, 8.2, 8.3, 8.8_
  - [x] 13.2 Print the run summary
    - Print the effective parameters, input detection count, reconciled observation count, open and closed cluster counts, noise and pending counts, the size distribution and the extent distribution against the configured limit.
    - _Requirements: 8.4, 8.5, 8.6, 8.7_
  - [x] 13.3 Add machine-readable output
    - Emit the same content as JSON so parameter sweeps can be diffed.
    - _Requirements: 8.10_
  - [x] 13.4 Implement exit codes
    - Return non-zero on run failure or invariant violation, and use a distinct code for configuration and parameter errors.
    - _Requirements: 8.9, 2.2_
  - [x] 13.5 Wire `migrate`
    - Apply the clustering migrations and report the resulting revision.
    - _Requirements: 6.10_

- [x] 14. Implement the stress harness
  - [x] 14.1 Load scenarios from configuration
    - Read each scenario's name, description, source, AOI or fixture, date range and expectation thresholds from configuration rather than code.
    - _Requirements: 9.4_
  - [x] 14.2 Implement the scenario runner
    - Run each scenario, evaluate its expectations and print a per-scenario verdict alongside the cluster count, size distribution, extent distribution and observed maximum extent.
    - _Requirements: 9.1, 9.8_
  - [x] 14.3 Add the dense November 2025 India scenario
    - Cover 2025-11-01 through 2025-11-20 over the India AOI from the store.
    - Assert no cluster exceeds the maximum diameter, and assert clusters stay at site scale through the minimum cluster count and maximum mean cluster size thresholds, so a runaway collapse into few large clusters fails.
    - _Requirements: 9.2, 9.5, 9.6_
  - [x] 14.4 Add the sparse September 8 2026 India scenario
    - Cover the single date over the India AOI from the store, assert isolated detections do not merge into large clusters, and report the noise fraction.
    - _Requirements: 9.3, 9.5, 9.7_
  - [x] 14.5 Add the synthetic savanna and chain scenarios
    - Run both fixtures through the same harness with no database or credentials, and label their results synthetic.
    - _Requirements: 9.3, 9.10_
  - [x] 14.6 Implement skip and failure semantics
    - Report a store-backed scenario with no detections in range as skipped with the reason, rather than passing silently or failing spuriously.
    - Exit non-zero when any expectation fails, so a build can gate on the verdict.
    - _Requirements: 9.9, 9.11_

- [ ] 15. Add integration tests against PostGIS
  - [ ] 15.1 Set up the database test harness
    - Apply migrations once per session against a test database from the environment, and skip the suite cleanly when no database is reachable.
    - _Requirements: 6.10_
  - [ ] 15.2 Test idempotent persistence
    - Run the same range twice and confirm one row per cluster, member and noise observation, with identical aggregates.
    - _Requirements: 6.9_
  - [ ] 15.3 Test identity stability across runs
    - Extend a cluster with later detections in a second run and confirm the identifier is unchanged and the aggregates advanced.
    - _Requirements: 6.3, 6.5_
  - [ ] 15.4 Test the as-of replay view
    - Compare `clustering.clusters_as_of` at an earlier timestamp against a fresh run evaluated at that timestamp.
    - _Requirements: 3.5_
  - [ ] 15.5 Test isolation from ingestion
    - Confirm a clustering run leaves every ingestion-owned table byte-identical.
    - _Requirements: 11.8_

- [x] 16. Final acceptance verification and documentation
  - [x] 16.1 Verify the epsilon and extent acceptance criteria end to end
    - Confirm a configuration above 750 metres is refused before any detection is read.
    - Confirm that across every fixture and available store-backed scenario no persisted cluster exceeds 2000 metres, and report the observed maximum.
    - _Requirements: 2.2, 4.2, 4.5, 9.5_
  - [x] 16.2 Verify the causality acceptance criteria end to end
    - Run the same detection stream to several cutoffs through the CLI and confirm no earlier result changed retroactively.
    - _Requirements: 3.1, 3.3, 3.5_
  - [x] 16.3 Verify the contract mapping
    - Confirm each emitted cluster satisfies `data-contracts.md`, that `features.firms` is fully populated with UTC timestamps, and that downstream blocks are null and awaiting later stages.
    - _Requirements: 7.1, 7.7_
  - [ ] 16.4 Measure performance on the dense range
    - Record wall-clock duration, peak memory, detection and observation counts for the twenty-day dense range, and confirm memory tracks the active set rather than the range length.
    - _Requirements: 10.4, 10.7_
  - [x] 16.5 Write the clustering README
    - Document the parameters and their bounds, the three DBSCAN deviations and why they exist, the settled horizon, the difference between `formed_at` and `first_detected`, and the event-time replay caveat.
    - Document how to run, preview and stress-test, including which scenarios need ingested data and which run on fixtures alone, with copy-paste PowerShell commands and no real secrets.
    - State plainly that FIRMS clusters are an observational reference for tuning, not verified burn-type labels, per the labelling constraint in the steering methodology.
    - _Requirements: 4.7, 8.1, 9.10, 11.4_

## Requirement Traceability

| Requirement | Implemented by tasks |
|---|---|
| 1. FIRMS-only clustering input | 10.1, 10.2, 10.4 |
| 2. DBSCAN parameters | 2, 3.1, 13.4, 16.1 |
| 3. Causality and replay determinism | 6.1, 6.2, 6.3, 6.10, 7, 9.3, 10.1, 12.1, 12.3, 15.4, 16.2 |
| 4. Bounded cluster extent | 3.3, 6.5, 6.6, 6.7, 8, 11.4, 12.5, 16.1 |
| 5. Cross-satellite reconciliation | 4, 11.4 |
| 6. Identity, lifecycle and persistence | 6.3, 6.4, 6.8, 9, 11.1, 11.2, 15.2, 15.3 |
| 7. Contract output | 3.4, 11.3, 11.4, 11.5, 16.3 |
| 8. Command-line entrypoint | 12.5, 13 |
| 9. Stress-test validation | 8, 14, 16.1 |
| 10. Performance and partitioning | 3.3, 5, 6.9, 10.3, 11.2, 12.2, 16.4 |
| 11. Run tracking and observability | 1.3, 1.4, 9.1, 9.2, 12.2, 12.4, 15.5, 16.5 |
