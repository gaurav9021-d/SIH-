# Clustering Engine

Groups FIRMS detections into persistent, site-scale clusters. One cluster is meant to be one facility or burn event, not one satellite pixel.

Reads `ingestion.fire_detections`. Writes the `clustering` schema. Never writes to an ingestion table.

## The algorithm, and why it is not stock DBSCAN

The engine implements **causal diameter-bounded spatiotemporal DBSCAN** in a single pass over observations in acquisition-time order.

Epsilon alone cannot keep clusters small. DBSCAN groups by transitive density connectivity: if A–B are within epsilon and B–C are within epsilon, all three merge even when A and C are far apart. A line of detections spaced 700 m apart therefore merges into one cluster of unbounded length, which is exactly the geometry Punjab and Haryana stubble burning produces in November.

The fixtures measure this directly:

| Fixture | Extent bound removed | Bound at 2000 m |
|---|---|---|
| 29 detections, 700 m apart along 20 km | **1 cluster, 19,600 m** | 10 clusters, max 1,400 m |
| 1,369 detections over 20 × 20 km | **1 cluster, 28,081 m** | 134 clusters, max 1,999 m |

So epsilon bounds the *neighbourhood* and `max_cluster_diameter_m` bounds the *cluster*. Three deliberate deviations follow, each recorded per cluster in the emitted `algorithm` block:

1. **Diameter-bounded growth.** An observation is refused if admitting it would push the cluster past the limit.
2. **Merging disabled.** An observation reachable from two clusters joins the nearest one. Merging would break the bound and force one of the two cluster identities to be discarded.
3. **Streaming core-point determination.** Density is evaluated as observations arrive, so cluster-or-noise status is provisional until the linkage window closes.

Results will not match a reference DBSCAN implementation on the same points. That is the point.

## Causality

At evaluation time T the engine reads only detections acquired at or before T, and a later run never rewrites what an earlier run reported.

Two timestamps per cluster carry different meanings, and conflating them is what breaks replay tests:

- `first_detected` — acquisition time of the earliest member. Can predate the moment the cluster was knowable.
- `formed_at` — acquisition time of the observation that completed `min_samples`. The moment the cluster existed to an observer.

Reconstructing "state as of T1" filters on `formed_at <= T1`, so a cluster that only became knowable later is excluded rather than retroactively inserted into T1's history. `test_properties.py` asserts this at Hypothesis-generated cutoffs.

Decisions wait in exactly two bounded places:

| Stage | Waits for | Bound |
|---|---|---|
| Reconciliation | A second satellite seeing the same fire | 15 minutes |
| Cluster or noise | A partner satisfying `min_samples` | 7 days |

Hence `settled_horizon(T) = T − linkage_gap − reconcile_tolerance`. Observations at or before the horizon have final assignments; between the horizon and T they are **provisional**, and runs report them separately instead of presenting them as settled.

**Event-time only.** There is no knowledge-time bound. Replaying a range after FIRMS standard-processing data backfills can therefore change results, because the input set changed, not because the algorithm looked forward. Each run records its evaluation time so the two causes can be told apart.

## Parameters

| Parameter | Default | Notes |
|---|---|---|
| `eps_m` | 750.0 | Hard ceiling. Above it the engine refuses to start rather than silently widening. |
| `min_samples` | 2 | Catches a repeatedly detected flare; a single isolated ping stays noise. |
| `inactivity_days` | 7.0 | Idle longer than this and the cluster closes, permanently. |
| `max_linkage_gap_days` | 7.0 | Maximum time gap for two observations to be density-connected. |
| `max_cluster_diameter_m` | 2000.0 | The extent guard. |
| `reconcile_spatial_m` | 375.0 | One VIIRS pixel. |
| `reconcile_temporal_minutes` | 15.0 | Collapses overlapping satellite passes. |
| `h3_resolution` / `h3_ring` | 6 / 1 | Neighbour index only, never work splitting. |

Distances use a local tangent-plane approximation from the WGS 84 radii of curvature, pinned against `geographiclib`'s exact geodesic to under 0.05 m (measured: 1.6e-05 m). Longitude is scaled by latitude, so epsilon means the same thing in Punjab and in a savanna fixture.

The H3 halo is **measured, not assumed**: startup computes the smallest cell inradius at the configured resolution and requires `ring × inradius >= eps_m`. Resolution 6 gives 2519 m (comfortable), resolution 7 gives 966 m (passes, marginal), resolution 8 gives 360 m and is correctly refused.

Only migrations create tables. Reconciliation applies across *different* product/satellite pairs, so VIIRS-and-MODIS views of one fire collapse, while adjacent pixels from one sensor stay separate as genuine fire extent.

## Install

```powershell
python -m pip install -e ".[dev]"
```

## Run without a database

The extent and causality proofs need no credentials and no PostGIS:

```powershell
python -m pytest clustering/tests -q
python -m clustering stress --scenario chain_runaway_guard
python -m clustering stress
```

`stress` probes the database briefly; if it is unreachable, store-backed scenarios report `skipped` with a reason and the fixture scenarios still run. A scenario never passes silently on missing data.

## Run against the store

```powershell
docker compose --env-file .env -f docker/docker-compose.yml up -d postgis
python -m ingestion migrate
python -m clustering migrate

# Dense November 2025 stubble burning, computed but not persisted
python -m clustering preview --start 2025-11-01 --end 2025-11-20 --aoi india

# Same range, persisted
python -m clustering run --start 2025-11-01 --end 2025-11-20 --aoi india

# Causal replay at an explicit cutoff
python -m clustering run --start 2025-11-01 --end 2025-11-20 `
  --eval-time 2025-11-10T00:00:00Z

# Parameter sweep, machine-readable
python -m clustering preview --start 2025-11-01 --end 2025-11-20 --eps-m 500 --json
```

`--start` and `--end` are inclusive calendar dates; the run prints the resulting half-open UTC interval, so 20 November is never dropped. `--eval-time` defaults to the end of that interval.

The summary reports the effective parameters, detections read, reconciled observations, open and closed cluster counts, noise and pending counts, the size distribution with a histogram, and the extent distribution against the configured limit with remaining headroom.

Store-backed scenarios need the November 2025 India FIRMS backfill ingested first (see `ingestion/README.md`). Without it they skip.

## Inspect results

```sql
SELECT count(*) AS clusters,
       round(max(extent_m)::numeric, 1) AS max_extent_m,
       round(avg(observation_count)::numeric, 1) AS mean_size
FROM clustering.clusters
WHERE params_fingerprint = :fingerprint;

-- State as it was knowable at an earlier moment
SELECT * FROM clustering.clusters_as_of(
  TIMESTAMPTZ '2025-11-10 00:00:00+00', :fingerprint
);
```

`provisional = true` marks clusters newer than the settled horizon. Consumers that ignore the flag may act on assignments that still change inside the linkage window.

## Known limitations

- **Boundary sensitivity.** In a continuous burning belt, where a cluster boundary falls depends on causal arrival order. Identity is stable for a fixed input set, but adding earlier-arriving data can shift boundaries. This is inherent to imposing a 2 km ceiling on a continuous phenomenon, which is why the stress scenarios assert distributions rather than exact cluster counts.
- **`min_samples = 2`** admits some coincidental pairs. Settled noise is persisted so the tradeoff can be measured rather than argued.
- **Fixture realism.** `dense_savanna_grid` proves behaviour under synthetic density, not agreement with real savanna fire geometry. It is labelled synthetic wherever it appears. Validating against real savanna data would need the ingestion AOI expanded, which is deliberately out of scope.
- **No H3 work splitting.** Sharding by cell would change memberships at boundaries, so H3 is an index only. `test_index_equivalence.py` asserts the indexed and brute-force searches agree.

## Scope

FIRMS detections only. Land cover, OSM, TROPOMI NO2, optical scenes and wind never influence cluster formation; they are later-stage enrichment. Emitted clusters populate `features.firms` and leave the other feature blocks, `classification` and `anomaly` as `null`.

FIRMS clusters are an observational reference for tuning clustering density. They are **not** verified burn-type labels. Keep supervised label construction separate, per `.kiro/steering/ml-methodology.md`.
