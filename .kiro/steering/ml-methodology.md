
1. SPATIAL LEAKAGE PREVENTION:
   Never use random train/test splits. All cross-validation and final model evaluation
   must use spatial-block holdout splits (K-Means spatial clustering or rigid grid
   bounding boxes) ensuring no test site falls within the spatial influence buffer of
   training sites. Raw latitude and longitude must NEVER be input features.

2. STRICT CAUSAL TEMPORAL INTEGRITY:
   Feature engineering and clustering must operate strictly on causal, trailing-window
   data. At any evaluation timestamp T, the pipeline may only inspect data where
   acq_timestamp <= T. Future pings must never retroactively alter historic cluster
   geometries or running statistics.

3. UNBIASED GROUND TRUTH LABELS:
   Do not generate ground truth labels via naive spatial joins with OpenStreetMap
   (e.g., polygon intersection = "Industrial"). This introduces spatial circularity
   and misclassifies unmapped rural industrial facilities as agricultural burns or
   wildfires. OSM is strictly an initial heuristic seed, verified through Sentinel-2
   high-resolution optical spot-checks.

4. REALISTIC METRIC TARGETS:
   Under rigorous spatial-block cross-validation, realistic F1 scores land between
   0.75 and 0.88. Any pipeline run yielding > 0.95 F1 indicates data leakage
   (e.g., coordinate overfitting or temporal cross-talk) and must fail the build.

5. ASYNCHRONOUS TWO-TIER DESIGN:
   Tier 1 evaluation relies entirely on FIRMS, land-cover, and static infrastructure
   context. Tier 2 enrichments (TROPOMI NO2, optical structures) are missing 30% to
   60% of the time due to satellite revisit intervals and cloud masking. The ML model
   must handle nulls natively; code must never assume auxiliary data is present.

6. METEOROLOGICAL SOURCE ROUTING:
   Use NCMRWF 4-km forecasts for high-resolution analysis over India; use NOAA GFS
   0.25° globally. Never invoke HRRR (CONUS/US-only).