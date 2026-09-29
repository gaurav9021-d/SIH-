An AI-driven operational pipeline for SIH26162 that ingests global satellite
thermal telemetry, clusters detections into persistent geospatial entities, and
classifies them into 5 classes: industrial_fire, gas_flare, agricultural_burning,
mining_activity, and wildfire. A secondary parallel engine detects abnormal
thermal deviations (leaks/explosions). The system outputs an interactive GIS
dashboard presenting calibrated confidence percentages, classification tiers,
and per-sensor freshness timestamps