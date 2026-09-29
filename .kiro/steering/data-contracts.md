Every record in the spatiotemporal store carries an explicit "as_of"
freshness timestamp per field, not just a row-level updated_at. A cluster
object's JSON shape (used by both the model and the API) is:

{
  "cluster_id": "string",
  "centroid": {"lat": 21.1458, "lon": 79.0882},
  "bounding_box": {"min_lat": 21.140, "min_lon": 79.080, "max_lat": 21.150, "max_lon": 79.095},
  "first_detected": "ISO8601",
  "last_detected": "ISO8601",
  "detection_count": 14,
  "features": {
    "firms": {"persistence_days": 2.58, "frp_mean": 42.4, "frp_var": 12.1,
               "day_night_ratio": 0.42, "as_of": "ISO8601"},
    "land_cover": {"class": "string", "as_of": "ISO8601"},
    "osm": {"tag": "string|null", "distance_m": "number|null", "as_of": "ISO8601|null"},
    "no2": {"column_density": "number|null", "dispersion_verified": "boolean", "as_of": "ISO8601|null"},
    "optical": {"structure_class": "string|null", "cloud_cover_pct": "number", "as_of": "ISO8601|null"},
    "wind": {"speed_mps": "number", "direction_deg": "number", "source": "string", "as_of": "ISO8601"}
  },
  "classification": {
    "category": "industrial_fire|gas_flare|agricultural_burning|mining_activity|wildfire|needs_review",
    "confidence_pct": "number (0-100)",
    "tier": "1|2",
    "per_class_probabilities": {"industrial_fire": 0.051, "gas_flare": 0.874, "...": 0.075},
    "classified_at": "ISO8601",
    "classification_history": [
      {"tier": 1, "category": "string", "confidence_pct": "number", "timestamp": "ISO8601"}
    ]
  },
  "anomaly": {"is_anomaly": "boolean", "score": "number|null", "description": "string|null", "flagged_at": "ISO8601|null"}
}