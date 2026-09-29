"""Clustering configuration: AOIs, parameters, runtime and stress scenarios.

Reuses the ingestion layer's loader conventions with a `CLUSTER__` override
prefix. Credentials still come only from the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from ingestion.core.errors import ConfigurationError
from ingestion.core.settings import (
    Aoi,
    DatabaseConfig,
    SecretSettings,
    apply_environment_overrides,
)

from clustering.core.params import ClusterParams, from_mapping

DEFAULT_CONFIG_PATH = "clustering/config/clustering.yaml"
OVERRIDE_PREFIX = "CLUSTER__"


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    batch_size: int = 20_000
    flush_every: int = 5_000


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    description: str
    source: str  # store | fixture
    expect: dict[str, Any]
    aoi: str | None = None
    fixture: str | None = None
    start: date | None = None
    end: date | None = None


@dataclass(frozen=True, slots=True)
class ClusteringConfig:
    aois: dict[str, Aoi]
    default_aoi: str
    params: ClusterParams
    runtime: RuntimeConfig
    database: DatabaseConfig
    scenarios: tuple[Scenario, ...]

    def get_aoi(self, name: str | None = None) -> Aoi:
        selected = name or self.default_aoi
        try:
            return self.aois[selected]
        except KeyError as exc:
            choices = ", ".join(sorted(self.aois))
            raise ConfigurationError(
                f"Unknown AOI {selected!r}; configured AOIs: {choices}"
            ) from exc

    def scenario(self, name: str) -> Scenario:
        for candidate in self.scenarios:
            if candidate.name == name:
                return candidate
        choices = ", ".join(s.name for s in self.scenarios)
        raise ConfigurationError(
            f"Unknown scenario {name!r}; configured scenarios: {choices}"
        )


def load_config(path: str | Path | None = None) -> ClusteringConfig:
    config_path = Path(
        path or os.getenv("CLUSTER_CONFIG", DEFAULT_CONFIG_PATH)
    )
    if not config_path.is_file():
        raise ConfigurationError(f"Configuration file not found: {config_path}")

    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ConfigurationError("The clustering configuration root must be a mapping")
    apply_environment_overrides(data, OVERRIDE_PREFIX)

    try:
        aois = {
            name: Aoi.from_list(name, list(bounds))
            for name, bounds in data["aois"].items()
        }
        default_aoi = str(data["default_aoi"])
        runtime_data = data.get("runtime") or {}
        database_data = data["database"]
        scenarios = tuple(
            _scenario(entry) for entry in (data.get("stress") or {}).get("scenarios", [])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigurationError(f"Invalid clustering configuration: {exc}") from exc

    if default_aoi not in aois:
        raise ConfigurationError("default_aoi must name a configured AOI")

    runtime = RuntimeConfig(
        batch_size=int(runtime_data.get("batch_size", 20_000)),
        flush_every=int(runtime_data.get("flush_every", 5_000)),
    )
    if runtime.batch_size < 1 or runtime.flush_every < 1:
        raise ConfigurationError("runtime batch_size and flush_every must be positive")

    database = DatabaseConfig(
        pool_size=int(database_data["pool_size"]),
        max_overflow=int(database_data["max_overflow"]),
        pool_timeout_s=float(database_data["pool_timeout_s"]),
        pool_recycle_s=int(database_data["pool_recycle_s"]),
        batch_size=int(database_data["batch_size"]),
        operation_max_attempts=int(database_data["operation_max_attempts"]),
        connection_budget=int(database_data["connection_budget"]),
    )

    return ClusteringConfig(
        aois=aois,
        default_aoi=default_aoi,
        params=from_mapping(data.get("params")),
        runtime=runtime,
        database=database,
        scenarios=scenarios,
    )


def _scenario(entry: dict[str, Any]) -> Scenario:
    source = str(entry["source"])
    if source not in {"store", "fixture"}:
        raise ConfigurationError(
            f"Scenario {entry.get('name')!r} has unknown source {source!r}"
        )
    if source == "fixture" and not entry.get("fixture"):
        raise ConfigurationError(
            f"Scenario {entry.get('name')!r} is fixture-backed but names no fixture"
        )
    if source == "store" and not entry.get("aoi"):
        raise ConfigurationError(
            f"Scenario {entry.get('name')!r} is store-backed but names no AOI"
        )
    return Scenario(
        name=str(entry["name"]),
        description=str(entry.get("description", "")).strip(),
        source=source,
        expect=dict(entry.get("expect") or {}),
        aoi=entry.get("aoi"),
        fixture=entry.get("fixture"),
        start=entry.get("start"),
        end=entry.get("end"),
    )


def load_secrets() -> SecretSettings:
    return SecretSettings()
