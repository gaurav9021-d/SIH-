"""Environment-only secrets and validated non-secret YAML configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from ingestion.core.errors import ConfigurationError


class SecretSettings(BaseSettings):
    """Secrets are deliberately excluded from YAML and source code."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    firms_map_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("FIRMS_MAP_KEY", "MAP_KEY"),
    )
    database_url: SecretStr | None = Field(
        default=None,
        validation_alias="DATABASE_URL",
    )

    def require_firms_key(self) -> str:
        if self.firms_map_key is None:
            raise ConfigurationError(
                "FIRMS_MAP_KEY is required (legacy MAP_KEY is also accepted)"
            )
        return self.firms_map_key.get_secret_value()

    def require_database_url(self) -> str:
        if self.database_url is None:
            raise ConfigurationError("DATABASE_URL is required")
        url = self.database_url.get_secret_value()
        if url.startswith("postgres://"):
            url = "postgresql+psycopg://" + url.removeprefix("postgres://")
        elif url.startswith("postgresql://"):
            url = "postgresql+psycopg://" + url.removeprefix("postgresql://")
        if not url.startswith("postgresql+psycopg://"):
            raise ConfigurationError(
                "DATABASE_URL must use PostgreSQL (postgresql+psycopg://...)"
            )
        return url


@dataclass(frozen=True)
class Aoi:
    name: str
    west: float
    south: float
    east: float
    north: float

    @classmethod
    def from_list(cls, name: str, values: list[float]) -> "Aoi":
        if len(values) != 4:
            raise ConfigurationError(f"AOI {name!r} must have [west, south, east, north]")
        west, south, east, north = (float(value) for value in values)
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ConfigurationError(f"AOI {name!r} has invalid bounds")
        return cls(name=name, west=west, south=south, east=east, north=north)

    @property
    def bounds(self) -> tuple[float, float, float, float]:
        return self.west, self.south, self.east, self.north

    @property
    def api_bounds(self) -> str:
        return ",".join(f"{value:g}" for value in self.bounds)


@dataclass(frozen=True)
class HttpConfig:
    connect_timeout_s: float
    read_timeout_s: float
    write_timeout_s: float
    pool_timeout_s: float
    max_attempts: int
    max_total_wait_s: float
    backoff_initial_s: float
    backoff_max_s: float
    user_agent: str


@dataclass(frozen=True)
class DatabaseConfig:
    pool_size: int
    max_overflow: int
    pool_timeout_s: float
    pool_recycle_s: int
    batch_size: int
    operation_max_attempts: int
    connection_budget: int


@dataclass(frozen=True)
class FirmsConfig:
    base_url: str
    chunk_days: int
    product_families: dict[str, tuple[str, ...]]
    transactions_per_10_minutes: int
    map_key_limit_per_10_minutes: int
    reserve_for_live_polling: int
    max_concurrent_backfills: int
    lease_ttl_s: int
    lease_heartbeat_s: int


@dataclass(frozen=True)
class AppConfig:
    aois: dict[str, Aoi]
    default_aoi: str
    http: HttpConfig
    database: DatabaseConfig
    firms: FirmsConfig

    def get_aoi(self, name: str | None = None) -> Aoi:
        selected = name or self.default_aoi
        try:
            return self.aois[selected]
        except KeyError as exc:
            choices = ", ".join(sorted(self.aois))
            raise ConfigurationError(
                f"Unknown AOI {selected!r}; configured AOIs: {choices}"
            ) from exc


def load_secrets() -> SecretSettings:
    return SecretSettings()


def load_config(path: str | Path | None = None) -> AppConfig:
    config_path = Path(path or os.getenv("INGEST_CONFIG", "ingestion/config/ingestion.yaml"))
    if not config_path.is_file():
        raise ConfigurationError(f"Configuration file not found: {config_path}")

    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ConfigurationError("The ingestion configuration root must be a mapping")
    apply_environment_overrides(data)

    try:
        aois = {
            name: Aoi.from_list(name, list(bounds))
            for name, bounds in data["aois"].items()
        }
        default_aoi = str(data["default_aoi"])
        http_data = data["http"]
        db_data = data["database"]
        firms_data = data["firms"]
        backfill_data = firms_data["backfill"]

        http = HttpConfig(
            connect_timeout_s=float(http_data["connect_timeout_s"]),
            read_timeout_s=float(http_data["read_timeout_s"]),
            write_timeout_s=float(http_data["write_timeout_s"]),
            pool_timeout_s=float(http_data["pool_timeout_s"]),
            max_attempts=int(http_data["max_attempts"]),
            max_total_wait_s=float(http_data["max_total_wait_s"]),
            backoff_initial_s=float(http_data["backoff_initial_s"]),
            backoff_max_s=float(http_data["backoff_max_s"]),
            user_agent=str(http_data["user_agent"]),
        )
        database = DatabaseConfig(
            pool_size=int(db_data["pool_size"]),
            max_overflow=int(db_data["max_overflow"]),
            pool_timeout_s=float(db_data["pool_timeout_s"]),
            pool_recycle_s=int(db_data["pool_recycle_s"]),
            batch_size=int(db_data["batch_size"]),
            operation_max_attempts=int(db_data["operation_max_attempts"]),
            connection_budget=int(db_data["connection_budget"]),
        )
        families = {
            str(name): tuple(str(product) for product in products)
            for name, products in firms_data["product_families"].items()
        }
        firms = FirmsConfig(
            base_url=str(firms_data["base_url"]).rstrip("/"),
            chunk_days=int(firms_data["chunk_days"]),
            product_families=families,
            transactions_per_10_minutes=int(
                backfill_data["transactions_per_10_minutes"]
            ),
            map_key_limit_per_10_minutes=int(
                backfill_data["map_key_limit_per_10_minutes"]
            ),
            reserve_for_live_polling=int(backfill_data["reserve_for_live_polling"]),
            max_concurrent_backfills=int(backfill_data["max_concurrent"]),
            lease_ttl_s=int(backfill_data["lease_ttl_s"]),
            lease_heartbeat_s=int(backfill_data["lease_heartbeat_s"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigurationError(f"Invalid ingestion configuration: {exc}") from exc

    config = AppConfig(
        aois=aois,
        default_aoi=default_aoi,
        http=http,
        database=database,
        firms=firms,
    )
    _validate_config(config)
    return config


def apply_environment_overrides(
    data: dict[str, Any], prefix: str = "INGEST__"
) -> None:
    """Apply PREFIX_SECTION__KEY overrides, parsing values as YAML scalars.

    Shared with the clustering stage, which uses its own prefix.
    """

    for env_name, raw_value in os.environ.items():
        if not env_name.upper().startswith(prefix):
            continue
        path = [part.lower() for part in env_name[len(prefix) :].split("__") if part]
        if not path:
            continue
        node: dict[str, Any] = data
        for part in path[:-1]:
            child = node.setdefault(part, {})
            if not isinstance(child, dict):
                raise ConfigurationError(
                    f"Environment override {env_name} traverses non-mapping key {part}"
                )
            node = child
        node[path[-1]] = yaml.safe_load(raw_value)


def _validate_config(config: AppConfig) -> None:
    if config.default_aoi not in config.aois:
        raise ConfigurationError("default_aoi must name a configured AOI")
    if not 1 <= config.firms.chunk_days <= 5:
        raise ConfigurationError("FIRMS chunk_days must be between 1 and 5")
    if not config.firms.product_families:
        raise ConfigurationError("At least one FIRMS product family is required")
    if any(not products for products in config.firms.product_families.values()):
        raise ConfigurationError("Every FIRMS product family needs a candidate product")
    if config.http.max_attempts < 1 or config.http.max_total_wait_s <= 0:
        raise ConfigurationError("HTTP retry limits must be positive")
    if config.database.pool_size < 1 or config.database.max_overflow < 0:
        raise ConfigurationError("Database pool values are invalid")
    if config.database.batch_size < 1 or config.database.connection_budget < 1:
        raise ConfigurationError("Database batch_size and connection_budget must be positive")
    firms = config.firms
    if firms.transactions_per_10_minutes + firms.reserve_for_live_polling > firms.map_key_limit_per_10_minutes:
        raise ConfigurationError(
            "FIRMS backfill quota plus live-polling reserve exceeds the MAP_KEY limit"
        )
    if firms.max_concurrent_backfills < 1:
        raise ConfigurationError("FIRMS max_concurrent backfills must be positive")
    if firms.lease_heartbeat_s < 1 or firms.lease_ttl_s <= 2 * firms.lease_heartbeat_s:
        raise ConfigurationError(
            "FIRMS lease_ttl_s must be more than twice lease_heartbeat_s"
        )
