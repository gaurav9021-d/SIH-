"""NASA FIRMS Area API client."""

from __future__ import annotations

from typing import Protocol
from urllib.parse import quote

from ingestion.core.http import ResilientHttpClient
from ingestion.core.settings import Aoi, FirmsConfig
from ingestion.firms.models import HttpTextResult, ProductAvailability
from ingestion.firms.parser import parse_availability_csv


class Quota(Protocol):
    def reserve(self, cost: int) -> None: ...


class FirmsClient:
    def __init__(
        self,
        *,
        http: ResilientHttpClient,
        config: FirmsConfig,
        map_key: str,
        quota: Quota | None = None,
    ) -> None:
        self.http = http
        self.config = config
        self.map_key = map_key
        self.quota = quota

    def availability(self) -> tuple[ProductAvailability, ...]:
        key = quote(self.map_key, safe="")
        url = f"{self.config.base_url}/data_availability/csv/{key}/ALL"
        response = self.http.get_text(
            url,
            source="firms",
            product="availability",
            before_attempt=(lambda: self.quota.reserve(1)) if self.quota else None,
        )
        return parse_availability_csv(response.text)

    def area(
        self,
        *,
        product: str,
        aoi: Aoi,
        start_date: str,
        day_count: int,
    ) -> HttpTextResult:
        if not 1 <= day_count <= 5:
            raise ValueError("FIRMS Area API day_count must be between 1 and 5")
        key = quote(self.map_key, safe="")
        url = (
            f"{self.config.base_url}/area/csv/{key}/{quote(product, safe='')}"
            f"/{aoi.api_bounds}/{day_count}/{start_date}"
        )
        response = self.http.get_text(
            url,
            source="firms",
            product=product,
            before_attempt=(lambda: self.quota.reserve(day_count)) if self.quota else None,
        )
        return HttpTextResult(
            text=response.text,
            retries=response.retries,
            retrieved_at=response.retrieved_at,
        )
