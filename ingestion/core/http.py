"""One retry-aware HTTP boundary shared by ingestion adapters."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    stop_before_delay,
    wait_exponential_jitter,
)

from ingestion.core.errors import (
    NonRetryableHTTPError,
    RetryableHTTPError,
    RetryExhaustedError,
)
from ingestion.core.logging import get_logger, log_event, redact
from ingestion.core.settings import HttpConfig

LOGGER = get_logger("http")


@dataclass(frozen=True)
class TextResponse:
    text: str
    retries: int
    retrieved_at: datetime


class ResilientHttpClient:
    def __init__(self, config: HttpConfig) -> None:
        self.config = config
        timeout = httpx.Timeout(
            connect=config.connect_timeout_s,
            read=config.read_timeout_s,
            write=config.write_timeout_s,
            pool=config.pool_timeout_s,
        )
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": config.user_agent, "Accept": "text/csv,*/*;q=0.1"},
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ResilientHttpClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def get_text(
        self,
        url: str,
        *,
        source: str,
        product: str,
        before_attempt: Callable[[], None] | None = None,
    ) -> TextResponse:
        """GET text, reserving quota immediately before every outbound attempt."""

        retry_count = 0
        exponential_wait = wait_exponential_jitter(
            initial=self.config.backoff_initial_s,
            max=self.config.backoff_max_s,
        )

        def wait_with_retry_after(state: RetryCallState) -> float:
            base_wait = float(exponential_wait(state))
            if state.outcome is None or not state.outcome.failed:
                return base_wait
            error = state.outcome.exception()
            retry_after = (
                error.retry_after if isinstance(error, RetryableHTTPError) else 0.0
            )
            return max(base_wait, retry_after)

        def before_sleep(state: RetryCallState) -> None:
            nonlocal retry_count
            retry_count += 1
            error = state.outcome.exception() if state.outcome else None
            status = error.status_code if isinstance(error, RetryableHTTPError) else None
            log_event(
                LOGGER,
                logging.WARNING,
                "http_retry",
                source=source,
                product=product,
                attempt=state.attempt_number,
                status=status,
                error_type=type(error).__name__ if error else None,
                wait_s=round(float(state.next_action.sleep), 3)
                if state.next_action
                else None,
            )

        retryer = Retrying(
            retry=retry_if_exception_type((RetryableHTTPError, httpx.TransportError)),
            wait=wait_with_retry_after,
            stop=stop_after_attempt(self.config.max_attempts)
            | stop_before_delay(self.config.max_total_wait_s),
            before_sleep=before_sleep,
            reraise=True,
        )

        def attempt() -> httpx.Response:
            if before_attempt is not None:
                before_attempt()
            response = self._client.get(url)
            if response.status_code == 429 or response.status_code >= 500:
                raise RetryableHTTPError(
                    response.status_code,
                    _parse_retry_after(response.headers.get("Retry-After")),
                )
            if 400 <= response.status_code < 500:
                raise NonRetryableHTTPError(response.status_code)
            return response

        try:
            response = retryer(attempt)
        except NonRetryableHTTPError:
            raise
        except (RetryableHTTPError, httpx.TransportError) as exc:
            detail = redact(str(exc))
            raise RetryExhaustedError(
                f"{source}/{product} exhausted HTTP retries: {detail}",
                retries=retry_count,
            ) from exc

        return TextResponse(
            text=response.text,
            retries=retry_count,
            retrieved_at=datetime.now(UTC),
        )


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return max(0.0, float(value.strip()))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0
