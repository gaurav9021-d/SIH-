"""Typed failures used by the ingestion boundary layers."""

from __future__ import annotations


class IngestionError(Exception):
    """Base class for expected ingestion failures."""


class ConfigurationError(IngestionError):
    """Configuration or a required secret is missing or invalid."""


class LeaseLostError(IngestionError):
    """A process no longer owns the lease for work it was performing."""


class UpstreamFormatError(IngestionError):
    """An upstream response does not match its documented format."""


class RetryableHTTPError(IngestionError):
    """HTTP response that may succeed after a delay."""

    def __init__(self, status_code: int, retry_after: float = 0.0) -> None:
        self.status_code = status_code
        self.retry_after = max(0.0, retry_after)
        super().__init__(f"retryable upstream HTTP status {status_code}")


class NonRetryableHTTPError(IngestionError):
    """HTTP response that should not be retried."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"non-retryable upstream HTTP status {status_code}")


class RetryExhaustedError(IngestionError):
    """A transient HTTP operation exhausted its retry budget."""

    def __init__(self, message: str, *, retries: int) -> None:
        self.retries = retries
        super().__init__(message)


class DatabaseUnavailableError(IngestionError):
    """A database operation exhausted its retry budget."""
