"""Exceptions for pyBDL API client."""

from typing import Any


class GUSBDLError(Exception):
    """Base exception for all GUS BDL API errors."""


class BDLHTTPError(GUSBDLError):
    """Raised when the BDL API responds with an HTTP error or the request fails."""

    def __init__(
        self,
        *,
        status_code: int | None,
        response_body: Any = None,
        url: str | None = None,
        message: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.response_body = response_body
        self.url = url

        if message is None:
            parts = ["BDL request failed"]
            if status_code is not None:
                parts.append(f"with HTTP {status_code}")
            if url:
                parts.append(f"for {url}")
            if response_body not in (None, ""):
                parts.append(f": {response_body}")
            message = " ".join(parts)
        super().__init__(message)


class BDLResponseError(GUSBDLError):
    """Raised when the BDL API returns an unexpected or invalid payload."""

    def __init__(self, message: str, *, payload: Any = None) -> None:
        self.payload = payload
        super().__init__(message)


class RateLimitError(GUSBDLError):
    """Raised when rate limit is exceeded."""

    def __init__(
        self,
        retry_after: float,
        limit_info: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> None:
        self.retry_after = retry_after
        self.limit_info = limit_info or {}

        if message is None:
            periods = ", ".join(f"{info['limit']} req/{info['period']}s" for info in self.limit_info.get("quotas", []))
            message = f"Rate limit exceeded ({periods}). Retry after {retry_after:.1f}s."

        super().__init__(message)


class RateLimitDelayExceeded(RateLimitError):
    """Raised when required delay exceeds max_delay setting."""

    def __init__(
        self,
        actual_delay: float,
        max_delay: float,
        limit_info: dict[str, Any] | None = None,
    ) -> None:
        self.actual_delay = actual_delay
        self.max_delay = max_delay

        message = f"Required delay ({actual_delay:.1f}s) exceeds maximum allowed delay ({max_delay:.1f}s)."
        super().__init__(
            retry_after=actual_delay,
            limit_info=limit_info,
            message=message,
        )
