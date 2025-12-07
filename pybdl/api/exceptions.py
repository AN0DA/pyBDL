"""Exceptions for pyBDL API client."""

from typing import Any


class GUSBDLError(Exception):
    """Base exception for all GUS BDL API errors."""

    pass


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
