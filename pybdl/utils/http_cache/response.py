"""Helpers for hishel-backed HTTP responses."""

import httpx


def is_from_http_cache(response: httpx.Response) -> bool:
    """True when hishel served the response from cache."""
    return bool(response.extensions.get("hishel_from_cache", False))
