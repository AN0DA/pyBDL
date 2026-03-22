import asyncio
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Literal, cast, overload

import httpx
from hishel import AsyncSqliteStorage, FilterPolicy, SyncSqliteStorage
from hishel.httpx import AsyncCacheClient, SyncCacheClient
from tqdm import tqdm

from pybdl.api.exceptions import BDLHTTPError, BDLResponseError
from pybdl.api.utils.rate_limiter import AsyncRateLimiter, PersistentQuotaCache, RateLimiter
from pybdl.config import BDL_API_BASE_URL, DEFAULT_QUOTAS, BDLConfig, QuotaMap

# Centralized type literals for API parameters
LanguageLiteral = Literal["pl", "en"]
FormatLiteral = Literal["json", "jsonapi", "xml"]
AcceptHeaderLiteral = Literal["application/json", "application/vnd.api+json", "application/xml"]


class BaseAPIClient:
    """Base client for BDL API interactions with both sync and async support.

    This class provides:
    - Authentication and request caching
    - Proxy configuration
    - Response handling
    - Paginated fetching with optional progress bars (sync & async)
    """

    def __init__(self, config: BDLConfig, extra_headers: dict[str, str] | None = None):
        """
        Initialize base API client for BDL.

        Args:
            config: BDL configuration object.
            extra_headers: Optional extra headers (e.g., Accept-Language) to include in requests.
        """
        self.config = config
        is_registered = bool(config.api_key)
        quotas: QuotaMap = cast(
            QuotaMap,
            config.custom_quotas if config.custom_quotas is not None else DEFAULT_QUOTAS,
        )
        self._quota_cache = PersistentQuotaCache(
            config.quota_cache_enabled,
            cache_file=config.quota_cache_file,
            use_global_cache=config.use_global_cache,
        )
        self._sync_limiter = RateLimiter(
            quotas,
            is_registered,
            self._quota_cache,
            raise_on_limit=config.raise_on_rate_limit,
        )
        self._async_limiter = AsyncRateLimiter(
            quotas,
            is_registered,
            self._quota_cache,
            raise_on_limit=config.raise_on_rate_limit,
        )
        self._proxy_url = self._build_proxy_url()
        self._http_cache_path = self._build_http_cache_path()
        default_headers = self._build_default_headers(extra_headers)
        self.session = self._build_sync_client(default_headers)
        self._async_client = self._build_async_client(default_headers)

    def _build_proxy_url(self) -> str | None:
        if not self.config.proxy_url:
            return None
        if not (self.config.proxy_username and self.config.proxy_password):
            return self.config.proxy_url

        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(self.config.proxy_url)
        auth = f"{self.config.proxy_username}:{self.config.proxy_password}"
        return urlunparse(parsed._replace(netloc=f"{auth}@{parsed.netloc}"))

    def _build_default_headers(self, extra_headers: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["X-ClientId"] = self.config.api_key
        if extra_headers:
            headers.update({key: str(value) for key, value in extra_headers.items() if value is not None})
        return headers

    def _build_http_cache_path(self) -> Path | None:
        if self.config.cache_backend != "file":
            return None
        return self._quota_cache.cache_file.with_name("http_cache.db")

    def _build_cache_policy(self) -> FilterPolicy:
        return FilterPolicy()

    def _build_sync_client(self, default_headers: Mapping[str, str]) -> httpx.Client:
        if self.config.cache_backend == "memory":
            return SyncCacheClient(
                headers=default_headers,
                proxy=self._proxy_url,
                storage=SyncSqliteStorage(database_path=":memory:"),
                policy=self._build_cache_policy(),
            )
        if self.config.cache_backend == "file" and self._http_cache_path is not None:
            return SyncCacheClient(
                headers=default_headers,
                proxy=self._proxy_url,
                storage=SyncSqliteStorage(database_path=str(self._http_cache_path)),
                policy=self._build_cache_policy(),
            )
        return httpx.Client(headers=default_headers, proxy=self._proxy_url)

    def _build_async_client(self, default_headers: Mapping[str, str]) -> httpx.AsyncClient:
        if self.config.cache_backend == "memory":
            return AsyncCacheClient(
                headers=default_headers,
                proxy=self._proxy_url,
                storage=AsyncSqliteStorage(database_path=":memory:"),
                policy=self._build_cache_policy(),
            )
        if self.config.cache_backend == "file" and self._http_cache_path is not None:
            return AsyncCacheClient(
                headers=default_headers,
                proxy=self._proxy_url,
                storage=AsyncSqliteStorage(database_path=str(self._http_cache_path)),
                policy=self._build_cache_policy(),
            )
        return httpx.AsyncClient(headers=default_headers, proxy=self._proxy_url)

    def close(self) -> None:
        """Close synchronous HTTP resources."""
        self.session.close()

    async def aclose(self) -> None:
        """Close synchronous and asynchronous HTTP resources."""
        self.close()
        await self._async_client.aclose()

    def __enter__(self) -> "BaseAPIClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        self.close()
        return False

    async def __aenter__(self) -> "BaseAPIClient":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        await self.aclose()
        return False

    @staticmethod
    def _format_to_accept_header(format: FormatLiteral | None) -> str | None:
        """
        Convert format parameter to Accept header value.

        Args:
            format: Format string ("json", "jsonapi", or "xml").

        Returns:
            Accept header value or None if format is None.
        """
        if format is None:
            return None
        format_to_accept = {
            "json": "application/json",
            "jsonapi": "application/vnd.api+json",
            "xml": "application/xml",
        }
        return format_to_accept.get(format)

    def _prepare_api_params_and_headers(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """
        Prepare query parameters and headers for API requests.

        Args:
            lang: Language code (defaults to config.language if not provided).
            format: Format string (defaults to config.format if not provided).
            if_none_match: If-None-Match header value.
            if_modified_since: If-Modified-Since header value.
            extra_params: Additional query parameters to merge.

        Returns:
            Tuple of (params dict, headers dict).
        """
        # Set defaults from config
        if lang is None:
            lang_val = self.config.language.value if hasattr(self.config.language, "value") else self.config.language
            lang = lang_val if isinstance(lang_val, str) else lang_val.value  # type: ignore[assignment]
        if format is None:
            format_val = self.config.format.value if hasattr(self.config.format, "value") else self.config.format
            format = format_val if isinstance(format_val, str) else format_val.value  # type: ignore[assignment]

        params: dict[str, Any] = {}
        if lang:
            params["lang"] = lang
        if format:
            params["format"] = format
        if extra_params:
            params.update(extra_params)

        headers: dict[str, str] = {}
        # Set Accept-Language header from lang parameter
        if lang:
            headers["Accept-Language"] = lang
        # Set Accept header from format parameter
        accept_header = BaseAPIClient._format_to_accept_header(format)
        if accept_header:
            headers["Accept"] = accept_header
        if if_none_match:
            headers["If-None-Match"] = if_none_match
        if if_modified_since:
            headers["If-Modified-Since"] = if_modified_since

        return params, headers

    def _build_url(self, endpoint: str) -> str:
        """
        Build the full API URL for a given endpoint.

        Args:
            endpoint: API endpoint path (without base URL).

        Returns:
            Full URL string for the API endpoint.
        """
        endpoint = endpoint.strip("/")
        return f"{BDL_API_BASE_URL}/{endpoint}"

    @staticmethod
    def _metadata_from_response(data: dict[str, Any], results_key: str) -> dict[str, Any]:
        return {key: value for key, value in data.items() if key not in {results_key, "page", "pageSize", "links"}}

    def _merge_headers(self, headers: Mapping[str, str] | None = None) -> dict[str, str]:
        request_headers = {key: str(value) for key, value in self.session.headers.items()}
        if headers:
            request_headers.update({key: str(value) for key, value in headers.items()})
        return request_headers

    def _extract_error_detail(self, response: httpx.Response) -> Any:
        try:
            return response.json()
        except Exception:
            return response.text

    def _parse_response_json(self, response: httpx.Response) -> dict[str, Any]:
        try:
            data = response.json()
        except Exception as exc:
            raise BDLResponseError("Received a non-JSON response from the BDL API.", payload=response.text) from exc
        if not isinstance(data, dict):
            raise BDLResponseError("Expected a JSON object response from the BDL API.", payload=data)
        if "error" in data:
            raise BDLResponseError("BDL API returned an error payload.", payload=data)
        return data

    def _process_response(self, response: httpx.Response) -> dict[str, Any]:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise BDLHTTPError(
                status_code=response.status_code,
                response_body=self._extract_error_detail(response),
                url=str(response.url),
            ) from exc
        return self._parse_response_json(response)

    def _should_retry_status(self, status_code: int) -> bool:
        return status_code in self.config.retry_status_codes

    @staticmethod
    def _parse_retry_after_seconds(response: httpx.Response) -> float | None:
        raw = response.headers.get("Retry-After")
        if raw is None:
            return None
        text = str(raw).strip()
        if not text:
            return None
        try:
            return max(0.0, float(text))
        except ValueError:
            pass
        try:
            dt = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        now = datetime.now(dt.tzinfo)
        return max(0.0, (dt - now).total_seconds())

    def _retry_delay(self, attempt: int, response: httpx.Response | None = None) -> float:
        if response is not None:
            parsed = BaseAPIClient._parse_retry_after_seconds(response)
            if parsed is not None:
                return min(parsed, self.config.max_retry_delay)
        delay = self.config.retry_backoff_factor * (2**attempt)
        return min(delay, self.config.max_retry_delay)

    def _retry_delay_after_429(self, attempt: int, response: httpx.Response) -> float:
        """Wait after HTTP 429: honor ``Retry-After`` when set, else exponential backoff."""
        parsed = self._parse_retry_after_seconds(response)
        if parsed is not None:
            return min(parsed, self.config.http_429_max_delay)
        delay = self.config.retry_backoff_factor * (2**attempt)
        return min(self.config.http_429_max_delay, delay)

    def _request_sync_url(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        query = params.copy() if params else None
        lang = self.config.language.value if hasattr(self.config.language, "value") else self.config.language
        if params is not None or "?" not in url:
            query = query or {}
            query.setdefault("lang", lang)
        request_headers = self._merge_headers(headers)

        last_error: Exception | None = None
        retries_other = 0
        retries_429 = 0
        max_iterations = self.config.request_retries + self.config.http_429_max_retries + 500

        for _ in range(max_iterations):
            reservation = self._sync_limiter.acquire()
            try:
                response = self.session.request(method, url, params=query, headers=request_headers)
            except httpx.HTTPError as exc:
                last_error = exc
                if retries_other < self.config.request_retries:
                    retries_other += 1
                    time.sleep(self._retry_delay(retries_other - 1))
                    continue
                raise BDLHTTPError(
                    status_code=None,
                    response_body=str(exc),
                    url=url,
                ) from exc

            if response.extensions.get("hishel_from_cache", False):
                self._sync_limiter.release(reservation)

            if response.status_code == 429 and 429 in self.config.retry_status_codes:
                if retries_429 < self.config.http_429_max_retries:
                    retries_429 += 1
                    time.sleep(self._retry_delay_after_429(retries_429 - 1, response))
                    continue
                return self._process_response(response)

            if (
                self._should_retry_status(response.status_code)
                and response.status_code != 429
                and retries_other < self.config.request_retries
            ):
                retries_other += 1
                time.sleep(self._retry_delay(retries_other - 1, response))
                continue

            return self._process_response(response)

        raise BDLHTTPError(status_code=None, response_body=str(last_error), url=url)

    def _request_sync(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Make a single HTTP request (sync).

        Args:
            endpoint: API endpoint (path).
            method: HTTP method (default: GET).
            params: Query parameters (merged into the request).
            headers: Optional request headers.

        Returns:
            Decoded JSON response as a dictionary.
        """
        return self._request_sync_url(
            self._build_url(endpoint),
            method=method,
            params=params,
            headers=headers,
        )

    def _paginated_request_sync(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        return_all: bool = True,
    ) -> Iterator[dict[str, Any]]:
        """
        Fetch all paginated results synchronously.

        Args:
            endpoint: API endpoint.
            method: HTTP method.
            params: Optional query parameters.
            headers: Optional request headers.
            results_key: Key in JSON response where data resides.
            page_size: Items per page.
            max_pages: Maximum pages to yield.
            return_all: If False, only returns first page.

        Yields:
            Response for each page as a dictionary.
        """
        query = params.copy() if params else {}
        lang = self.config.language.value if hasattr(self.config.language, "value") else self.config.language
        query.setdefault("lang", lang)
        query["page-size"] = page_size

        fetched_pages = 0
        next_url = None
        first_page = True

        while True:
            if first_page:
                resp = self._request_sync(endpoint, method=method, params=query, headers=headers)
                first_page = False
            else:
                if not next_url:
                    break
                resp = self._request_sync_url(next_url, method=method, headers=headers)

            if results_key not in resp:
                raise BDLResponseError(f"Response does not contain key '{results_key}'", payload=resp)
            if not resp.get(results_key):
                break
            yield resp

            fetched_pages += 1
            if not return_all or (max_pages and fetched_pages >= max_pages):
                break

            links = resp.get("links", {})
            next_url = links.get("next")
            if not next_url:
                break

    @overload
    def fetch_all_results(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        return_metadata: Literal[False] = False,
        show_progress: bool = True,
    ) -> list[dict[str, Any]]: ...

    @overload
    def fetch_all_results(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        return_metadata: Literal[True],
        show_progress: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    def fetch_all_results(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        return_metadata: bool = False,
        show_progress: bool = True,
    ) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], dict[str, Any]]:
        """
        Fetch paginated results synchronously and combine them into a single list.

        Args:
            endpoint: API endpoint.
            method: HTTP method (default: GET).
            params: Query parameters.
            headers: Optional request headers.
            results_key: Key for extracting data from each page.
            page_size: Items per page.
            max_pages: Optional limit of pages.
            return_metadata: If True, return (results, metadata).
            show_progress: Display progress via tqdm.

        Returns:
            Combined list of results, optionally with metadata.
        """
        all_results: list[dict[str, Any]] = []
        metadata: dict[str, Any] = {}
        progress_bar = (
            tqdm(desc=f"Fetching {endpoint.split('/')[-1]}", unit=" pages", leave=True) if show_progress else None
        )

        first_page = True
        try:
            for page in self._paginated_request_sync(
                endpoint,
                method=method,
                params=params,
                headers=headers,
                results_key=results_key,
                page_size=page_size,
                max_pages=max_pages,
            ):
                if results_key not in page:
                    raise BDLResponseError(f"Response does not contain key '{results_key}'", payload=page)
                if first_page and return_metadata:
                    metadata = self._metadata_from_response(page, results_key)
                    if progress_bar is not None and "totalCount" in page:
                        total_pages = (page["totalCount"] + page_size - 1) // page_size
                        total_pages = min(total_pages, max_pages) if max_pages else total_pages
                        progress_bar.total = total_pages
                    first_page = False

                all_results.extend(page.get(results_key, []))

                if progress_bar is not None:
                    progress_bar.update(1)
                    progress_bar.set_postfix({"items": len(all_results)})
        finally:
            if progress_bar is not None:
                progress_bar.close()

        return (all_results, metadata) if return_metadata else all_results

    def fetch_all_results_with_metadata(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        show_progress: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return self.fetch_all_results(
            endpoint,
            method=method,
            params=params,
            headers=headers,
            results_key=results_key,
            page_size=page_size,
            max_pages=max_pages,
            return_metadata=True,
            show_progress=show_progress,
        )

    @overload
    def fetch_single_result(
        self,
        endpoint: str,
        *,
        results_key: None = None,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_metadata: Literal[False] = False,
    ) -> dict[str, Any]: ...

    @overload
    def fetch_single_result(
        self,
        endpoint: str,
        *,
        results_key: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    @overload
    def fetch_single_result(
        self,
        endpoint: str,
        *,
        results_key: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_metadata: Literal[True],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    @overload
    def fetch_single_result(
        self,
        endpoint: str,
        *,
        results_key: None = None,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_metadata: Literal[True],
    ) -> tuple[dict[str, Any], dict[str, Any]]: ...

    def fetch_single_result(
        self,
        endpoint: str,
        *,
        results_key: str | None = None,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_metadata: bool = False,
    ) -> (
        dict[str, Any]
        | list[dict[str, Any]]
        | tuple[list[dict[str, Any]], dict[str, Any]]
        | tuple[dict[str, Any], dict[str, Any]]
    ):
        """
        Fetch a single result, non-paginated (sync).

        Args:
            endpoint: API endpoint.
            results_key: If not None, extract this key from the JSON.
            method: HTTP method.
            params: Query parameters.
            headers: Optional request headers.
            return_metadata: Also return metadata if True.

        Returns:
            Dictionary or list, optionally with separate metadata.
        """
        response = self._request_sync(
            endpoint=endpoint,
            method=method,
            params=params,
            headers=headers,
        )

        if results_key is None:
            if return_metadata:
                return response, {}
            return response

        if not isinstance(response, dict) or results_key not in response:
            raise BDLResponseError(f"Response does not contain key '{results_key}'", payload=response)

        results_val = response[results_key]
        if return_metadata:
            metadata = self._metadata_from_response(response, results_key)
            return results_val, metadata

        return results_val

    def fetch_single_result_with_metadata(
        self,
        endpoint: str,
        *,
        results_key: str | None = None,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]] | tuple[list[dict[str, Any]], dict[str, Any]]:
        return self.fetch_single_result(
            endpoint,
            results_key=results_key,
            method=method,
            params=params,
            headers=headers,
            return_metadata=True,
        )

    def _fetch_collection_endpoint(
        self,
        endpoint: str,
        *,
        extra_params: dict[str, Any] | None = None,
        lang: LanguageLiteral | None = None,
        format: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        results_key: str = "results",
    ) -> list[dict[str, Any]]:
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=cast(FormatLiteral | None, format),
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        if max_pages == 1:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return self.fetch_single_result(
                endpoint,
                results_key=results_key,
                params=params_with_page_size,
                headers=headers,
            )

        return self.fetch_all_results(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            results_key=results_key,
        )

    def _fetch_detail_endpoint(
        self,
        endpoint: str,
        *,
        extra_params: dict[str, Any] | None = None,
        lang: LanguageLiteral | None = None,
        format: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
    ) -> dict[str, Any]:
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=cast(FormatLiteral | None, format),
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )
        return self.fetch_single_result(endpoint, params=params or None, headers=headers or None)

    async def _request_async_url(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        query = params.copy() if params else None
        lang = self.config.language.value if hasattr(self.config.language, "value") else self.config.language
        if params is not None or "?" not in url:
            query = query or {}
            query.setdefault("lang", lang)
        request_headers = self._merge_headers(headers)

        last_error: Exception | None = None
        retries_other = 0
        retries_429 = 0
        max_iterations = self.config.request_retries + self.config.http_429_max_retries + 500

        for _ in range(max_iterations):
            reservation = await self._async_limiter.acquire()
            try:
                response = await self._async_client.request(method, url, params=query, headers=request_headers)
            except httpx.HTTPError as exc:
                last_error = exc
                if retries_other < self.config.request_retries:
                    retries_other += 1
                    await asyncio.sleep(self._retry_delay(retries_other - 1))
                    continue
                raise BDLHTTPError(
                    status_code=getattr(getattr(exc, "response", None), "status_code", None),
                    response_body=str(exc),
                    url=url,
                ) from exc

            if response.extensions.get("hishel_from_cache", False):
                await self._async_limiter.release(reservation)

            if response.status_code == 429 and 429 in self.config.retry_status_codes:
                if retries_429 < self.config.http_429_max_retries:
                    retries_429 += 1
                    await asyncio.sleep(self._retry_delay_after_429(retries_429 - 1, response))
                    continue
                return self._process_response(response)

            if (
                self._should_retry_status(response.status_code)
                and response.status_code != 429
                and retries_other < self.config.request_retries
            ):
                retries_other += 1
                await asyncio.sleep(self._retry_delay(retries_other - 1, response))
                continue

            return self._process_response(response)

        raise BDLHTTPError(status_code=None, response_body=str(last_error), url=url)

    async def _request_async(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Make a single HTTP request (async).
        """
        return await self._request_async_url(
            self._build_url(endpoint),
            method=method,
            params=params,
            headers=headers,
        )

    async def _paginated_request_async(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        return_all: bool = True,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Fetch all paginated results asynchronously.

        Yields each page's JSON as a dict.
        """
        query = params.copy() if params else {}
        lang = self.config.language.value if hasattr(self.config.language, "value") else self.config.language
        query.setdefault("lang", lang)
        query["page-size"] = page_size

        fetched_pages = 0
        next_url = None
        first_page = True

        while True:
            if first_page:
                resp = await self._request_async(endpoint, method=method, params=query, headers=headers)
                first_page = False
            else:
                if not next_url:
                    break
                resp = await self._request_async_url(next_url, method=method, headers=headers)

            if results_key not in resp:
                raise BDLResponseError(f"Response does not contain key '{results_key}'", payload=resp)
            if not resp.get(results_key):
                break

            yield resp
            fetched_pages += 1
            if not return_all or (max_pages and fetched_pages >= max_pages):
                break
            next_url = resp.get("links", {}).get("next")
            if not next_url:
                break

    @overload
    async def afetch_all_results(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        return_metadata: Literal[False] = False,
        show_progress: bool = True,
    ) -> list[dict[str, Any]]: ...

    @overload
    async def afetch_all_results(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        return_metadata: Literal[True],
        show_progress: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    async def afetch_all_results(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        return_metadata: bool = False,
        show_progress: bool = True,
    ) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], dict[str, Any]]:
        """
        Asynchronously fetch paginated results and combine them into a single list.

        Args:
            endpoint: API endpoint.
            method: HTTP method (default: GET).
            params: Query parameters.
            headers: Optional request headers.
            results_key: Key for extracting data from each page.
            page_size: Items per page.
            max_pages: Optional limit of pages.
            return_metadata: If True, return (results, metadata).
            show_progress: Display progress via tqdm.

        Returns:
            Combined list of results, optionally with metadata.
        """
        all_results: list[dict[str, Any]] = []
        metadata: dict[str, Any] = {}
        first_page = True
        progress_bar = (
            tqdm(desc=f"Fetching {endpoint.split('/')[-1]} (async)", unit=" pages", leave=True)
            if show_progress
            else None
        )

        try:
            async for page in self._paginated_request_async(
                endpoint,
                method=method,
                params=params,
                headers=headers,
                results_key=results_key,
                page_size=page_size,
                max_pages=max_pages,
            ):
                if results_key not in page:
                    raise BDLResponseError(f"Response does not contain key '{results_key}'", payload=page)
                if first_page and return_metadata:
                    metadata = self._metadata_from_response(page, results_key)
                    if progress_bar is not None and "totalCount" in page:
                        total_pages = (page["totalCount"] + page_size - 1) // page_size
                        total_pages = min(total_pages, max_pages) if max_pages else total_pages
                        progress_bar.total = total_pages
                    first_page = False

                all_results.extend(page.get(results_key, []))
                if progress_bar is not None:
                    progress_bar.update(1)
                    progress_bar.set_postfix({"items": len(all_results)})
        finally:
            if progress_bar is not None:
                progress_bar.close()

        return (all_results, metadata) if return_metadata else all_results

    async def afetch_all_results_with_metadata(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_key: str = "results",
        page_size: int = 100,
        max_pages: int | None = None,
        show_progress: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return await self.afetch_all_results(
            endpoint,
            method=method,
            params=params,
            headers=headers,
            results_key=results_key,
            page_size=page_size,
            max_pages=max_pages,
            return_metadata=True,
            show_progress=show_progress,
        )

    @overload
    async def afetch_single_result(
        self,
        endpoint: str,
        *,
        method: str = "GET",
        results_key: Literal[None] = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_metadata: Literal[False] = False,
    ) -> dict[str, Any]: ...

    @overload
    async def afetch_single_result(
        self,
        endpoint: str,
        *,
        results_key: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    @overload
    async def afetch_single_result(
        self,
        endpoint: str,
        *,
        return_metadata: Literal[True],
        results_key: Literal[None] = None,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]: ...

    @overload
    async def afetch_single_result(
        self,
        endpoint: str,
        *,
        results_key: str,
        return_metadata: Literal[True],
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    async def afetch_single_result(
        self,
        endpoint: str,
        *,
        results_key: str | None = None,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_metadata: bool = False,
    ) -> (
        dict[str, Any]
        | list[dict[str, Any]]
        | tuple[dict[str, Any], dict[str, Any]]
        | tuple[list[dict[str, Any]], dict[str, Any]]
    ):
        """
        Asynchronously fetch a single result, non-paginated.

        Args:
            endpoint: API endpoint.
            results_key: If not None, extract this key from the JSON.
            method: HTTP method.
            params: Query parameters.
            headers: Optional request headers.
            return_metadata: Also return metadata if True.

        Returns:
            Dictionary or list, optionally with separate metadata.
        """
        response = await self._request_async(
            endpoint=endpoint,
            method=method,
            params=params,
            headers=headers,
        )

        if results_key is None:
            return (response, {}) if return_metadata else response

        if not isinstance(response, dict) or results_key not in response:
            raise BDLResponseError(f"Response does not contain key '{results_key}'", payload=response)

        results_val = cast(list[dict[str, Any]], response[results_key])
        if return_metadata:
            metadata = self._metadata_from_response(response, results_key)
            return results_val, metadata

        return results_val

    async def afetch_single_result_with_metadata(
        self,
        endpoint: str,
        *,
        results_key: str | None = None,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]] | tuple[list[dict[str, Any]], dict[str, Any]]:
        return await self.afetch_single_result(
            endpoint,
            results_key=results_key,
            method=method,
            params=params,
            headers=headers,
            return_metadata=True,
        )

    async def _afetch_collection_endpoint(
        self,
        endpoint: str,
        *,
        extra_params: dict[str, Any] | None = None,
        lang: LanguageLiteral | None = None,
        format: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        results_key: str = "results",
    ) -> list[dict[str, Any]]:
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=cast(FormatLiteral | None, format),
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        if max_pages == 1:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return await self.afetch_single_result(
                endpoint,
                results_key=results_key,
                params=params_with_page_size,
                headers=headers,
            )

        return await self.afetch_all_results(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            results_key=results_key,
        )

    async def _afetch_detail_endpoint(
        self,
        endpoint: str,
        *,
        extra_params: dict[str, Any] | None = None,
        lang: LanguageLiteral | None = None,
        format: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
    ) -> dict[str, Any]:
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=cast(FormatLiteral | None, format),
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )
        return await self.afetch_single_result(endpoint, params=params or None, headers=headers or None)
