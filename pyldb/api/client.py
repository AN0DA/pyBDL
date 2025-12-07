from collections.abc import AsyncIterator, Iterator
from typing import Any, Literal, cast, overload

import httpx
from requests import HTTPError, Response, Session
from requests_cache import CachedSession
from tqdm import tqdm

from pyldb.api.utils.rate_limiter import AsyncRateLimiter, PersistentQuotaCache, RateLimiter
from pyldb.config import DEFAULT_QUOTAS, LDB_API_BASE_URL, LDBConfig

# Centralized type literals for API parameters
LanguageLiteral = Literal["pl", "en"]
FormatLiteral = Literal["json", "jsonapi", "xml"]
AcceptHeaderLiteral = Literal["application/json", "application/vnd.api+json", "application/xml"]


class BaseAPIClient:
    """Base client for LDB API interactions with both sync and async support.

    This class provides:
    - Authentication and request caching
    - Proxy configuration
    - Response handling
    - Paginated fetching with optional progress bars (sync & async)
    """

    _global_sync_limiters: dict[bool, RateLimiter] = {}
    _global_async_limiters: dict[bool, AsyncRateLimiter] = {}
    _quota_cache = None

    def __init__(self, config: LDBConfig, extra_headers: dict[str, str] | None = None):
        """
        Initialize base API client for LDB.

        Args:
            config: LDB configuration object.
            extra_headers: Optional extra headers (e.g., Accept-Language) to include in requests.
        """
        self.config = config
        self.session: CachedSession | Session

        # Initialize session with caching if enabled
        if config.use_cache:
            self.session = CachedSession(
                expire_after=config.cache_expire_after,
                backend="memory",
            )
        else:
            self.session = Session()

        if config.proxy_url:
            proxies = {
                "http": config.proxy_url,
                "https": config.proxy_url,
            }
            if config.proxy_username and config.proxy_password:
                from urllib.parse import urlparse, urlunparse

                parsed = urlparse(config.proxy_url)
                auth = f"{config.proxy_username}:{config.proxy_password}"
                new_netloc = f"{auth}@{parsed.netloc}"
                auth_proxy_url = urlunparse(parsed._replace(netloc=new_netloc))
                proxies = {
                    "http": auth_proxy_url,
                    "https": auth_proxy_url,
                }
            self.session.proxies.update(proxies)

        # Headers
        self.session.headers.update(
            {
                "Content-Type": "application/json",
            }
        )
        if config.api_key:
            self.session.headers.update({"X-ClientId": config.api_key})
        if extra_headers:
            # Ensure all header values are strings
            self.session.headers.update({k: str(v) for k, v in extra_headers.items() if v is not None})

        # Determine registration status based on api_key presence
        is_registered = bool(config.api_key)

        # Determine quotas
        # If custom_quotas is set, use those (single int values)
        # Otherwise, use DEFAULT_QUOTAS (tuple format, rate limiter will select based on is_registered)
        if config.custom_quotas is not None:  # noqa: SIM108
            quotas = config.custom_quotas
        else:
            quotas = DEFAULT_QUOTAS

        if BaseAPIClient._quota_cache is None:
            BaseAPIClient._quota_cache = PersistentQuotaCache(config.quota_cache_enabled)

        # Use separate limiters for registered vs anonymous users
        # When custom_quotas are provided, create per-instance limiters (not shared)
        # Otherwise, reuse shared limiters based on registration status
        if config.custom_quotas is not None:
            # Custom quotas are client-specific, create per-instance limiters
            self._sync_limiter = RateLimiter(quotas, is_registered, BaseAPIClient._quota_cache)
            self._async_limiter = AsyncRateLimiter(quotas, is_registered, BaseAPIClient._quota_cache)
        else:
            # Use shared limiters for DEFAULT_QUOTAS (keyed by registration status)
            if is_registered not in BaseAPIClient._global_sync_limiters:
                BaseAPIClient._global_sync_limiters[is_registered] = RateLimiter(
                    quotas, is_registered, BaseAPIClient._quota_cache
                )
            if is_registered not in BaseAPIClient._global_async_limiters:
                BaseAPIClient._global_async_limiters[is_registered] = AsyncRateLimiter(
                    quotas, is_registered, BaseAPIClient._quota_cache
                )

            self._sync_limiter = BaseAPIClient._global_sync_limiters[is_registered]
            self._async_limiter = BaseAPIClient._global_async_limiters[is_registered]

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
        return f"{LDB_API_BASE_URL}/{endpoint}"

    def _process_response(self, response: Response) -> dict[str, Any]:
        """
        Process and validate an API response.

        Args:
            response: HTTP response object.

        Returns:
            Decoded JSON response as a dictionary.

        Raises:
            RuntimeError: If the response contains an HTTP error.
            ValueError: If the API returns an error in the response body.
        """
        try:
            response.raise_for_status()
        except HTTPError as exc:
            try:
                error_detail = response.json()
            except Exception:
                error_detail = response.text
            raise RuntimeError(f"HTTP error {response.status_code}: {error_detail}") from exc

        data = response.json()
        if "error" in data:
            raise ValueError(f"API Error: {data['error']}")
        return data

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
        self._sync_limiter.acquire()
        url = self._build_url(endpoint)

        query = params.copy() if params else {}
        lang = self.config.language.value if hasattr(self.config.language, "value") else self.config.language
        query.setdefault("lang", lang)

        req_headers: dict[str, str] = {k: str(v) for k, v in self.session.headers.items()}
        if headers:
            req_headers.update(headers)

        response = self.session.request(method, url, params=query, headers=req_headers)
        return self._process_response(response)

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
                req_headers: dict[str, str] = {k: str(v) for k, v in self.session.headers.items()}
                response = self.session.request(method, next_url, headers=req_headers)
                resp = self._process_response(response)

            if results_key not in resp:
                raise ValueError(f"Response does not contain key '{results_key}'")
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
        all_results = []
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
                    raise ValueError(f"Response does not contain key '{results_key}'")
                if first_page and return_metadata:
                    metadata = {k: v for k, v in page.items() if k not in {results_key, "page", "pageSize", "links"}}
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
            raise ValueError(f"Response does not contain key '{results_key}'")

        results_val = response[results_key]
        if return_metadata:
            metadata = {k: v for k, v in response.items() if k not in {results_key, "page", "pageSize", "links"}}
            return results_val, metadata

        return results_val

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
        await self._async_limiter.acquire()
        url = self._build_url(endpoint)

        query = params.copy() if params else {}
        lang = self.config.language.value if hasattr(self.config.language, "value") else self.config.language
        query.setdefault("lang", lang)

        req_headers: dict[str, str] = {k: str(v) for k, v in self.session.headers.items()}
        if headers:
            req_headers.update(headers)

        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, params=query, headers=req_headers)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                try:
                    error_detail = response.json()
                except Exception:
                    error_detail = response.text
                raise RuntimeError(f"HTTP error {response.status_code}: {error_detail}") from exc

        data = response.json()
        if "error" in data:
            raise ValueError(f"API Error: {data['error']}")
        return data

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

        str_headers: dict[str, str] = {k: str(v) for k, v in self.session.headers.items()}

        async with httpx.AsyncClient() as client:
            while True:
                if first_page:
                    resp = await self._request_async(endpoint, method=method, params=query, headers=headers)
                    first_page = False
                else:
                    if not next_url:
                        break
                    response = await client.request(method, next_url, headers=str_headers)
                    try:
                        response.raise_for_status()
                    except httpx.HTTPStatusError as exc:
                        try:
                            error_detail = response.json()
                        except Exception:
                            error_detail = response.text
                        raise RuntimeError(f"HTTP error {response.status_code}: {error_detail}") from exc
                    resp = response.json()

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
                    raise ValueError(f"Response does not contain key '{results_key}'")
                if first_page and return_metadata:
                    metadata = {k: v for k, v in page.items() if k not in {results_key, "page", "pageSize", "links"}}
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
            raise ValueError(f"Response does not contain key '{results_key}'")

        results_val = cast(list[dict[str, Any]], response[results_key])
        if return_metadata:
            metadata = {k: v for k, v in response.items() if k not in {results_key, "page", "pageSize", "links"}}
            return results_val, metadata

        return results_val
