from typing import Any

from pyldb.api.client import (
    DEFAULT_FORMAT,
    DEFAULT_LANG,
    FormatLiteral,
    LanguageLiteral,
    BaseAPIClient,
)


class AggregatesAPI(BaseAPIClient):
    """
    Client for the LDB /aggregates endpoints.

    Provides access to aggregation level metadata, listing and detail of
    aggregates, and aggregates API metadata within the Local Data Bank (LDB).
    """

    def list_aggregates(
        self,
        sort: str | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        List all aggregates, optionally sorted.

        Maps to: GET /aggregates

        Args:
            sort: Sorting order, e.g., 'Id', '-Id', 'Name', '-Name', etc.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of aggregate metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if sort:
            extra_params["sort"] = sort
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        return self.fetch_all_results(
            "aggregates", params=params, headers=headers if headers else None, page_size=page_size, max_pages=max_pages
        )

    def get_aggregate(
        self,
        aggregate_id: str,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve metadata details for a specific aggregate.

        Maps to: GET /aggregates/{id}

        Args:
            aggregate_id: Aggregate identifier.
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with aggregate metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            f"aggregates/{aggregate_id}", params=params if params else None, headers=headers if headers else None
        )

    def get_aggregates_metadata(
        self,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        List all aggregates metadata.

        Maps to: GET /aggregates/metadata

        Args:
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with aggregate metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            "aggregates/metadata", params=params if params else None, headers=headers if headers else None
        )

    async def alist_aggregates(
        self,
        sort: str | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Asynchronously list all aggregates, optionally sorted.

        Maps to: GET /aggregates

        Args:
            sort: Sorting order, e.g., 'Id', '-Id', 'Name', '-Name', etc.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of aggregate metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if sort:
            extra_params["sort"] = sort
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        return await self.afetch_all_results(
            "aggregates", params=params, headers=headers if headers else None, page_size=page_size, max_pages=max_pages
        )

    async def aget_aggregate(
        self,
        aggregate_id: str,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve metadata details for a specific aggregate.

        Maps to: GET /aggregates/{id}

        Args:
            aggregate_id: Aggregate identifier.
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with aggregate metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            f"aggregates/{aggregate_id}", params=params if params else None, headers=headers if headers else None
        )

    async def aget_aggregates_metadata(
        self,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously list all aggregates metadata.

        Maps to: GET /aggregates/metadata

        Args:
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with aggregate metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            "aggregates/metadata", params=params if params else None, headers=headers if headers else None
        )
