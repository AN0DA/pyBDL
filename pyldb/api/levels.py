from typing import Any

from pyldb.api.client import (
    DEFAULT_FORMAT,
    DEFAULT_LANG,
    FormatLiteral,
    LanguageLiteral,
    BaseAPIClient,
)


class LevelsAPI(BaseAPIClient):
    """
    Client for the LDB /levels endpoints.

    Provides access to administrative unit aggregation levels and their metadata
    in the Local Data Bank (LDB).
    """

    def list_levels(
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
        List all administrative unit aggregation levels.

        Maps to: GET /levels

        Args:
            sort: Optional sorting order, e.g., 'Id', '-Id', 'Name', '-Name'.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of aggregation level metadata dictionaries.
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
            "levels", params=params, headers=headers if headers else None, page_size=page_size, max_pages=max_pages
        )

    def get_level(
        self,
        level_id: int,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve metadata for a specific aggregation level.

        Maps to: GET /levels/{id}

        Args:
            level_id: Aggregation level identifier (integer).
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with level metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            f"levels/{level_id}", params=params if params else None, headers=headers if headers else None
        )

    def get_levels_metadata(
        self,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve general metadata and version information for the /levels endpoint.

        Maps to: GET /levels/metadata

        Args:
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with API metadata and versioning info.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            "levels/metadata", params=params if params else None, headers=headers if headers else None
        )

    async def alist_levels(
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
        Asynchronously list all administrative unit aggregation levels.

        Maps to: GET /levels

        Args:
            sort: Optional sorting order, e.g., 'Id', '-Id', 'Name', '-Name'.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of aggregation level metadata dictionaries.
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
            "levels", params=params, headers=headers if headers else None, page_size=page_size, max_pages=max_pages
        )

    async def aget_level(
        self,
        level_id: int,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve metadata for a specific aggregation level.

        Maps to: GET /levels/{id}

        Args:
            level_id: Aggregation level identifier (integer).
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with level metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            f"levels/{level_id}", params=params if params else None, headers=headers if headers else None
        )

    async def aget_levels_metadata(
        self,
        lang: LanguageLiteral | None = DEFAULT_LANG,
        format: FormatLiteral | None = DEFAULT_FORMAT,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve general metadata and version information for the /levels endpoint.

        Maps to: GET /levels/metadata

        Args:
            lang: Expected response content language (default: "en").
            format: Expected response content type (default: "json").
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with API metadata and versioning info.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            "levels/metadata", params=params if params else None, headers=headers if headers else None
        )
