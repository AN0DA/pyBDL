from typing import Any

from pyldb.api.client import (
    FormatLiteral,
    LanguageLiteral,
    BaseAPIClient,
)


class UnitsAPI(BaseAPIClient):
    """
    Client for the LDB /units endpoints.

    Provides access to administrative unit metadata in the Local Data Bank (LDB),
    including listing units (with filtering by level, parent, etc.), retrieving unit details,
    and accessing general units API metadata.
    """

    def list_units(
        self,
        parent_id: str | None = None,
        level: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
    ) -> list[dict[str, Any]]:
        """
        List all administrative units, optionally filtered by level, parent, or name.

        Maps to: GET /units

        Args:
            parent_id: Optional parent unit ID.
            level: Optional list of administrative levels to filter by.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order, e.g. 'id', '-id', 'name', '-name'.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.

        Returns:
            List of unit metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if parent_id:
            extra_params["parent-id"] = parent_id
        if level:
            extra_params["level"] = level
        if page is not None:
            extra_params["page"] = page
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

        if all_pages:
            return self.fetch_all_results(
                "units",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            # When all_pages=False, we need to fetch only the first page with the specified page_size
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return self.fetch_single_result(
                "units", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    def get_unit(
        self,
        unit_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve metadata details for a specific administrative unit.

        Maps to: GET /units/{id}

        Args:
            unit_id: Administrative unit identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with unit metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(f"units/{unit_id}", params=params if params else None, headers=headers if headers else None)

    def search_units(
        self,
        name: str | None = None,
        level: list[int] | None = None,
        years: list[int] | None = None,
        kind: str | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Search for administrative units by name and optional filters.

        Maps to: GET /units/search

        Args:
            name: Optional substring to search in unit name.
            level: Optional list of administrative levels to filter by.
            years: Optional list of years to filter by.
            kind: Optional kind filter.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.

        Returns:
            List of unit metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if name:
            extra_params["name"] = name
        if level:
            extra_params["level"] = level
        if years:
            extra_params["year"] = years
        if kind:
            extra_params["kind"] = kind
        if page is not None:
            extra_params["page"] = page
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

        if all_pages:
            return self.fetch_all_results(
                "units/search",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return self.fetch_single_result(
                "units/search", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    def list_localities(
        self,
        parent_id: str,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
    ) -> list[dict[str, Any]]:
        """
        List all statistical localities, optionally filtered by parent.

        Maps to: GET /units/localities

        Args:
            parent_id: Parent unit ID (required).
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.

        Returns:
            List of locality metadata dictionaries.
        """
        extra_params: dict[str, Any] = {"parent-id": parent_id}
        if page is not None:
            extra_params["page"] = page
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

        if all_pages:
            return self.fetch_all_results(
                "units/localities",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return self.fetch_single_result(
                "units/localities", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    def get_locality(
        self,
        locality_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve metadata details for a specific statistical locality.

        Maps to: GET /units/localities/{id}

        Args:
            locality_id: Locality identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with locality metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            f"units/localities/{locality_id}", params=params if params else None, headers=headers if headers else None
        )

    def search_localities(
        self,
        name: str | None = None,
        years: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Search for statistical localities by name and optional filters.

        Maps to: GET /units/localities/search

        Args:
            name: Optional substring to search in locality name.
            years: Optional list of years to filter by.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.

        Returns:
            List of locality metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if name:
            extra_params["name"] = name
        if years:
            extra_params["year"] = years
        if page is not None:
            extra_params["page"] = page
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

        if all_pages:
            return self.fetch_all_results(
                "units/localities/search",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return self.fetch_single_result(
                "units/localities/search", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    def get_units_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve general metadata and version information for the /units endpoint.

        Maps to: GET /units/metadata

        Args:
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with endpoint metadata and versioning info.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            "units/metadata", params=params if params else None, headers=headers if headers else None
        )

    async def alist_units(
        self,
        parent_id: str | None = None,
        level: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Asynchronously list all administrative units, optionally filtered by level, parent, or name.

        Maps to: GET /units

        Args:
            parent_id: Optional parent unit ID.
            level: Optional list of administrative levels to filter by.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order, e.g. 'id', '-id', 'name', '-name'.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.

        Returns:
            List of unit metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if parent_id:
            extra_params["parent-id"] = parent_id
        if level:
            extra_params["level"] = level
        if page is not None:
            extra_params["page"] = page
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

        if all_pages:
            return await self.afetch_all_results(
                "units",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return await self.afetch_single_result(
                "units", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    async def aget_unit(
        self,
        unit_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve metadata details for a specific administrative unit.

        Maps to: GET /units/{id}

        Args:
            unit_id: Administrative unit identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with unit metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(f"units/{unit_id}", params=params if params else None, headers=headers if headers else None)

    async def asearch_units(
        self,
        name: str | None = None,
        level: list[int] | None = None,
        years: list[int] | None = None,
        kind: str | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Asynchronously search for administrative units by name and optional filters.

        Maps to: GET /units/search

        Args:
            name: Optional substring to search in unit name.
            level: Optional list of administrative levels to filter by.
            years: Optional list of years to filter by.
            kind: Optional kind filter.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.

        Returns:
            List of unit metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if name:
            extra_params["name"] = name
        if level:
            extra_params["level"] = level
        if years:
            extra_params["year"] = years
        if kind:
            extra_params["kind"] = kind
        if page is not None:
            extra_params["page"] = page
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

        if all_pages:
            return await self.afetch_all_results(
                "units/search",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return await self.afetch_single_result(
                "units/search", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    async def alist_localities(
        self,
        parent_id: str,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Asynchronously list all statistical localities, optionally filtered by parent.

        Maps to: GET /units/localities

        Args:
            parent_id: Parent unit ID (required).
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.

        Returns:
            List of locality metadata dictionaries.
        """
        extra_params: dict[str, Any] = {"parent-id": parent_id}
        if page is not None:
            extra_params["page"] = page
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

        if all_pages:
            return await self.afetch_all_results(
                "units/localities",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return await self.afetch_single_result(
                "units/localities", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    async def aget_locality(
        self,
        locality_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve metadata details for a specific statistical locality.

        Maps to: GET /units/localities/{id}

        Args:
            locality_id: Locality identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with locality metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            f"units/localities/{locality_id}", params=params if params else None, headers=headers if headers else None
        )

    async def asearch_localities(
        self,
        name: str | None = None,
        years: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Asynchronously search for statistical localities by name and optional filters.

        Maps to: GET /units/localities/search

        Args:
            name: Optional substring to search in locality name.
            years: Optional list of years to filter by.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.

        Returns:
            List of locality metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if name:
            extra_params["name"] = name
        if years:
            extra_params["year"] = years
        if page is not None:
            extra_params["page"] = page
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

        if all_pages:
            return await self.afetch_all_results(
                "units/localities/search",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            return await self.afetch_single_result(
                "units/localities/search", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    async def aget_units_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve general metadata and version information for the /units endpoint.

        Maps to: GET /units/metadata

        Args:
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with endpoint metadata and versioning info.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            "units/metadata", params=params if params else None, headers=headers if headers else None
        )
