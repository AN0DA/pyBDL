from typing import Any

from pyldb.api.client import (
    FormatLiteral,
    LanguageLiteral,
    BaseAPIClient,
)


class VariablesAPI(BaseAPIClient):
    """
    Client for the LDB /variables endpoints.

    Provides access to variable metadata in the Local Data Bank (LDB),
    including listing variables (with filtering by category or aggregate), retrieving
    variable details, and accessing general variables API metadata.
    """

    def list_variables(
        self,
        subject_id: str | None = None,
        level: int | None = None,
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
        List all variables, optionally filtered by subject, level, or year.

        Maps to: GET /variables

        Args:
            subject_id: Optional subject ID to filter variables.
            level: Optional level to filter variables.
            years: Optional list of years to filter variables.
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
            List of variable metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if subject_id:
            extra_params["subject-id"] = subject_id
        if level is not None:
            extra_params["level"] = level
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
                "variables",
                params=params if params else None,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy() if params else {}
            params_with_page_size["page-size"] = page_size
            return self.fetch_single_result(
                "variables", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    def get_variable(
        self,
        variable_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve metadata details for a specific variable.

        Maps to: GET /variables/{id}

        Args:
            variable_id: Variable identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with variable metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            f"variables/{variable_id}", params=params if params else None, headers=headers if headers else None
        )

    def search_variables(
        self,
        name: str | None = None,
        subject_id: str | None = None,
        level: int | None = None,
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
        Search for variables by name and optional filters.

        Maps to: GET /variables/search

        Args:
            name: Optional substring to search in variable name.
            subject_id: Optional subject ID to filter variables.
            level: Optional level to filter variables.
            years: Optional list of years to filter variables.
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
            List of variable metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if name:
            extra_params["name"] = name
        if subject_id:
            extra_params["subject-id"] = subject_id
        if level is not None:
            extra_params["level"] = level
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
                "variables/search",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy() if params else {}
            params_with_page_size["page-size"] = page_size
            return self.fetch_single_result(
                "variables/search", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    def get_variables_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve general metadata and version information for the /variables endpoint.

        Maps to: GET /variables/metadata

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
            "variables/metadata", params=params if params else None, headers=headers if headers else None
        )

    async def alist_variables(
        self,
        subject_id: str | None = None,
        level: int | None = None,
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
        Asynchronously list all variables, optionally filtered by subject, level, or year.

        Maps to: GET /variables

        Args:
            subject_id: Optional subject ID to filter variables.
            level: Optional level to filter variables.
            years: Optional list of years to filter variables.
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
            List of variable metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if subject_id:
            extra_params["subject-id"] = subject_id
        if level is not None:
            extra_params["level"] = level
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
                "variables",
                params=params if params else None,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy() if params else {}
            params_with_page_size["page-size"] = page_size
            return await self.afetch_single_result(
                "variables", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    async def aget_variable(
        self,
        variable_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve metadata details for a specific variable.

        Maps to: GET /variables/{id}

        Args:
            variable_id: Variable identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with variable metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            f"variables/{variable_id}", params=params if params else None, headers=headers if headers else None
        )

    async def asearch_variables(
        self,
        name: str | None = None,
        subject_id: str | None = None,
        level: int | None = None,
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
        Asynchronously search for variables by name and optional filters.

        Maps to: GET /variables/search

        Args:
            name: Optional substring to search in variable name.
            subject_id: Optional subject ID to filter variables.
            level: Optional level to filter variables.
            years: Optional list of years to filter variables.
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
            List of variable metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if name:
            extra_params["name"] = name
        if subject_id:
            extra_params["subject-id"] = subject_id
        if level is not None:
            extra_params["level"] = level
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
                "variables/search",
                params=params,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        else:
            params_with_page_size = params.copy() if params else {}
            params_with_page_size["page-size"] = page_size
            return await self.afetch_single_result(
                "variables/search", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )

    async def aget_variables_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve general metadata and version information for the /variables endpoint.

        Maps to: GET /variables/metadata

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
            "variables/metadata", params=params if params else None, headers=headers if headers else None
        )
