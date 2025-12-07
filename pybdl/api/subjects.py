from typing import Any

from pybdl.api.client import (
    BaseAPIClient,
    FormatLiteral,
    LanguageLiteral,
)


class SubjectsAPI(BaseAPIClient):
    """
    Client for the BDL /subjects endpoints.

    Provides access to the subject hierarchy (thematic areas) in the Local Data Bank (BDL),
    including subject browsing, detail retrieval, and metadata.
    """

    def list_subjects(
        self,
        parent_id: str | None = None,
        sort: str | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        List all subjects, optionally filtered by parent subject.

        Maps to: GET /subjects

        Args:
            parent_id: Optional parent subject ID. If not specified, returns all top-level subjects.
            sort: Optional sorting order, e.g. 'id', '-id', 'name', '-name'.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all pages, 1 for single page).
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of subject metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if parent_id:
            extra_params["parent-id"] = parent_id
        if sort:
            extra_params["sort"] = sort
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        if max_pages == 1:
            # Fetch only the first page
            params_with_page_size = params.copy() if params else {}
            params_with_page_size["page-size"] = page_size
            return self.fetch_single_result(
                "subjects", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )
        else:
            # Fetch all pages (max_pages=None) or up to max_pages
            return self.fetch_all_results(
                "subjects",
                params=params if params else None,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )

    def get_subject(
        self,
        subject_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve metadata for a specific subject.

        Maps to: GET /subjects/{id}

        Args:
            subject_id: Subject identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with subject metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            f"subjects/{subject_id}", params=params if params else None, headers=headers if headers else None
        )

    def search_subjects(
        self,
        name: str,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search for subjects by name.

        Maps to: GET /subjects/search

        Args:
            name: Subject name to search for (required).
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order, e.g. 'id', '-id', 'name', '-name'.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of subject metadata dictionaries matching the search.
        """
        extra_params: dict[str, Any] = {"name": name}
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

        return self.fetch_all_results(
            "subjects/search",
            params=params,
            headers=headers if headers else None,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
        )

    def get_subjects_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve general metadata and version information for the /subjects endpoint.

        Maps to: GET /subjects/metadata

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
            "subjects/metadata", params=params if params else None, headers=headers if headers else None
        )

    async def alist_subjects(
        self,
        parent_id: str | None = None,
        sort: str | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Asynchronously list all subjects, optionally filtered by parent subject.

        Maps to: GET /subjects

        Args:
            parent_id: Optional parent subject ID. If not specified, returns all top-level subjects.
            sort: Optional sorting order, e.g. 'id', '-id', 'name', '-name'.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all pages, 1 for single page).
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of subject metadata dictionaries.
        """
        extra_params: dict[str, Any] = {}
        if parent_id:
            extra_params["parent-id"] = parent_id
        if sort:
            extra_params["sort"] = sort
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        if max_pages == 1:
            # Fetch only the first page
            params_with_page_size = params.copy() if params else {}
            params_with_page_size["page-size"] = page_size
            return await self.afetch_single_result(
                "subjects", results_key="results", params=params_with_page_size, headers=headers if headers else None
            )
        else:
            # Fetch all pages (max_pages=None) or up to max_pages
            return await self.afetch_all_results(
                "subjects",
                params=params if params else None,
                headers=headers if headers else None,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )

    async def aget_subject(
        self,
        subject_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve metadata for a specific subject.

        Maps to: GET /subjects/{id}

        Args:
            subject_id: Subject identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with subject metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            f"subjects/{subject_id}", params=params if params else None, headers=headers if headers else None
        )

    async def asearch_subjects(
        self,
        name: str,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        sort: str | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Asynchronously search for subjects by name.

        Maps to: GET /subjects/search

        Args:
            name: Subject name to search for (required).
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            sort: Optional sorting order, e.g. 'id', '-id', 'name', '-name'.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of subject metadata dictionaries matching the search.
        """
        extra_params: dict[str, Any] = {"name": name}
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

        return await self.afetch_all_results(
            "subjects/search",
            params=params,
            headers=headers if headers else None,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
        )

    async def aget_subjects_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve general metadata and version information for the /subjects endpoint.

        Maps to: GET /subjects/metadata

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
            "subjects/metadata", params=params if params else None, headers=headers if headers else None
        )
