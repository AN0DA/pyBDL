from typing import Any

from pyldb.api.client import (
    BaseAPIClient,
    FormatLiteral,
    LanguageLiteral,
)


class AttributesAPI(BaseAPIClient):
    """
    Client for the LDB /attributes endpoints.

    Provides paginated, filterable access to attribute metadata, attribute details,
    and API metadata for attributes in the Local Data Bank (LDB).
    """

    def list_attributes(
        self,
        sort: str | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        List all attributes, optionally filtered by variable.

        Maps to: GET /attributes

        Args:
            sort: Optional sorting order.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of attribute metadata dictionaries.
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
            "attributes", params=params, headers=headers if headers else None, page_size=page_size, max_pages=max_pages
        )

    def get_attribute(
        self,
        attribute_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve metadata details for a specific attribute.

        Maps to: GET /attributes/{id}

        Args:
            attribute_id: Attribute identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with attribute metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            f"attributes/{attribute_id}", params=params if params else None, headers=headers if headers else None
        )

    def get_attributes_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve general metadata and version information for the /attributes endpoint.

        Maps to: GET /attributes/metadata

        Args:
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
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
            "attributes/metadata", params=params if params else None, headers=headers if headers else None
        )

    async def alist_attributes(
        self,
        sort: str | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Asynchronously list all attributes, optionally filtered by variable.

        Maps to: GET /attributes

        Args:
            sort: Optional sorting order.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            List of attribute metadata dictionaries.
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
            "attributes", params=params, headers=headers if headers else None, page_size=page_size, max_pages=max_pages
        )

    async def aget_attribute(
        self,
        attribute_id: str,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve metadata details for a specific attribute.

        Maps to: GET /attributes/{id}

        Args:
            attribute_id: Attribute identifier.
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with attribute metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            f"attributes/{attribute_id}", params=params if params else None, headers=headers if headers else None
        )

    async def aget_attributes_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve general metadata and version information for the /attributes endpoint.

        Maps to: GET /attributes/metadata

        Args:
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
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
            "attributes/metadata", params=params if params else None, headers=headers if headers else None
        )
