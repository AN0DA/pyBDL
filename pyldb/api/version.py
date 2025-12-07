from typing import Any, Literal

from pyldb.api.client import (
    LanguageLiteral,
    BaseAPIClient,
)


class VersionAPI(BaseAPIClient):
    """
    Client for the LDB /version endpoint.

    Provides access to version and build information for the Local Data Bank (LDB) API.
    """

    def get_version(
        self,
        lang: LanguageLiteral | None = None,
        format: Literal["json", "xml"] | None = None, # /version endpoint doesn't support jsonapi
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve the API version and build information.

        Maps to: GET /version

        Args:
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format). Note: only "json" and "xml" are supported for this endpoint.
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with version and build metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            "version", params=params if params else None, headers=headers if headers else None
        )

    async def aget_version(
        self,
        lang: LanguageLiteral | None = None,
        format: Literal["json", "xml"] | None = None, # /version endpoint doesn't support jsonapi
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve the API version and build information.

        Maps to: GET /version

        Args:
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format). Note: only "json" and "xml" are supported for this endpoint.
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            Dictionary with version and build metadata.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            "version", params=params if params else None, headers=headers if headers else None
        )
