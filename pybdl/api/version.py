from typing import Any, Literal

from pybdl.api.client import BaseAPIClient, LanguageLiteral


class VersionAPI(BaseAPIClient):
    """Client for the BDL `/version` endpoint."""

    def get_version(
        self,
        lang: LanguageLiteral | None = None,
        format: Literal["json", "xml"] | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._fetch_detail_endpoint(
            "version",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )

    async def aget_version(
        self,
        lang: LanguageLiteral | None = None,
        format: Literal["json", "xml"] | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._afetch_detail_endpoint(
            "version",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
