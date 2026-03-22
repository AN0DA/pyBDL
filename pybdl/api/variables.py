from typing import Any

from pybdl.api.client import BaseAPIClient, FormatLiteral, LanguageLiteral


class VariablesAPI(BaseAPIClient):
    """Client for the BDL `/variables` endpoints."""

    @staticmethod
    def _list_params(
        subject_id: str | None,
        level: int | None,
        years: list[int] | None,
        page: int | None,
        sort: str | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if subject_id:
            params["subject-id"] = subject_id
        if level is not None:
            params["level"] = level
        if years:
            params["year"] = years
        if page is not None:
            params["page"] = page
        if sort:
            params["sort"] = sort
        if extra_query:
            params.update(extra_query)
        return params

    @staticmethod
    def _search_params(
        name: str | None,
        subject_id: str | None,
        level: int | None,
        years: list[int] | None,
        page: int | None,
        sort: str | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params = VariablesAPI._list_params(subject_id, level, years, page, sort, extra_query)
        if name:
            params["name"] = name
        return params

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
    ) -> list[dict[str, Any]]:
        return self._fetch_collection_endpoint(
            "variables",
            extra_params=self._list_params(subject_id, level, years, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
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
        return self._fetch_detail_endpoint(
            f"variables/{variable_id}",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
    ) -> list[dict[str, Any]]:
        return self._fetch_collection_endpoint(
            "variables/search",
            extra_params=self._search_params(name, subject_id, level, years, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
        )

    def get_variables_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._fetch_detail_endpoint(
            "variables/metadata",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
    ) -> list[dict[str, Any]]:
        return await self._afetch_collection_endpoint(
            "variables",
            extra_params=self._list_params(subject_id, level, years, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
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
        return await self._afetch_detail_endpoint(
            f"variables/{variable_id}",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
    ) -> list[dict[str, Any]]:
        return await self._afetch_collection_endpoint(
            "variables/search",
            extra_params=self._search_params(name, subject_id, level, years, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
        )

    async def aget_variables_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._afetch_detail_endpoint(
            "variables/metadata",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
