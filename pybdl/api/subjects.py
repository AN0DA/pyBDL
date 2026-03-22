from typing import Any

from pybdl.api.client import BaseAPIClient, FormatLiteral, LanguageLiteral


class SubjectsAPI(BaseAPIClient):
    """Client for the BDL `/subjects` endpoints."""

    @staticmethod
    def _list_params(
        parent_id: str | None,
        sort: str | None,
        page: int | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if parent_id:
            params["parent-id"] = parent_id
        if sort:
            params["sort"] = sort
        if page is not None:
            params["page"] = page
        if extra_query:
            params.update(extra_query)
        return params

    @staticmethod
    def _search_params(
        name: str,
        page: int | None,
        sort: str | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"name": name}
        if page is not None:
            params["page"] = page
        if sort:
            params["sort"] = sort
        if extra_query:
            params.update(extra_query)
        return params

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
        return self._fetch_collection_endpoint(
            "subjects",
            extra_params=self._list_params(parent_id, sort, page, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
        return self._fetch_detail_endpoint(
            f"subjects/{subject_id}",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
        return self._fetch_collection_endpoint(
            "subjects/search",
            extra_params=self._search_params(name, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
        return self._fetch_detail_endpoint(
            "subjects/metadata",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
        return await self._afetch_collection_endpoint(
            "subjects",
            extra_params=self._list_params(parent_id, sort, page, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
        return await self._afetch_detail_endpoint(
            f"subjects/{subject_id}",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
        return await self._afetch_collection_endpoint(
            "subjects/search",
            extra_params=self._search_params(name, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
        return await self._afetch_detail_endpoint(
            "subjects/metadata",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
