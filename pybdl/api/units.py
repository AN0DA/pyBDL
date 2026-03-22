from typing import Any

from pybdl.api.client import BaseAPIClient, FormatLiteral, LanguageLiteral


class UnitsAPI(BaseAPIClient):
    """Client for the BDL `/units` endpoints."""

    @staticmethod
    def _list_units_params(
        parent_id: str | None,
        level: list[int] | None,
        page: int | None,
        sort: str | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if parent_id:
            params["parent-id"] = parent_id
        if level:
            params["level"] = level
        if page is not None:
            params["page"] = page
        if sort:
            params["sort"] = sort
        if extra_query:
            params.update(extra_query)
        return params

    @staticmethod
    def _search_units_params(
        name: str | None,
        level: list[int] | None,
        years: list[int] | None,
        kind: str | None,
        page: int | None,
        sort: str | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if name:
            params["name"] = name
        if level:
            params["level"] = level
        if years:
            params["year"] = years
        if kind:
            params["kind"] = kind
        if page is not None:
            params["page"] = page
        if sort:
            params["sort"] = sort
        if extra_query:
            params.update(extra_query)
        return params

    @staticmethod
    def _list_localities_params(
        parent_id: str,
        page: int | None,
        sort: str | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"parent-id": parent_id}
        if page is not None:
            params["page"] = page
        if sort:
            params["sort"] = sort
        if extra_query:
            params.update(extra_query)
        return params

    @staticmethod
    def _search_localities_params(
        name: str | None,
        years: list[int] | None,
        page: int | None,
        sort: str | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if name:
            params["name"] = name
        if years:
            params["year"] = years
        if page is not None:
            params["page"] = page
        if sort:
            params["sort"] = sort
        if extra_query:
            params.update(extra_query)
        return params

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
    ) -> list[dict[str, Any]]:
        return self._fetch_collection_endpoint(
            "units",
            extra_params=self._list_units_params(parent_id, level, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
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
        return self._fetch_detail_endpoint(
            f"units/{unit_id}",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )

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
    ) -> list[dict[str, Any]]:
        return self._fetch_collection_endpoint(
            "units/search",
            extra_params=self._search_units_params(name, level, years, kind, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
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
    ) -> list[dict[str, Any]]:
        return self._fetch_collection_endpoint(
            "units/localities",
            extra_params=self._list_localities_params(parent_id, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
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
        return self._fetch_detail_endpoint(
            f"units/localities/{locality_id}",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
    ) -> list[dict[str, Any]]:
        return self._fetch_collection_endpoint(
            "units/localities/search",
            extra_params=self._search_localities_params(name, years, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
        )

    def get_units_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._fetch_detail_endpoint(
            "units/metadata",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
    ) -> list[dict[str, Any]]:
        return await self._afetch_collection_endpoint(
            "units",
            extra_params=self._list_units_params(parent_id, level, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
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
        return await self._afetch_detail_endpoint(
            f"units/{unit_id}",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )

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
    ) -> list[dict[str, Any]]:
        return await self._afetch_collection_endpoint(
            "units/search",
            extra_params=self._search_units_params(name, level, years, kind, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
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
    ) -> list[dict[str, Any]]:
        return await self._afetch_collection_endpoint(
            "units/localities",
            extra_params=self._list_localities_params(parent_id, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
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
        return await self._afetch_detail_endpoint(
            f"units/localities/{locality_id}",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
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
    ) -> list[dict[str, Any]]:
        return await self._afetch_collection_endpoint(
            "units/localities/search",
            extra_params=self._search_localities_params(name, years, page, sort, extra_query),
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
        )

    async def aget_units_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._afetch_detail_endpoint(
            "units/metadata",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
