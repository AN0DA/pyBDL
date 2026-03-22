"""Unit tests for enrichment decorator and helpers."""

from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pybdl.access.base import BaseAccess
from pybdl.access.enrichment import EnrichmentSpec, with_enrichment


class DummyAccess(BaseAccess):
    """Concrete access for enrichment tests."""

    pass


def _base_df() -> pd.DataFrame:
    return pd.DataFrame([{"id": 1, "value": 10}])


@pytest.mark.unit
def test_enrichment_merges_and_renames() -> None:
    """Ensure enrichment merges lookup data and applies rename map."""
    spec = EnrichmentSpec(
        flag="enrich_dummy",
        id_column="id",
        cache_key="dummy",
        sync_loader=lambda self: pd.DataFrame([{"id": 1, "name": "one"}]),
        async_loader=AsyncMock(return_value=pd.DataFrame([{"id": 1, "name": "one"}])),
        rename_map={"name": "dummy_name"},
    )

    @with_enrichment(spec)
    def fetch(self: BaseAccess) -> pd.DataFrame:
        return _base_df()

    access = DummyAccess(MagicMock())
    df = fetch(access, enrich_dummy=True)

    assert "dummy_name" in df.columns
    assert df.loc[0, "dummy_name"] == "one"


@pytest.mark.unit
def test_enrichment_preserves_metadata_tuple() -> None:
    """Tuple results keep metadata untouched."""
    spec = EnrichmentSpec(
        flag="enrich_dummy",
        id_column="id",
        cache_key="dummy",
        sync_loader=lambda self: pd.DataFrame([{"id": 1, "name": "one"}]),
        async_loader=AsyncMock(return_value=pd.DataFrame([{"id": 1, "name": "one"}])),
        rename_map={"name": "dummy_name"},
    )

    @with_enrichment(spec)
    def fetch(self: BaseAccess) -> tuple[pd.DataFrame, dict[str, Any]]:
        return _base_df(), {"meta": 123}

    access = DummyAccess(MagicMock())
    df, meta = fetch(access, enrich_dummy=True)

    assert "dummy_name" in df.columns
    assert meta == {"meta": 123}


@pytest.mark.unit
def test_enrichment_uses_cache_once() -> None:
    """Loader is called once per access instance due to caching."""
    loader = MagicMock(return_value=pd.DataFrame([{"id": 1, "name": "cached"}]))
    spec = EnrichmentSpec(
        flag="enrich_dummy",
        id_column="id",
        cache_key="dummy",
        sync_loader=lambda self: loader(self),
        async_loader=AsyncMock(return_value=pd.DataFrame([{"id": 1, "name": "cached"}])),
        rename_map={"name": "dummy_name"},
    )

    @with_enrichment(spec)
    def fetch(self: BaseAccess) -> pd.DataFrame:
        return _base_df()

    access = DummyAccess(MagicMock())
    fetch(access, enrich_dummy=True)
    fetch(access, enrich_dummy=True)

    loader.assert_called_once()


@pytest.mark.unit
def test_signature_injected_keyword_flag() -> None:
    """Decorator injects keyword-only enrich flag into signature."""
    spec = EnrichmentSpec(
        flag="enrich_dummy",
        id_column="id",
        cache_key="dummy",
        sync_loader=lambda self: pd.DataFrame(),
        async_loader=AsyncMock(return_value=pd.DataFrame()),
    )

    @with_enrichment(spec)
    def fetch(self: BaseAccess, some_arg: int) -> pd.DataFrame:
        return _base_df()

    sig = inspect.signature(fetch)
    assert "enrich_dummy" in sig.parameters
    param = sig.parameters["enrich_dummy"]
    assert param.default is False
    assert param.kind == inspect.Parameter.KEYWORD_ONLY


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_enrichment_path() -> None:
    """Async wrapper awaits async loader and merges data."""
    async_loader = AsyncMock(return_value=pd.DataFrame([{"id": 1, "name": "async"}]))
    spec = EnrichmentSpec(
        flag="enrich_dummy",
        id_column="id",
        cache_key="dummy",
        sync_loader=lambda self: pd.DataFrame([{"id": 1, "name": "sync"}]),
        async_loader=lambda self: async_loader(self),
        rename_map={"name": "dummy_name"},
    )

    @with_enrichment(spec)
    async def afetch(self: BaseAccess) -> pd.DataFrame:
        return _base_df()

    access = DummyAccess(MagicMock())
    df = await afetch(access, enrich_dummy=True)

    async_loader.assert_awaited_once()
    assert "dummy_name" in df.columns
    assert df.loc[0, "dummy_name"] == "async"
