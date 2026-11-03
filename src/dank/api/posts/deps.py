"""Wires PostService to its adapters. Tests override the adapters here."""

import datetime as dt
from typing import Annotated

from fastapi import Depends

from dank.api.deps import ResourcesDep
from dank.api.posts.clickhouse import ClickHousePostStore
from dank.api.posts.embeddings import CachedSearchEmbeddings
from dank.api.posts.service import (
    Clock,
    PostService,
    PostStore,
    SearchEmbeddings,
)
from dank.api.sources.deps import source_catalog
from dank.api.sources.service import SourceCatalog
from dank.embeddings import get_embedding_model


def post_store(resources: ResourcesDep) -> PostStore:
    return ClickHousePostStore(resources.clickhouse, resources.assets_dir)


def search_embeddings(resources: ResourcesDep) -> SearchEmbeddings:
    return CachedSearchEmbeddings(resources.clickhouse, get_embedding_model)


def clock() -> Clock:
    return lambda: dt.datetime.now(dt.UTC)


def post_service(
    catalog: Annotated[SourceCatalog, Depends(source_catalog)],
    store: Annotated[PostStore, Depends(post_store)],
    embeddings: Annotated[SearchEmbeddings, Depends(search_embeddings)],
    now: Annotated[Clock, Depends(clock)],
) -> PostService:
    return PostService(catalog, store, embeddings, now)


PostServiceDep = Annotated[PostService, Depends(post_service)]
