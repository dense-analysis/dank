"""Wires the viewer to its adapters. Tests override the store here."""

from typing import Annotated

from fastapi import Depends

from dank.api.deps import ResourcesDep, SettingsDep
from dank.api.posts.deps import clock, search_embeddings
from dank.api.posts.service import Clock, SearchEmbeddings
from dank.api.viewer.clickhouse import ClickHouseViewerStore
from dank.api.viewer.pages import ViewerPages
from dank.api.viewer.service import ViewerService, ViewerStore
from dank.web.app import assets_path


def viewer_store(
    resources: ResourcesDep,
    embeddings: Annotated[SearchEmbeddings, Depends(search_embeddings)],
) -> ViewerStore:
    return ClickHouseViewerStore(resources.clickhouse, embeddings)


def viewer_service(
    store: Annotated[ViewerStore, Depends(viewer_store)],
    now: Annotated[Clock, Depends(clock)],
) -> ViewerService:
    return ViewerService(store, now)


def viewer_pages(settings: SettingsDep) -> ViewerPages:
    return ViewerPages(assets_path(settings))


def page_size(resources: ResourcesDep) -> int:
    return resources.page_size


ViewerServiceDep = Annotated[ViewerService, Depends(viewer_service)]
ViewerPagesDep = Annotated[ViewerPages, Depends(viewer_pages)]
PageSizeDep = Annotated[int, Depends(page_size)]
