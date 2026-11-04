"""Wires the YouTube services to their adapters. Tests override them here."""

from typing import Annotated

from fastapi import Depends

from dank.api.deps import ResourcesDep
from dank.api.posts.deps import clock
from dank.api.posts.service import Clock
from dank.api.sources.deps import source_catalog
from dank.api.sources.service import SourceCatalog
from dank.api.youtube.clickhouse import ClickHouseYouTubeStore
from dank.api.youtube.collector import COLLECTION_LIMITS, ScrapeVideoCollector
from dank.api.youtube.service import (
    CollectionLimits,
    CollectionLocks,
    VideoCaptureService,
    VideoCollector,
    YouTubeService,
    YouTubeStore,
)


def youtube_store(resources: ResourcesDep) -> YouTubeStore:
    return ClickHouseYouTubeStore(
        resources.clickhouse, resources.settings, resources.assets_dir,
    )


def video_collector(resources: ResourcesDep) -> VideoCollector:
    return ScrapeVideoCollector(
        resources.clickhouse,
        metadata=resources.metadata_gate.limiter,
        transcript=resources.transcript_gate.limiter,
    )


def collection_locks(resources: ResourcesDep) -> CollectionLocks:
    return CollectionLocks(
        metadata=resources.metadata_gate.lock,
        transcript=resources.transcript_gate.lock,
    )


def collection_limits() -> CollectionLimits:
    return COLLECTION_LIMITS


def youtube_service(
    catalog: Annotated[SourceCatalog, Depends(source_catalog)],
    store: Annotated[YouTubeStore, Depends(youtube_store)],
    now: Annotated[Clock, Depends(clock)],
) -> YouTubeService:
    return YouTubeService(catalog, store, now)


def video_capture_service(
    store: Annotated[YouTubeStore, Depends(youtube_store)],
    collector: Annotated[VideoCollector, Depends(video_collector)],
    locks: Annotated[CollectionLocks, Depends(collection_locks)],
    limits: Annotated[CollectionLimits, Depends(collection_limits)],
) -> VideoCaptureService:
    return VideoCaptureService(store, collector, locks, limits)


YouTubeServiceDep = Annotated[YouTubeService, Depends(youtube_service)]
VideoCaptureServiceDep = Annotated[
    VideoCaptureService, Depends(video_capture_service),
]
