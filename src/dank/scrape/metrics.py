from __future__ import annotations

import asyncio
import datetime
import logging
import time
import uuid
from collections import Counter
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import NamedTuple

from dank.config import SourceConfig
from dank.runtime import RuntimeInfo

logger = logging.getLogger(__name__)

RSS_CONCURRENCY = 4
MEDIA_CONCURRENCY = 4
COUNTERS = (
    "posts_queued", "posts_saved", "media_references",
    "asset_records_saved", "files_downloaded", "files_cached",
    "files_failed", "media_skipped", "downloaded_file_bytes",
    "http_requests", "http_429", "retries", "fetch_failures",
    "parse_failures", "request_ms", "retry_wait_ms",
)


class ScrapeOptions(NamedTuple):
    keep_feed_on_fetch_failure: int = 0
    max_asset_bytes: int | None = None
    feed_staleness_days: int = 14
    x_max_posts: int = 200
    x_max_scrolls: int = 20
    x_scroll_pause_seconds: float = 1.5
    max_entries_per_feed: int = 0


class SourceMetrics:
    def __init__(self, index: int, source: SourceConfig) -> None:
        self.index = index
        self.source = source
        self.feed_urls = source.feed_urls
        self.started_at = datetime.datetime.now(datetime.UTC)
        self.started = time.monotonic()
        self.finished_at: datetime.datetime | None = None
        self.elapsed_ms: int | None = None
        self.collection_ms: int | None = None
        self.counts: Counter[str] = Counter()
        self.pending = 0
        self.active_http = 0
        self.active_media = 0
        self.waiting_retries = 0
        self.discovery_done = False
        self.skipped = False
        self.status = "running"
        self.error_type = ""

    def discovered(self, posts: int, assets: int) -> None:
        self.counts["posts_queued"] += posts
        self.counts["media_references"] += assets
        self.pending += posts + assets

    def collected(self) -> None:
        self.collection_ms = elapsed_ms(self.started)
        self.discovery_done = True
        self.maybe_finish()

    def persisted(self, count: int) -> None:
        self.pending -= count
        self.maybe_finish()

    def maybe_finish(self) -> None:
        if (
            self.finished_at is not None
            or not self.discovery_done or self.pending
        ):
            return

        failures = sum(self.counts[name] for name in (
            "fetch_failures", "parse_failures", "files_failed",
        ))

        if failures:
            status = "partial" if (
                self.counts["posts_saved"]
                or self.counts["asset_records_saved"]
            ) else "failed"
        else:
            status = "skipped" if self.skipped else "completed"

        self.finish(status)

    def finish(self, status: str, error_type: str = "") -> None:
        if self.finished_at is not None:
            return

        self.finished_at = datetime.datetime.now(datetime.UTC)
        self.elapsed_ms = elapsed_ms(self.started)
        self.status = status
        self.error_type = error_type or self.error_type
        logger.info(
            "%s: %s in %.1fs; collection=%.1fs; posts=%d; "
            "files=%d new/%d cached/%d failed; retries=%d",
            self.source.domain, status, self.elapsed_ms / 1000,
            (self.collection_ms or 0) / 1000, self.counts["posts_saved"],
            self.counts["files_downloaded"], self.counts["files_cached"],
            self.counts["files_failed"], self.counts["retries"],
        )

    def row(self, run_id: uuid.UUID, version: int) -> dict[str, object]:
        return {
            "run_id": run_id, "source_index": self.index,
            "domain": self.source.domain,
            "source": "x" if self.source.domain == "x.com" else "rss",
            "accounts": list(self.source.accounts),
            "tags": list(self.source.tags),
            "feed_urls": list(self.feed_urls),
            "started_at": self.started_at, "finished_at": self.finished_at,
            "elapsed_ms": self.elapsed_ms, "collection_ms": self.collection_ms,
            "status": self.status, "error_type": self.error_type,
            "version": version,
            **{name: self.counts[name] for name in COUNTERS},
        }


class RunMetrics:
    def __init__(
        self, runtime: RuntimeInfo, source_count: int, batch_size: int,
        *, headless: bool, options: ScrapeOptions,
    ) -> None:
        self.run_id = uuid.uuid4()
        self.started_at = datetime.datetime.now(datetime.UTC)
        self.started = time.monotonic()
        self.finished_at: datetime.datetime | None = None
        self.elapsed_ms: int | None = None
        self.status = "running"
        self.error_type = ""
        self.runtime = runtime
        self.source_count = source_count
        self.batch_size = batch_size
        self.headless = headless
        self.options = options
        self.sources: dict[int, SourceMetrics] = {}
        self.queue_size = 0

    def counts(self) -> Counter[str]:
        result: Counter[str] = Counter()

        for source in self.sources.values():
            result.update(source.counts)

        return result

    def finish(self, error: BaseException | None = None) -> None:
        if error is not None:
            status = "cancelled" if isinstance(
                error, asyncio.CancelledError,
            ) else "failed"

            for source in self.sources.values():
                source.finish(status, type(error).__name__)

            self.status = status
            self.error_type = type(error).__name__
        else:
            outcomes = {source.status for source in self.sources.values()}

            if outcomes <= {"skipped"}:
                self.status = "skipped"
            elif outcomes & {"partial", "failed"}:
                counts = self.counts()
                saved = counts["posts_saved"] + counts["asset_records_saved"]
                self.status = "partial" if saved else "failed"
            else:
                self.status = "completed"

        self.finished_at = datetime.datetime.now(datetime.UTC)
        self.elapsed_ms = elapsed_ms(self.started)

    def row(self, version: int) -> dict[str, object]:
        counts = self.counts()

        return {
            "run_id": self.run_id, "started_at": self.started_at,
            "finished_at": self.finished_at, "elapsed_ms": self.elapsed_ms,
            "status": self.status, "error_type": self.error_type,
            "sources_selected": self.source_count,
            "sources_started": len(self.sources),
            "runtime": self.runtime.runtime, "platform": self.runtime.platform,
            "python_version": self.runtime.python_version,
            "cpu_limit": self.runtime.cpu_limit,
            "memory_limit_bytes": self.runtime.memory_limit_bytes,
            "code_version": self.runtime.code_version,
            "source_concurrency": 1, "rss_concurrency": RSS_CONCURRENCY,
            "media_concurrency": MEDIA_CONCURRENCY,
            "batch_size": self.batch_size, "headless": int(self.headless),
            "version": version,
            **self.options._asdict(),
            **{name: counts[name] for name in COUNTERS},
        }

    async def report_progress(self) -> None:
        while True:
            await asyncio.sleep(5)
            sources = list(self.sources.values())
            counts = self.counts()
            logger.info(
                "Run %s: elapsed=%.1fs; sources=%d/%d finished; "
                "HTTP=%d active; media=%d active; retries=%d waiting; "
                "queued batches=%d; pending records=%d; posts saved=%d",
                str(self.run_id)[:8], elapsed_ms(self.started) / 1000,
                sum(source.finished_at is not None for source in sources),
                self.source_count,
                sum(source.active_http for source in sources),
                sum(source.active_media for source in sources),
                sum(source.waiting_retries for source in sources),
                self.queue_size, sum(source.pending for source in sources),
                counts["posts_saved"],
            )


current_source: ContextVar[SourceMetrics | None] = ContextVar(
    "scrape_source_metrics", default=None,
)


def elapsed_ms(started: float) -> int:
    return max(0, round((time.monotonic() - started) * 1000))


def count(name: str, amount: int = 1) -> None:
    source = current_source.get()

    if source is not None:
        source.counts[name] += amount


@contextmanager
def request_metrics(*, retry: bool = False) -> Generator[None]:
    source = current_source.get()
    started = time.monotonic()

    if source is not None:
        source.active_http += 1
        source.counts["http_requests"] += 1
        source.counts["retries"] += int(retry)

    try:
        yield
    finally:
        if source is not None:
            source.active_http -= 1
            source.counts["request_ms"] += elapsed_ms(started)


async def retry_sleep(delay: float) -> None:
    source = current_source.get()
    started = time.monotonic()

    if source is not None:
        source.waiting_retries += 1

    try:
        await asyncio.sleep(delay)
    finally:
        if source is not None:
            source.waiting_retries -= 1
            source.counts["retry_wait_ms"] += elapsed_ms(started)
