"""Compare homepage listing collectors without persisting their discoveries."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from zendriver import Browser, Tab, cdp

from dank.config import Settings
from dank.scrape.types import ScrapeBatch
from dank.scrape.youtube import (
    YouTubeFeedError,
    YouTubeFeedIncomplete,
    _collect_home_feed,  # pyright: ignore[reportPrivateUsage]
    _localized,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.zendriver import BrowserConfig, BrowserSession
from dank.youtube_session import save_youtube_session_from_page

from .browser import navigate
from .payloads import ORIGIN, YouTubeFeed, as_dict
from .ytdlp_home import collect_ytdlp_home

MAX_COMPARISON_SECONDS = 240.0
WARMUP_TIMEOUT_SECONDS = 60.0
LIMITATIONS = (
    "One paired run is descriptive: personalized recommendations can change "
    "between requests even with the same starting cookies.",
    "Browser results exclude news shelves, Playables and live content; flat "
    "yt-dlp results cannot reliably identify or exclude those shelves.",
    "Video overlap combines videos and Shorts by video ID. Community posts "
    "and unidentified entries are reported separately.",
    "A missing shelf label is not evidence that an item came from no shelf. "
    "Flat yt-dlp metadata is not the original YouTube renderer.",
    "Timing covers listing collection, with browser/session warmup separate; "
    "there are no media downloads, caption requests or library writes.",
)


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def _coverage(entry: Mapping[str, Any], method: str) -> dict[str, bool]:
    raw = as_dict(entry.get("raw"))

    if method == "browser":
        return {
            "title": bool(raw.get("title")),
            "channel": bool(raw.get("channel_name") or raw.get("channel_id")),
            "thumbnail": bool(raw.get("thumbnail_url")),
            "shelf_label": bool(as_dict(raw.get("discovery")).get("shelf")),
            "source_renderer": bool(raw.get("renderer")),
            "post_text": entry.get("kind") == "post" and bool(raw.get("text")),
        }

    return {
        "title": bool(raw.get("title")),
        "channel": bool(
            raw.get("channel") or raw.get("channel_id") or raw.get("uploader"),
        ),
        "thumbnail": bool(raw.get("thumbnail") or raw.get("thumbnails")),
        "shelf_label": False,
        "source_renderer": False,
        "post_text": entry.get("kind") == "post" and bool(raw.get("text")),
    }


def summarize_entries(
    entries: Sequence[Mapping[str, Any]],
    *,
    method: str,
) -> dict[str, Any]:
    """Count identities once, retaining coverage from all their appearances."""
    identities: dict[str, list[Mapping[str, Any]]] = {}
    unidentified = 0

    for entry in entries:
        identity = str(entry.get("identity") or "")

        if not identity:
            unidentified += 1
            continue

        identities.setdefault(identity, []).append(entry)

    kinds: dict[str, list[str]] = {"video": [], "short": [], "post": []}
    coverage = dict.fromkeys(
        (
            "title",
            "channel",
            "thumbnail",
            "shelf_label",
            "source_renderer",
            "post_text",
        ),
        0,
    )
    conflicts: list[str] = []

    for identity, appearances in identities.items():
        observed_kinds = {entry.get("kind") for entry in appearances}

        if len(observed_kinds) > 1:
            conflicts.append(identity)

        # A video observed as a Short remains one video identity in the union.
        kind = (
            "post"
            if identity.startswith("post:")
            else "short"
            if "short" in observed_kinds
            else "video"
        )
        kinds[kind].append(identity)
        available = [_coverage(entry, method) for entry in appearances]

        for field in coverage:
            coverage[field] += int(any(item[field] for item in available))

    return {
        "raw_entries": len(entries),
        "unique_items": len(identities),
        "unidentified_entries": unidentified,
        "duplicate_appearances": len(entries) - unidentified - len(identities),
        "counts": {kind: len(values) for kind, values in kinds.items()},
        "identities": sorted(identities),
        "video_identities": sorted(kinds["video"] + kinds["short"]),
        "kind_conflict_identities": sorted(conflicts),
        "metadata_coverage": {
            "denominator": len(identities),
            "available_counts": coverage,
        },
    }


def compare_entries(
    browser: Sequence[Mapping[str, Any]],
    ytdlp: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    browser_summary = summarize_entries(browser, method="browser")
    ytdlp_summary = summarize_entries(ytdlp, method="yt_dlp")
    left = set(browser_summary["video_identities"])
    right = set(ytdlp_summary["video_identities"])
    common = left & right
    union = left | right

    return {
        "browser": browser_summary,
        "yt_dlp": ytdlp_summary,
        "video_overlap": {
            "common_count": len(common),
            "union_count": len(union),
            "jaccard": len(common) / len(union) if union else None,
            "common": sorted(common),
            "browser_only": sorted(left - right),
            "yt_dlp_only": sorted(right - left),
            "browser_only_count": len(left - right),
            "yt_dlp_only_count": len(right - left),
        },
    }


class _ObservedBatches(list[ScrapeBatch]):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.get_running_loop().time()
        self.first_entry_seconds: float | None = None

    def append(self, batch: ScrapeBatch) -> None:
        if batch.posts and self.first_entry_seconds is None:
            self.first_entry_seconds = (
                asyncio.get_running_loop().time() - self.started
            )

        super().append(batch)


async def _browser_collection(
    page: Any,
    settings: Settings,
    timeout_seconds: float,
) -> dict[str, Any]:
    batches = _ObservedBatches()
    outcome, stop, error_type = "completed", "end of feed", ""

    try:
        await _collect_home_feed(
            page,
            YouTubeFeed(ORIGIN + "/", "Recommendations", "home"),
            settings.youtube._replace(
                authenticated=True,
                include_shorts=True,
                include_posts=True,
            ),
            batches,
            timeout_seconds=timeout_seconds,
            max_items=10000,
            max_scrolls=2000,
        )
    except YouTubeFeedIncomplete as error:
        stop = next(
            (
                reason
                for reason in (
                    "time budget",
                    "item limit",
                    "scroll limit",
                    "repeated stall",
                )
                if reason in str(error)
            ),
            "incomplete",
        )
        outcome = "timeout" if stop == "time budget" else "incomplete"
        error_type = type(error).__name__
    except Exception as error:
        outcome, stop, error_type = (
            "failed",
            "listing error",
            type(error).__name__,
        )

    entries: list[dict[str, Any]] = []

    for batch in batches:
        for post in batch.posts:
            raw = as_dict(json.loads(post.payload))
            entries.append(
                {
                    "identity": post.post_id,
                    "kind": raw.get("kind", "unknown"),
                    "position": len(entries) + 1,
                    "raw": raw,
                },
            )

    return {
        "outcome": outcome,
        "stop_reason": stop,
        "error_type": error_type,
        "partial": outcome != "completed",
        "entries": entries,
        "first_entry_seconds": batches.first_entry_seconds,
    }


async def _ytdlp_collection(
    cookies: Sequence[Mapping[str, object]],
    timeout_seconds: float,
    region: str,
) -> dict[str, Any]:
    result = await collect_ytdlp_home(
        cookies,
        timeout_seconds=timeout_seconds,
        region=region,
    )

    return {
        "outcome": result.outcome,
        "stop_reason": result.outcome,
        "partial": result.outcome != "completed",
        "error_type": result.error_type,
        "entries": [dict(entry._asdict()) for entry in result.entries],
        "first_entry_seconds": result.first_entry_seconds,
        "collector_elapsed_seconds": result.elapsed_seconds,
        "warning_count": len(result.warnings),
        "warnings": list(result.warnings),
        "limitations": list(result.limitations),
    }


async def _timed(
    ready: asyncio.Event,
    collect: Callable[[], Awaitable[dict[str, Any]]],
) -> dict[str, Any]:
    await ready.wait()
    started_at = _now()
    started = asyncio.get_running_loop().time()
    result: dict[str, Any]

    try:
        result = await collect()
    except Exception as error:
        # Transport exceptions may carry credentials; retain the type only.
        result = {
            "outcome": "failed",
            "stop_reason": "collector error",
            "partial": True,
            "error_type": type(error).__name__,
            "entries": [],
            "first_entry_seconds": None,
        }

    return {
        **result,
        "started_at": started_at,
        "finished_at": _now(),
        "elapsed_seconds": asyncio.get_running_loop().time() - started,
    }


def _validate_budget(timeout_seconds: float) -> None:
    if (
        not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= MAX_COMPARISON_SECONDS
    ):
        raise ValueError(
            "Comparison timeout must be positive and at most 240 seconds",
        )


async def compare_authenticated_home(
    settings: Settings,
    browser: Browser,
    page: Tab,
    timeout_seconds: float = MAX_COMPARISON_SECONDS,
) -> dict[str, Any]:
    """Compare on the caller's authenticated page; leave its session open."""
    _validate_budget(timeout_seconds)

    if page.browser is not browser:
        raise ValueError("The comparison page must belong to its browser")

    started_at = _now()
    loop = asyncio.get_running_loop()
    started = loop.time()

    async with asyncio.timeout(WARMUP_TIMEOUT_SECONDS):
        signed_in = await page.evaluate(
            "location.origin === 'https://www.youtube.com' && "
            "window.ytcfg?.get?.('LOGGED_IN') === true",
        )

        if signed_in is not True:
            raise YouTubeFeedError(
                "Sign in to YouTube before comparing collectors",
            )

        cookies = [
            cookie.to_json()
            for cookie in await page.send(cdp.storage.get_cookies())
            if any(
                cookie.domain.lstrip(".") == host
                or cookie.domain.lstrip(".").endswith("." + host)
                for host in ("youtube.com", "google.com")
            )
        ]
        snapshot_at = _now()

    warmup_seconds = loop.time() - started
    warmup_finished_at = _now()
    ready = asyncio.Event()

    try:
        async with asyncio.TaskGroup() as group:
            browser_task = group.create_task(
                _timed(
                    ready,
                    lambda: _browser_collection(
                        page, settings, timeout_seconds,
                    ),
                ),
            )
            ytdlp_task = group.create_task(
                _timed(
                    ready,
                    lambda: _ytdlp_collection(
                        cookies,
                        timeout_seconds,
                        settings.youtube.region,
                    ),
                ),
            )
            ready.set()
    finally:
        cookies.clear()

    browser_result, ytdlp_result = browser_task.result(), ytdlp_task.result()
    metrics = compare_entries(
        browser_result["entries"], ytdlp_result["entries"],
    )
    start_skew = abs(
        (
            dt.datetime.fromisoformat(browser_result["started_at"])
            - dt.datetime.fromisoformat(ytdlp_result["started_at"])
        ).total_seconds(),
    )

    return {
        "schema_version": 1,
        "feed_url": ORIGIN + "/",
        "started_at": started_at,
        "finished_at": _now(),
        "listing_budget_seconds": timeout_seconds,
        "total_elapsed_seconds": loop.time() - started,
        "warmup": {
            "started_at": started_at,
            "finished_at": warmup_finished_at,
            "elapsed_seconds": warmup_seconds,
        },
        "cleanup_seconds": 0.0,
        "method_start_skew_seconds": start_skew,
        "session": {
            "authenticated_before_listing": True,
            "shared_cookie_snapshot_at": snapshot_at,
            "cache_updated": False,
        },
        "region": settings.youtube.region,
        "language": "en-GB",
        "browser": browser_result,
        "yt_dlp": ytdlp_result,
        "comparison": metrics,
        "limitations": list(LIMITATIONS),
    }


async def compare_home(
    settings: Settings,
    timeout_seconds: float = MAX_COMPARISON_SECONDS,
) -> dict[str, Any]:
    """Own the browser lifecycle around the reusable in-session comparison."""
    _validate_budget(timeout_seconds)
    started_at = _now()
    loop = asyncio.get_running_loop()
    started = loop.time()
    profile_dir = (
        settings.youtube.profile_dir
        or settings.data_dir / "youtube-browser-profile"
    )
    session_file = profile_dir / "youtube-session.json"
    config = BrowserConfig.from_settings(settings, headless=True)._replace(
        profile_dir=profile_dir,
        session_file=session_file,
    )

    async with BrowserSession(config) as session:
        async with asyncio.timeout(WARMUP_TIMEOUT_SECONDS):
            browser = await session.get_browser()
            page = await browser.get("about:blank", new_tab=True)
            await navigate(
                page,
                _localized(ORIGIN + "/", settings.youtube.region),
            )

        launch_seconds = loop.time() - started
        report = await compare_authenticated_home(
            settings,
            browser,
            page,
            timeout_seconds,
        )
        cleanup_started = loop.time()
        cache_updated = await save_youtube_session_from_page(
            browser, page, session_file,
        )
        report["session"]["cache_updated"] = cache_updated

    report["started_at"] = started_at
    report["finished_at"] = _now()
    report["total_elapsed_seconds"] = loop.time() - started
    report["cleanup_seconds"] = loop.time() - cleanup_started
    report["warmup"]["started_at"] = started_at
    report["warmup"]["elapsed_seconds"] += launch_seconds

    return report
