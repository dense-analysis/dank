"""Chronological hckrnews listings and optional official HN comment trees."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import math
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import aiohttp

from dank.model import RawPost
from dank.scrape.http import HostCooldownError, http_response
from dank.scrape.metrics import count
from dank.scrape.types import ScrapeBatch

if TYPE_CHECKING:
    from dank.config import HackerNewsSettings, SourceConfig

logger = logging.getLogger(__name__)
HCKRNEWS_URL = "https://hckrnews.com/"
HN_API_URL = "https://hacker-news.firebaseio.com/v0/item/"
LISTING_TIMEOUT_SECONDS = 30
FILTERS = ("top10", "top20", "top50", "all")


class _HomepageParser(HTMLParser):
    def __init__(self, document: str) -> None:
        super().__init__(convert_charrefs=True)
        self.document = document
        self.offsets = [0]

        for line in document.splitlines(keepends=True):
            self.offsets.append(self.offsets[-1] + len(line))

        self.entries: list[dict[str, Any]] = []
        self.entry: dict[str, Any] | None = None
        self.start = 0
        self.field = ""
        self.title = False
        self.source = False

    def _offset(self) -> int:
        line, column = self.getpos()

        return self.offsets[line - 1] + column

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        fields = dict(attrs)
        classes = (fields.get("class") or "").split()

        if tag == "li" and "entry" in classes:
            self.entry = {"id": fields.get("id"), "link_text": ""}
            self.start = self._offset()

        if self.entry is None:
            return

        if tag == "a":
            self._anchor(fields, classes)
        elif tag == "span":
            self.source = "source" in classes
            self.field = next(
                (name for name in ("points", "comments") if name in classes),
                "",
            )

            if "points" in classes:
                self.entry["homepage"] = "homepage" in classes

    def _anchor(
        self, fields: dict[str, str | None], classes: list[str],
    ) -> None:
        assert self.entry is not None

        if "hn" in classes:
            self.entry["date"] = fields.get("data-date")
        elif "link" in classes:
            self.title = True
            self.entry["link"] = fields.get("href")
            self.entry["dead"] = "dead" in classes
            self.entry["type"] = "job" if "job" in classes else "story"

    def handle_data(self, data: str) -> None:
        if self.entry is None:
            return

        if self.field:
            self.entry[self.field] = self.entry.get(self.field, "") + data
        elif self.title and not self.source:
            self.entry["link_text"] += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "li" and self.entry is not None:
            self.entry["link_text"] = self.entry["link_text"].strip()
            self.entry["html"] = self.document[self.start:self._offset() + 5]
            self.entries.append(self.entry)
            self.entry = None
        elif tag == "a":
            self.title = False
        elif tag == "span":
            self.field = ""
            self.source = False


def _parse_homepage(document: str) -> list[dict[str, Any]]:
    parser = _HomepageParser(document)
    parser.feed(document)

    if not parser.entries:
        raise ValueError("hckrnews homepage contains no entries")

    return parser.entries


class _Entry(NamedTuple):
    raw: dict[str, Any]
    post_id: str
    created: dt.datetime
    points: int


def _entry(raw: object) -> _Entry:
    if not isinstance(raw, dict):
        raise ValueError("hckrnews entry is not an object")

    fields = cast(dict[str, Any], raw)
    post_id = str(fields.get("id", ""))

    if not post_id.isascii() or not post_id.isdecimal():
        raise ValueError("hckrnews entry has no numeric HN identifier")

    try:
        created = dt.datetime.fromtimestamp(int(fields["date"]), dt.UTC)
        points = int(fields.get("points") or 0)
    except (KeyError, ValueError, TypeError, OverflowError, OSError) as error:
        raise ValueError(
            "hckrnews entry has invalid date or points",
        ) from error

    return _Entry(fields, post_id, created, points)


def _day_cutoff(entries: list[_Entry], mode: str) -> int:
    # Match the site's daily cutoff, including partial-day scaling and ties.
    # https://hckrnews.com/js/hckrnews-min.4535fb6d0e.js (Day.pointsAbove)
    base = {"top10": 10, "top20": 20}.get(mode, len(entries) // 2)
    hours = int((
        max(entry.created for entry in entries)
        - min(entry.created for entry in entries)
    ).total_seconds() // 3600)
    size = math.ceil(hours / 24 * base)
    scores = sorted((entry.points for entry in entries), reverse=True)

    if size == 0:
        return 1

    return scores[min(size, len(scores)) - 1] or 1


def _day_posts(
    entries: list[_Entry], mode: str, request_url: str,
) -> list[RawPost]:
    cutoffs = {name: _day_cutoff(entries, name) for name in FILTERS[:-1]}
    scores = sorted((entry.points for entry in entries), reverse=True)
    captured = dt.datetime.now(dt.UTC)
    posts: list[RawPost] = []

    ordered = sorted(entries, key=lambda value: value.created, reverse=True)

    for entry in ordered:
        matches = [
            name for name in FILTERS
            if name == "all" or entry.points >= cutoffs[name]
        ]

        if mode not in matches:
            continue

        payload = {
            "hckrnews": entry.raw,
            "hacker_news": {
                "day": entry.created.date().isoformat(),
                "rank": scores.index(entry.points) + 1,
                "filters": matches,
            },
        }
        posts.append(RawPost(
            domain="news.ycombinator.com", post_id=entry.post_id,
            url=f"https://news.ycombinator.com/item?id={entry.post_id}",
            post_created_at=entry.created, scraped_at=captured,
            source="hacker_news", request_url=request_url,
            payload=json.dumps(payload, ensure_ascii=False),
        ))

    return posts


def _group_entries(raw_entries: list[Any]) -> dict[dt.date, list[_Entry]]:
    days: dict[dt.date, list[_Entry]] = defaultdict(list)
    seen: set[str] = set()

    for raw in raw_entries:
        try:
            entry = _entry(raw)
        except ValueError as error:
            count("parse_failures")
            logger.warning("Skipping invalid Hacker News entry: %s", error)
            continue

        if entry.post_id not in seen:
            days[entry.created.date()].append(entry)
            seen.add(entry.post_id)

    return days


async def _fetch_text(client: aiohttp.ClientSession, url: str) -> str:
    async with asyncio.timeout(LISTING_TIMEOUT_SECONDS):
        async with http_response(client, url) as response:
            response.raise_for_status()

            return await response.text()


async def scrape_hacker_news(
    settings: HackerNewsSettings, source: SourceConfig,
    client: aiohttp.ClientSession,
) -> AsyncIterator[ScrapeBatch]:
    """Read the live two-day listing, then one archive per older UTC day."""
    mode = str(source.hacker_news.filter) if source.hacker_news else "top50"
    days = _group_entries(_parse_homepage(await _fetch_text(
        client, HCKRNEWS_URL,
    )))
    today = dt.datetime.now(dt.UTC).date()

    for offset in range(settings.days):
        day = today - dt.timedelta(days=offset)
        request_url = HCKRNEWS_URL
        entries = days.get(day)

        if entries is None:
            request_url = f"{HCKRNEWS_URL}data/{day:%Y%m%d}.js"

            try:
                document = await _fetch_text(client, request_url)
                raw: object = json.loads(document)

                if not isinstance(raw, list):
                    raise ValueError("hckrnews archive must be an array")

                entries = _group_entries(cast(list[Any], raw)).get(day, [])
            except (
                aiohttp.ClientError, TimeoutError, HostCooldownError,
            ) as err:
                count("fetch_failures")
                logger.warning("Hacker News %s unavailable: %s", day, err)
                continue
            except ValueError as error:
                count("parse_failures")
                logger.warning("Hacker News %s invalid: %s", day, error)
                continue

        if entries:
            posts = _day_posts(entries, mode, request_url)
            logger.info("Hacker News %s: %d %s stories", day, len(posts), mode)
            yield ScrapeBatch(posts, [])


async def _fetch_item(
    client: aiohttp.ClientSession, item_id: int,
) -> dict[str, Any]:
    url = f"{HN_API_URL}{item_id}.json"

    async with http_response(client, url) as response:
        response.raise_for_status()
        raw: object = json.loads(await response.text())

    if not isinstance(raw, dict):
        raise ValueError("HN item is missing or invalid")

    item = cast(dict[str, Any], raw)

    if item.get("id") != item_id:
        raise ValueError("HN item identifier does not match request")

    return item


def _kids(item: dict[str, Any]) -> list[int]:
    children: object = item.get("kids", [])

    if not isinstance(children, list):
        return []

    return [
        child for child in cast(list[object], children)
        if type(child) is int and child > 0
    ]


async def _capture_details(
    settings: HackerNewsSettings, post: RawPost, client: aiohttp.ClientSession,
) -> RawPost:
    payload: dict[str, Any] = json.loads(post.payload)
    comments: list[dict[str, Any]] = []
    capture: dict[str, Any] = {
        "status": "captured", "limit": settings.comment_limit,
        "captured": 0, "requests": 0,
    }
    pending: deque[int] = deque()
    seen: set[int] = {int(post.post_id)}

    try:
        async with asyncio.timeout(settings.detail_timeout_seconds):
            capture["requests"] += 1
            story = await _fetch_item(client, int(post.post_id))
            if story.get("type") not in {"story", "job", "poll"}:
                raise ValueError("HN item is not a story")

            payload["story"] = story
            capture["total"] = story.get("descendants", 0)
            pending.extend(_kids(story))

            # The API supplies ranked children; breadth first keeps top-level
            # discussion represented within the user-configured request limit.
            # https://github.com/HackerNews/API#items
            while pending and capture["requests"] <= settings.comment_limit:
                item_id = pending.popleft()

                if item_id in seen:
                    continue

                seen.add(item_id)
                capture["requests"] += 1
                comment = await _fetch_item(client, item_id)

                if comment.get("type") != "comment":
                    raise ValueError("HN discussion child is not a comment")

                comments.append(comment)
                pending.extend(child for child in _kids(comment)
                               if child not in seen)

            if any(item_id not in seen for item_id in pending):
                capture.update(status="partial", reason="comment_limit")
    except (
        aiohttp.ClientError, TimeoutError, HostCooldownError, ValueError,
    ) as err:
        capture.update(
            status="partial" if comments else "failed",
            reason="timeout" if isinstance(err, TimeoutError) else str(err),
        )
        count("fetch_failures")
        logger.warning(
            "Hacker News %s comments incomplete: %s", post.post_id, err,
        )

    capture["captured"] = len(comments)
    payload["comments"] = comments
    payload["comment_capture"] = capture

    return post._replace(
        scraped_at=dt.datetime.now(dt.UTC),
        request_url=f"{HN_API_URL}{post.post_id}.json",
        payload=json.dumps(payload, ensure_ascii=False),
    )


async def scrape_hacker_news_details(
    settings: HackerNewsSettings, posts: list[RawPost],
    client: aiohttp.ClientSession,
) -> AsyncIterator[ScrapeBatch]:
    """Capture bounded comment trees only for an explicit detail request."""
    for post in posts[:settings.detail_max_posts]:
        detailed = await _capture_details(settings, post, client)
        yield ScrapeBatch([detailed], [])
