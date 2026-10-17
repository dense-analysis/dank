from __future__ import annotations

import asyncio
import datetime
import json
import logging
import re
from typing import cast
from urllib.parse import urlencode, urljoin, urlsplit

import zendriver
from zendriver import cdp

from dank.config import RedditSettings
from dank.model import RawPost
from dank.scrape.http import HostCooldownError
from dank.scrape.metrics import count, current_source
from dank.scrape.zendriver import NetworkCapture, NetworkResponse

from .access import (
    RedditRateLimitError,
    RedditScrapeError,
    check_page_responses,
    check_response_status,
    current_requests,
    pace_request,
    request_slot,
)
from .cache import listing_signature
from .documents import (
    DETAIL_SNAPSHOT_SCRIPT,
    fetch_document,
    next_comments_url,
)
from .html import extract_html_posts
from .payloads import (
    POST_PATH,
    as_dict,
    extract_json_posts,
    number,
    public_url,
    text,
    timestamp,
)

logger = logging.getLogger(__name__)
DETAIL_TIMEOUT_SECONDS = 35
JSON_TIMEOUT_SECONDS = 10
MAX_COMMENT_SCROLLS = 5


def comment_capture(
    settings: RedditSettings,
    post: dict[str, object],
    *,
    status: str,
    comments: list[dict[str, object]] | None = None,
    has_more: bool = False,
) -> dict[str, object]:
    captured = comments or []
    total = number(post.get("num_comments"))

    return {
        "status": status,
        "sort": settings.comment_sort.value,
        "limit": settings.comment_limit,
        "total_available": total,
        "has_more": has_more or (total is not None and total > len(captured)),
        "comments": captured,
    }


def thread_url(post: dict[str, object], settings: RedditSettings) -> str:
    # Reddit calls Best "confidence" in its documented comments endpoint.
    # https://www.reddit.com/dev/api/#GET_comments_{article}
    sort = (
        "confidence"
        if settings.comment_sort == "best"
        else settings.comment_sort
    )

    return (
        text(post.get("permalink")).rstrip("/")
        + ".json?"
        + urlencode(
            {
                "sort": sort,
                "limit": settings.comment_limit
                if settings.capture_comments
                else 0,
                "raw_json": 1,
            },
        )
    )


def _comment(
    value: dict[str, object],
    post_id: str,
    parent_id: str,
    depth: int,
) -> dict[str, object] | None:
    identifier = text(value.get("name")) or text(value.get("id"))
    identifier = identifier.removeprefix("t1_")
    supplied_parent = text(value.get("parent_id")) or parent_id

    if (
        not re.fullmatch(r"[a-z0-9]+", identifier)
        or value.get("link_id", post_id) != post_id
        or supplied_parent != parent_id
        or not (text(value.get("body")) or text(value.get("body_html")))
    ):
        return None

    permalink = public_url(
        urljoin(
            "https://www.reddit.com",
            text(value.get("permalink")),
        ),
    )
    match = POST_PATH.match(urlsplit(permalink).path)

    if (
        urlsplit(permalink).hostname not in {"www.reddit.com", "reddit.com"}
        or match is None
        or f"t3_{match[2]}" != post_id
    ):
        permalink = ""

    return {
        "id": f"t1_{identifier}",
        "parent_id": parent_id,
        "author": text(value.get("author")) or "[deleted]",
        "body_html": text(value.get("body_html")),
        "body": text(value.get("body")),
        "score": number(value.get("score")),
        "created_utc": timestamp(value.get("created_utc")),
        "permalink": permalink,
        "is_op": value.get("is_submitter", value.get("is_op")) is True,
        "depth": depth,
    }


def extract_thread(
    payload: object,
    post_id: str,
    community: str,
    settings: RedditSettings,
) -> dict[str, object] | None:
    """Validate the target post before accepting its ordered comment tree."""
    if not isinstance(payload, list):
        return None

    listings = cast(list[object], payload)

    if len(listings) != 2:
        return None
    posts = extract_json_posts(listings[0], community)
    post = next((item for item in posts if item.get("id") == post_id), None)
    children = as_dict(as_dict(listings[1]).get("data")).get("children")

    if post is None or not isinstance(children, list):
        return None

    comments: list[dict[str, object]] = []
    seen: set[str] = set()
    more = False
    pending = [
        (item, post_id, 0) for item in reversed(cast(list[object], children))
    ]

    while pending:
        item, parent_id, depth = pending.pop()
        node = as_dict(item)
        value = as_dict(node.get("data"))

        if node.get("kind") != "t1":
            more = True
            continue

        comment = _comment(value, post_id, parent_id, depth)

        if comment is None:
            more = True
            continue

        identifier = text(comment["id"])

        if identifier in seen:
            continue

        if len(comments) >= settings.comment_limit:
            more = True
            break

        seen.add(identifier)
        comments.append(comment)
        replies = as_dict(as_dict(value.get("replies")).get("data")).get(
            "children",
        )

        if isinstance(replies, list):
            pending.extend(
                (reply, identifier, depth + 1)
                for reply in reversed(cast(list[object], replies))
            )

    post["comment_capture"] = comment_capture(
        settings,
        post,
        status="captured",
        comments=comments,
        has_more=more,
    )
    post["raw"] = {"thread": payload}

    return post


def _dom_comments(
    snapshot: dict[str, object],
    post: dict[str, object],
    settings: RedditSettings,
) -> dict[str, object] | None:
    supplied = snapshot.get("comments")

    if not isinstance(supplied, list):
        return None

    comments: list[dict[str, object]] = []
    depths: dict[str, int] = {text(post["id"]): -1}
    more = bool(snapshot.get("has_more"))

    for item in cast(list[object], supplied):
        value = as_dict(item)
        parent = text(value.get("parent_id"))

        if parent not in depths:
            more = True
            continue

        comment = _comment(value, text(post["id"]), parent, depths[parent] + 1)

        if comment is None:
            more = True
            continue

        identifier = text(comment["id"])

        if identifier in depths:
            continue

        if len(comments) >= settings.comment_limit:
            more = True
            break

        depths[identifier] = cast(int, comment["depth"])
        comments.append(comment)

    # A blank not-yet-loaded tree must not claim a successful capture.
    if not comments and number(post.get("num_comments")) != 0:
        return None

    return comment_capture(
        settings,
        post,
        status="captured",
        comments=comments,
        has_more=more,
    )


def _merge_detail(
    listing: dict[str, object],
    detail: dict[str, object],
) -> dict[str, object]:
    merged = {**listing, **detail}

    for key in ("selftext", "selftext_html"):
        if (
            not text(detail.get(key))
            and "thread" not in as_dict(detail.get("raw"))
        ):
            merged[key] = listing.get(key, "")

    media: dict[str, dict[str, object]] = {}

    for item in cast(list[object], listing.get("media", [])) + cast(
        list[object],
        detail.get("media", []),
    ):
        record = as_dict(item)
        url = text(record.get("url"))
        parsed = urlsplit(url)
        key = url

        if (
            record.get("asset_type") == "video"
            and parsed.hostname == "v.redd.it"
            and len(parsed.path.split("/")) > 1
        ):
            # Signed URL queries change between the feed and detail visit.
            key = "reddit-video:" + parsed.path.split("/")[1]

        media[key] = {**media.get(key, {}), **record}

    merged["media"] = list(media.values())
    merged["raw"] = {
        "listing": listing.get("raw"),
        "detail": detail.get("raw"),
    }

    return merged


async def _snapshot(page: zendriver.Tab) -> dict[str, object]:
    return as_dict(
        json.loads(str(await page.evaluate(DETAIL_SNAPSHOT_SCRIPT))),
    )


def _network_thread(
    responses: list[NetworkResponse],
    raw: RawPost,
    settings: RedditSettings,
) -> dict[str, object] | None:
    for response in responses:
        if not 200 <= response.status < 300:
            continue

        try:
            candidate = extract_thread(
                json.loads(response.body),
                raw.post_id,
                text(as_dict(json.loads(raw.payload)).get("subreddit")),
                settings,
            )
        except (ValueError, RecursionError):
            continue

        if candidate is not None:
            return candidate

    return None


async def _fetch_thread(
    page: zendriver.Tab,
    listing: dict[str, object],
    settings: RedditSettings,
) -> dict[str, object] | None:
    requests = current_requests.get()

    if requests is not None and not requests.json_available:
        return None

    await pace_request()
    # Abort in-page too, so a CDP timeout leaves no stray fetch behind.
    script = """(async () => {
        try {
            const response = await fetch(URL_VALUE, {
                credentials: 'same-origin',
                signal: AbortSignal.timeout(TIMEOUT_VALUE),
            });
            return JSON.stringify({
                status: response.status,
                retry_after: response.headers.get('Retry-After'),
                body: response.ok ? await response.text() : '',
            });
        } catch { return ''; }
    })()""".replace(
        "URL_VALUE", json.dumps(thread_url(listing, settings)),
    ).replace(
        "TIMEOUT_VALUE",
        str(JSON_TIMEOUT_SECONDS * 1000),
    )

    try:
        response = as_dict(json.loads(
            str(await page.evaluate(script, await_promise=True)),
        ))
        status = number(response.get("status")) or 0

        if status == 429:
            check_response_status(
                status, thread_url(listing, settings),
                text(response.get("retry_after")),
            )

        if status in (401, 403) and requests is not None:
            requests.json_available = False
            logger.info(
                "Reddit comments JSON denied (%d); using rendered threads "
                "for the rest of this run", status,
            )

        # JSON denial can still leave a usable rendered thread.
        if not 200 <= status < 300:
            return None

        payload = json.loads(text(response.get("body")))

        return extract_thread(
            payload,
            text(listing["id"]),
            text(listing["subreddit"]),
            settings,
        )
    except (ValueError, RecursionError):
        return None


async def _rendered_detail(
    page: zendriver.Tab,
    raw: RawPost,
    settings: RedditSettings,
) -> dict[str, object] | None:
    snapshot = await _snapshot(page)
    previous_count = 0
    stalls = 0

    for _ in range(MAX_COMMENT_SCROLLS if settings.capture_comments else 0):
        supplied = snapshot.get("comments")
        count = (
            len(cast(list[object], supplied))
            if isinstance(supplied, list)
            else 0
        )

        if (
            count >= settings.comment_limit or stalls >= 2
            or (count > 0 and snapshot.get("has_more") is False)
            or snapshot.get("can_scroll") is False
        ):
            break

        await pace_request()
        await page.evaluate("""(() => {
            const more = document.querySelector(
                '#top-level-more-comments-partial');
            (more || document.querySelector('shreddit-comment-tree'))
                ?.scrollIntoView({block: 'end'});
        })()""")
        await asyncio.sleep(settings.scrape.scroll_pause_seconds)
        snapshot = await _snapshot(page)
        stalls = stalls + 1 if count == previous_count else 0
        previous_count = count

    return _snapshot_detail(snapshot, raw, settings)


def _snapshot_detail(
    snapshot: dict[str, object], raw: RawPost, settings: RedditSettings,
) -> dict[str, object] | None:
    community = text(as_dict(json.loads(raw.payload)).get("subreddit"))
    current = POST_PATH.match(urlsplit(text(snapshot.get("url"))).path)

    if current is None or f"t3_{current[2]}" != raw.post_id:
        return None

    post = next(
        (
            item
            for item in extract_html_posts(
                text(snapshot.get("html")), community,
            )
            if item.get("id") == raw.post_id
        ),
        None,
    )

    if post is not None and settings.capture_comments:
        comments = _dom_comments(snapshot, post, settings)

        if comments is not None:
            post["comment_capture"] = comments
            post["raw"] = {
                **as_dict(post.get("raw")),
                "comments_html": text(snapshot.get("comments_html")),
            }

    return post


def _captured_detail(
    listing: dict[str, object], detail: dict[str, object],
) -> dict[str, object]:
    post = _merge_detail(listing, detail)
    captured_at = datetime.datetime.now(datetime.UTC).isoformat()
    post["detail_capture"] = {
        "status": "captured", "captured_at": captured_at,
        "listing_signature": listing_signature(listing),
    }
    comments = as_dict(post.get("comment_capture"))

    if comments.get("status") == "captured":
        post["comment_capture"] = {**comments, "captured_at": captured_at}

    return post


async def capture_post_detail(
    page: zendriver.Tab | None,
    raw: RawPost,
    settings: RedditSettings,
) -> RawPost:
    listing = as_dict(json.loads(raw.payload))
    capture = NetworkCapture(
        cast(zendriver.Tab, page),
        (r"https://(?:[a-z0-9-]+\.)*reddit\.com/",),
        (
            cdp.network.ResourceType.DOCUMENT,
            cdp.network.ResourceType.XHR,
            cdp.network.ResourceType.FETCH,
        ),
    )
    post = dict(listing)
    status = "failed" if settings.capture_comments else "disabled"
    post["comment_capture"] = comment_capture(settings, listing, status=status)
    post["detail_capture"] = {"status": "failed"}
    sort = (
        "confidence"
        if settings.comment_sort == "best"
        else settings.comment_sort
    )
    url = raw.url.rstrip("/") + "/?" + urlencode({"sort": sort})

    try:
        if page is None:
            raise RuntimeError("Detail browser tab unavailable")

        # Navigation cooldown and pacing do not consume the page-read budget.
        # Allow the bounded JSON/comment steps their intentional pacing waits.
        requests = current_requests.get()
        pacing_budget = (
            requests.interval * (MAX_COMMENT_SCROLLS + 1) if requests else 0
        )

        async with request_slot(url):
            async with asyncio.timeout(DETAIL_TIMEOUT_SECONDS + pacing_budget):
                await capture.start()
                await page.activate()
                await page.send(cdp.page.navigate(url))
                await asyncio.sleep(
                    max(1.0, settings.scrape.scroll_pause_seconds),
                )
                responses = await capture.drain(timeout_seconds=0.05)
                check_page_responses(responses, url)
                detail = (
                    _network_thread(responses, raw, settings)
                    if settings.capture_comments else None
                )

                if detail is None and settings.capture_comments:
                    detail = await _fetch_thread(page, listing, settings)

                if detail is None:
                    detail = await _rendered_detail(page, raw, settings)
                    check_page_responses(
                        await capture.drain(timeout_seconds=0.05), url,
                    )

                if detail is not None:
                    post = _captured_detail(post, detail)

                if not settings.capture_comments:
                    post["comment_capture"] = comment_capture(
                        settings, post, status="disabled",
                    )
    except Exception as error:
        _record_detail_failure(post, raw, error)
    finally:
        await capture.stop()

    return raw._replace(
        payload=json.dumps(post, separators=(",", ":")),
        scraped_at=datetime.datetime.now(datetime.UTC),
    )


def defer_post_detail(raw: RawPost, settings: RedditSettings) -> RawPost:
    """Keep a listing when detail visits must wait for a later run."""
    post = as_dict(json.loads(raw.payload))
    post["detail_capture"] = {"status": "failed", "reason": "rate_limited"}
    post["comment_capture"] = comment_capture(
        settings, post,
        status="failed" if settings.capture_comments else "disabled",
    )

    return raw._replace(payload=json.dumps(post, separators=(",", ":")))


def _record_detail_failure(
    post: dict[str, object], raw: RawPost, error: Exception,
) -> None:
    if isinstance(error, RedditRateLimitError | HostCooldownError):
        post["detail_capture"] = {
            "status": "failed", "reason": "rate_limited",
        }

    if isinstance(error, RedditScrapeError | HostCooldownError):
        count("fetch_failures")
        stats = current_source.get()

        if stats is not None:
            stats.error_type = type(error).__name__

    # A detail failure must not discard an already discovered listing.
    logger.warning(
        "Reddit detail unavailable for %s (%s: %s)",
        raw.post_id,
        type(error).__name__,
        error,
    )


async def capture_post_document(
    page: zendriver.Tab | None, raw: RawPost, settings: RedditSettings,
) -> RawPost | None:
    requests = current_requests.get()

    if page is None or (
        requests is not None and not requests.documents_available
    ):
        return None

    listing = as_dict(json.loads(raw.payload))
    sort = (
        "confidence"
        if settings.comment_sort == "best" else settings.comment_sort
    )
    url = raw.url.rstrip("/") + "/?" + urlencode({"sort": sort})

    try:
        snapshot = await fetch_document(page, url, raw.post_id)

        if snapshot is None or not text(snapshot.get("html")):
            return None

        snapshot = await _document_comments(page, snapshot, raw, settings)
        detail = _snapshot_detail(snapshot, raw, settings)

        if detail is None or (settings.capture_comments and as_dict(
            detail.get("comment_capture"),
        ).get("status") != "captured"):
            return None

        post = _captured_detail(listing, detail)
        as_dict(post["detail_capture"])["method"] = "document"

        if not settings.capture_comments:
            post["comment_capture"] = comment_capture(
                settings, post, status="disabled",
            )
    except (RedditScrapeError, HostCooldownError) as error:
        post = as_dict(json.loads(defer_post_detail(raw, settings).payload))
        post["detail_capture"] = {"status": "failed"}
        _record_detail_failure(post, raw, error)
    except Exception:
        # A document without a usable archive can still render normally.
        logger.debug(
            "Reddit document fallback for %s", raw.post_id, exc_info=True,
        )
        return None

    return raw._replace(
        payload=json.dumps(post, separators=(",", ":")),
        scraped_at=datetime.datetime.now(datetime.UTC),
    )


async def _document_comments(
    page: zendriver.Tab, snapshot: dict[str, object],
    raw: RawPost, settings: RedditSettings,
) -> dict[str, object]:
    community = text(as_dict(json.loads(raw.payload)).get("subreddit"))
    comments = list(cast(list[object], snapshot.get("comments", [])))
    latest = snapshot
    visited: set[tuple[str, str]] = set()

    for _ in range(MAX_COMMENT_SCROLLS if settings.capture_comments else 0):
        url = next_comments_url(latest, community, raw.post_id)

        unique = {text(as_dict(item).get("id")) for item in comments}
        cursor = text(latest.get("next_cursor"))

        if (
            len(unique) >= settings.comment_limit or not url
            or (url, cursor) in visited
        ):
            break

        if not cursor:
            raise ValueError("Reddit comment cursor unavailable")

        visited.add((url, cursor))
        more = await fetch_document(page, url, raw.post_id, cursor=cursor)

        if more is None:
            break

        comments.extend(cast(list[object], more.get("comments", [])))
        snapshot["comments_html"] = (
            text(snapshot.get("comments_html")) + "\n"
            + text(more.get("comments_html"))
        )
        latest = more

    return {**snapshot, "comments": comments}


def defer_post_refresh(raw: RawPost, settings: RedditSettings) -> RawPost:
    post = as_dict(json.loads(raw.payload))
    post["detail_capture"] = {"status": "deferred"}
    post["comment_capture"] = comment_capture(
        settings, post,
        status="deferred" if settings.capture_comments else "disabled",
    )

    return raw._replace(payload=json.dumps(post, separators=(",", ":")))
