"""Browsing rules: reader filters, cursors and the pages they select.

Both servers share these functions, so a filter or cursor means the same
thing on either. Rule violations raise InvalidInput with the wording the
reader shows.
"""

# `count` is the API's name for an author's post count, as in sources/model.py.
# pyright: reportIncompatibleMethodOverride=false

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import re
from collections.abc import Sequence
from typing import Any, NamedTuple, cast

from dank.config import HackerNewsFilter
from dank.errors import InvalidInput

AUTHOR_PAGE_LIMIT = 50
MAX_FLAIR_LENGTH = 300

type Embedding = tuple[float, ...]
# The reader helpers build one JSON-shaped projection of a post for both
# servers, with keys that are absent rather than null. schemas.py gives it
# its contract. See "Projections" in docs/api.md.
type PostPayload = dict[str, Any]


class PostRow(NamedTuple):
    domain: str
    post_id: str
    url: str
    author: str
    title: str
    html: str
    created_at: dt.datetime
    updated_at: dt.datetime
    source: str


class AssetRow(NamedTuple):
    post_id: str
    url: str
    local_path: str
    content_type: str
    size_bytes: int


class Recommendation(NamedTuple):
    """Where a post last appeared in the saved For You feed."""

    observed_at: str
    position: int


class ListedPost(NamedTuple):
    post: PostRow
    recommendation: Recommendation | None = None


class PostsPage(NamedTuple):
    posts: tuple[PostPayload, ...]
    next_cursor: str | None
    limited: bool


class AuthorCursor(NamedTuple):
    snapshot: str
    count: int
    handle: str


class AuthorGroup(NamedTuple):
    handle: str
    name: str
    icon_url: str | None
    count: int
    posts: tuple[PostPayload, ...]


class AuthorListing(NamedTuple):
    authors: tuple[AuthorGroup, ...]
    has_more: bool
    total: int
    unattributed_posts: int


class AuthorsPage(NamedTuple):
    authors: tuple[AuthorGroup, ...]
    next_cursor: str | None
    total: int
    unattributed_posts: int


class HomeCursor(NamedTuple):
    snapshot: str
    observed_at: str
    position: int
    domain: str
    post_id: str


class FilterInput(NamedTuple):
    """Filter values as a request sent them, before any rule has run."""

    q: str = ""
    mode: str = "words"
    sort: str = "newest"
    domains: Sequence[str] = ()
    tags: Sequence[str] = ()
    platforms: Sequence[str] = ()
    flairs: Sequence[str] = ()
    author: str = ""
    author_match: str = "contains"
    after: str = ""
    before: str = ""
    limit: int | str = 30
    cursor: str = ""
    kind: str = ""
    community: str = ""
    account: str = ""
    x_feed: str = ""
    # None means the key was absent, which differs from sending it empty.
    hn_filter: str | None = None
    x_view: str | None = None
    blog_feed: str | None = None


class ReaderFilters(NamedTuple):
    q: str = ""
    mode: str = "words"
    sort: str = "newest"
    domains: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    author: str = ""
    author_match: str = "contains"
    after: dt.datetime | None = None
    before: dt.datetime | None = None
    limit: int = 30
    cursor: str = ""
    kind: str = ""
    community: str = ""
    flairs: tuple[str, ...] = ()
    account: str = ""
    hn_filter: str = ""
    x_view: str = "posts"
    platforms: tuple[str, ...] = ()
    blog_feed: str = ""
    x_feed: str = ""

    def signature(self) -> str:
        # Bind cursors to filters, while allowing page-size changes.
        values = self._asdict()
        values.pop("cursor")
        values.pop("limit")

        return hashlib.sha256(
            json.dumps(values, sort_keys=True, default=str).encode(),
        ).hexdigest()[:24]


def reader_filters(raw: FilterInput) -> ReaderFilters:
    if raw.hn_filter is not None and (
        raw.kind != "hacker_news" or raw.hn_filter not in HackerNewsFilter
    ):
        raise InvalidInput("hn_filter requires a valid Hacker News ranking")

    if raw.x_view is not None and raw.kind != "x":
        raise InvalidInput("x_view requires kind=x")

    if raw.blog_feed is not None and (
        raw.kind != "stories" or raw.blog_feed not in {"sources", "external"}
    ):
        raise InvalidInput(
            "blog_feed requires kind=stories and sources or external",
        )

    default_ranking = "top50" if raw.kind == "hacker_news" else ""
    filters = ReaderFilters(
        q=raw.q.strip(),
        mode=raw.mode,
        sort=raw.sort,
        domains=_choices(raw.domains),
        tags=_choices(raw.tags),
        platforms=_choices(raw.platforms),
        blog_feed=raw.blog_feed or "",
        author=raw.author.strip(),
        author_match=raw.author_match,
        after=_date(raw.after),
        before=_date(raw.before),
        limit=parse_limit(raw.limit),
        cursor=raw.cursor,
        kind=raw.kind,
        community=raw.community.strip().lower(),
        flairs=_flair_choices(raw.flairs),
        x_view="posts" if raw.x_view is None else raw.x_view,
        x_feed=raw.x_feed,
        account=raw.account.strip().removeprefix("@").lower(),
        hn_filter=default_ranking if raw.hn_filter is None else raw.hn_filter,
    )
    _validate(filters)

    return filters


def _choices(values: Sequence[str]) -> tuple[str, ...]:
    if len(values) > 100 or any(not v.strip() or len(v) > 253 for v in values):
        raise InvalidInput(
            "Source and tag filters must be nonempty and bounded",
        )

    return tuple(sorted({value.strip().lower() for value in values}))


def _flair_choices(values: Sequence[str]) -> tuple[str, ...]:
    if len(values) > 100 or any(
        not value.strip() or len(value) > MAX_FLAIR_LENGTH for value in values
    ):
        raise InvalidInput("Flair filters must be nonempty and bounded")

    return tuple(sorted({value.strip() for value in values}))


def _date(value: str) -> dt.datetime | None:
    if not value:
        return None

    try:
        date = dt.date.fromisoformat(value)

        if date.isoformat() != value or not 1900 <= date.year <= 2298:
            raise ValueError
    except ValueError:
        raise InvalidInput(
            "Dates must be YYYY-MM-DD between 1900 and 2298",
        ) from None

    return dt.datetime.combine(date, dt.time(), tzinfo=dt.UTC)


def parse_limit(value: object) -> int:
    message = "limit must be an integer from 1 to 100"

    try:
        limit = int(cast(Any, value))
    except (TypeError, ValueError):
        raise InvalidInput(message) from None

    if not 1 <= limit <= 100:
        raise InvalidInput(message)

    return limit


def _validate(filters: ReaderFilters) -> None:
    _validate_collection_filters(filters)

    if filters.mode not in {"words", "meaning", "combined"}:
        raise InvalidInput("mode must be words, meaning or combined")

    if filters.sort not in {"newest", "oldest", "relevance"}:
        raise InvalidInput("sort must be newest, oldest or relevance")

    if filters.sort == "relevance" and not filters.q:
        raise InvalidInput("Relevance ordering requires a search")

    if len(filters.q) > 2000 or len(filters.q.split()) > 50:
        raise InvalidInput("Search is limited to 2000 characters and 50 terms")

    if len(filters.author) > 300:
        raise InvalidInput("Author filter is limited to 300 characters")

    if filters.author_match not in {"contains", "exact"}:
        raise InvalidInput("author_match must be contains or exact")

    if filters.after and filters.before and filters.after > filters.before:
        raise InvalidInput("after must not be later than before")

    if filters.cursor:
        if filters.x_feed:
            decode_home_cursor(filters)
        else:
            decode_cursor(filters)


def _validate_collection_filters(filters: ReaderFilters) -> None:
    if filters.x_feed and (
        filters.kind != "x" or filters.x_feed != "for_you"
    ):
        raise InvalidInput("x_feed requires kind=x and for_you")

    if filters.kind not in {
        "", "all", "stories", "reddit", "x", "youtube", "hacker_news",
    }:
        raise InvalidInput(
            "kind must be all, stories, reddit, x, youtube or hacker_news",
        )

    if filters.platforms and (
        filters.kind not in {"", "all"}
        or set(filters.platforms) - {
            "stories", "reddit", "x", "youtube", "hacker_news",
        }
    ):
        raise InvalidInput(
            "platform requires kind=all and stories, reddit, x, "
            "youtube or hacker_news",
        )

    if filters.blog_feed and (
        filters.kind != "stories"
        or filters.blog_feed not in {"sources", "external"}
    ):
        raise InvalidInput(
            "blog_feed requires kind=stories and sources or external",
        )

    if filters.hn_filter and (
        filters.kind != "hacker_news"
        or filters.hn_filter not in HackerNewsFilter
    ):
        raise InvalidInput(
            "hn_filter requires kind=hacker_news and "
            "top10, top20, top50 or all",
        )

    if filters.x_view not in {"posts", "replies"}:
        raise InvalidInput("x_view must be posts or replies")

    if filters.account and (
        filters.kind != "x"
        or not re.fullmatch(r"[a-z0-9_]{1,15}", filters.account)
    ):
        raise InvalidInput("account requires kind=x and an account handle")

    if filters.community and (
        filters.kind != "reddit"
        or not re.fullmatch(r"[a-z0-9_]{2,21}", filters.community)
    ):
        raise InvalidInput(
            "community requires kind=reddit and a community name",
        )

    if filters.flairs and filters.kind != "reddit":
        raise InvalidInput("flair requires kind=reddit")


def encode_cursor(
    filters: ReaderFilters, created_at: dt.datetime, domain: str, post_id: str,
) -> str:
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=dt.UTC)

    payload = [1, filters.signature(), created_at.isoformat(), domain, post_id]

    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def decode_cursor(filters: ReaderFilters) -> tuple[dt.datetime, str, str]:
    try:
        if len(filters.cursor) > 8192 or filters.sort == "relevance":
            raise ValueError

        raw: object = json.loads(base64.b64decode(
            filters.cursor, altchars=b"-_", validate=True,
        ))

        if not isinstance(raw, list):
            raise ValueError

        payload = cast(list[object], raw)

        if (
            len(payload) != 5
            or payload[0] != 1 or payload[1] != filters.signature()
            or not all(isinstance(value, str) for value in payload[2:])
        ):
            raise ValueError

        created_at = dt.datetime.fromisoformat(cast(str, payload[2]))

        if created_at.tzinfo is None:
            raise ValueError

        return created_at, cast(str, payload[3]), cast(str, payload[4])
    except (ValueError, TypeError, UnicodeError):
        raise InvalidInput("Invalid cursor for these filters") from None


def decode_home_cursor(filters: ReaderFilters) -> HomeCursor:
    try:
        if len(filters.cursor) > 8192 or filters.sort == "relevance":
            raise ValueError

        values = json.loads(
            base64.b64decode(filters.cursor, altchars=b"-_", validate=True),
        )

        if (
            not isinstance(values, list)
            or len(cast(list[Any], values)) != 7
            or cast(list[Any], values)[:2] != [2, filters.signature()]
        ):
            raise ValueError

        snapshot, observed, position, domain, post_id = (
            cast(list[Any], values)[2:]
        )

        if not all(
            isinstance(value, str)
            for value in (snapshot, observed, domain, post_id)
        ):
            raise ValueError

        cutoff, captured = (
            dt.datetime.fromisoformat(snapshot),
            dt.datetime.fromisoformat(observed),
        )

        if (
            cutoff.tzinfo is None
            or captured.tzinfo is None
            or captured > cutoff
            or type(position) is not int
            or not 1 <= position <= 2**31
        ):
            raise ValueError

        return HomeCursor(snapshot, observed, position, domain, post_id)
    except (ValueError, TypeError, UnicodeError):
        raise InvalidInput("Invalid cursor for these filters") from None


def encode_home_cursor(
    filters: ReaderFilters,
    recommendation: Recommendation,
    domain: str,
    post_id: str,
    snapshot: str,
) -> str:
    observed = recommendation.observed_at.replace(" ", "T")

    if dt.datetime.fromisoformat(observed).tzinfo is None:
        observed += "+00:00"

    return base64.urlsafe_b64encode(
        json.dumps(
            [
                2,
                filters.signature(),
                snapshot,
                observed,
                recommendation.position,
                domain,
                post_id,
            ],
        ).encode(),
    ).decode()


def feed_snapshot(filters: ReaderFilters, now: dt.datetime) -> str:
    """The For You history a page reads: one cutoff for a whole session."""
    if filters.x_feed and filters.cursor:
        return decode_home_cursor(filters).snapshot

    return now.isoformat()


def next_cursor(
    filters: ReaderFilters, last: ListedPost, snapshot: str,
) -> str | None:
    """The cursor after a page ending at `last`; relevance has none."""
    if filters.sort == "relevance":
        return None

    post = last.post

    if filters.x_feed and last.recommendation:
        return encode_home_cursor(
            filters, last.recommendation, post.domain, post.post_id, snapshot,
        )

    return encode_cursor(filters, post.created_at, post.domain, post.post_id)


def require_author_feed(filters: ReaderFilters) -> None:
    if filters.x_feed != "for_you" or filters.limit > AUTHOR_PAGE_LIMIT:
        raise InvalidInput("Authors require X For You and a limit up to 50")


def author_cursor(
    filters: ReaderFilters, snapshot: str, count: int, handle: str,
) -> str:
    return base64.urlsafe_b64encode(json.dumps([
        "x-authors", filters.signature(), snapshot, count, handle,
    ]).encode()).decode()


def parse_author_cursor(filters: ReaderFilters, cursor: str) -> AuthorCursor:
    try:
        if len(cursor) > 8192:
            raise ValueError

        decoded: object = json.loads(base64.b64decode(
            cursor, altchars=b"-_", validate=True,
        ))

        if not isinstance(decoded, list):
            raise ValueError

        raw = cast(list[Any], decoded)

        if (len(raw) != 5 or raw[:2] != ["x-authors", filters.signature()]
                or not isinstance(raw[2], str)
                or type(raw[3]) is not int or not 1 <= raw[3] < 2**53
                or not isinstance(raw[4], str)
                or not re.fullmatch(r"[a-z0-9_]{1,15}", raw[4])):
            raise ValueError

        snapshot = dt.datetime.fromisoformat(raw[2])

        if snapshot.tzinfo is None:
            raise ValueError

        return AuthorCursor(
            snapshot.astimezone(dt.UTC).isoformat(), raw[3], raw[4],
        )
    except (ValueError, TypeError, UnicodeError):
        raise InvalidInput(
            "Invalid author cursor for these filters",
        ) from None
