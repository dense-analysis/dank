"""Validated reader filters and parameterized queries, independent of HTTP."""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
from typing import Any, NamedTuple, cast

from multidict import MultiMapping

from dank.config import SourceConfig

POST_COLUMNS = (
    "domain, post_id, url, author, title, html, created_at, updated_at, source"
)


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

    def signature(self) -> str:
        # Bind cursors to filters, while allowing page-size changes.
        values = self._asdict()
        values.pop("cursor")
        values.pop("limit")

        return hashlib.sha256(
            json.dumps(values, sort_keys=True, default=str).encode(),
        ).hexdigest()[:24]


def parse_filters(query: MultiMapping[str]) -> ReaderFilters:
    unknown = set(query) - {
        "q", "mode", "sort", "domain", "tag", "author", "after", "before",
        "limit", "cursor", "author_match",
    }

    if unknown:
        raise ValueError("Unknown reader filter")

    for key in set(query) - {"domain", "tag"}:
        if len(query.getall(key)) != 1:
            raise ValueError(f"{key} must appear only once")

    filters = ReaderFilters(
        q=query.get("q", "").strip(),
        mode=query.get("mode", "words"),
        sort=query.get("sort", "newest"),
        domains=_choices(query.getall("domain", [])),
        tags=_choices(query.getall("tag", [])),
        author=query.get("author", "").strip(),
        author_match=query.get("author_match", "contains"),
        after=_date(query.get("after", "")),
        before=_date(query.get("before", "")),
        limit=_limit(query.get("limit", "30")),
        cursor=query.get("cursor", ""),
    )
    _validate(filters)

    return filters


def _choices(values: list[str]) -> tuple[str, ...]:
    if len(values) > 100 or any(not v.strip() or len(v) > 253 for v in values):
        raise ValueError("Source and tag filters must be nonempty and bounded")

    return tuple(sorted({value.strip().lower() for value in values}))


def _date(value: str) -> dt.datetime | None:
    if not value:
        return None

    try:
        date = dt.date.fromisoformat(value)

        if date.isoformat() != value or not 1900 <= date.year <= 2298:
            raise ValueError
    except ValueError:
        raise ValueError(
            "Dates must be YYYY-MM-DD between 1900 and 2298",
        ) from None

    return dt.datetime.combine(date, dt.time(), tzinfo=dt.UTC)


def _limit(value: str) -> int:
    try:
        limit = int(value)
    except ValueError:
        raise ValueError("limit must be an integer from 1 to 100") from None

    if not 1 <= limit <= 100:
        raise ValueError("limit must be an integer from 1 to 100")

    return limit


def _validate(filters: ReaderFilters) -> None:
    if filters.mode not in {"words", "meaning", "combined"}:
        raise ValueError("mode must be words, meaning or combined")

    if filters.sort not in {"newest", "oldest", "relevance"}:
        raise ValueError("sort must be newest, oldest or relevance")

    if filters.sort == "relevance" and not filters.q:
        raise ValueError("Relevance ordering requires a search")

    if len(filters.q) > 2000 or len(filters.q.split()) > 50:
        raise ValueError("Search is limited to 2000 characters and 50 terms")

    if len(filters.author) > 300:
        raise ValueError("Author filter is limited to 300 characters")

    if filters.author_match not in {"contains", "exact"}:
        raise ValueError("author_match must be contains or exact")

    if filters.after and filters.before and filters.after > filters.before:
        raise ValueError("after must not be later than before")

    if filters.cursor:
        decode_cursor(filters)


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
        raise ValueError("Invalid cursor for these filters") from None


def build_query(
    filters: ReaderFilters, sources: tuple[SourceConfig, ...],
    embedding: tuple[float, ...] | None = None,
) -> tuple[str, dict[str, Any]]:
    conditions, params = _conditions(filters, sources)
    score = "0"
    expressions: list[str] = []
    relevance_priority = ""

    if filters.q and filters.mode in {"meaning", "combined"}:
        if not embedding:
            raise RuntimeError("Search embedding is unavailable")

        params["embedding"] = list(embedding)
        lengths = [
            "length(title_embedding) = length(%(embedding)s)",
            "length(html_embedding) = length(%(embedding)s)",
        ]
        distance = (
            "cosineDistance(title_embedding, %(embedding)s) * 0.65 + "
            "cosineDistance(html_embedding, %(embedding)s) * 0.35"
        )

        if filters.mode == "combined":
            literal_conditions: list[str] = []
            title_score = _word_conditions(
                filters.q, literal_conditions, params,
            )
            expressions.extend([
                "(" + " AND ".join(literal_conditions)
                + ") AS reader_literal_match",
                f"if(reader_literal_match, {title_score}, 0) "
                "AS reader_title_score",
                _guarded_distance(lengths) + " AS reader_distance",
            ])
            conditions.append(
                "(reader_literal_match OR reader_distance <= 1.0)",
            )
            # Separate sort keys guarantee literal priority over any distance.
            relevance_priority = (
                "reader_literal_match DESC, reader_title_score ASC, "
            )
            distance = "reader_distance"
        else:
            conditions.extend(lengths)
            conditions.append(f"({distance}) <= 1.0")

        score = (
            f"({distance}) - 0.3 * exp(-greatest("
            "dateDiff('second', created_at, now64(3)), 0) / 86400.0 / 21.0)"
        )
    elif filters.q:
        score = _word_conditions(filters.q, conditions, params)

    order = "ASC" if filters.sort == "oldest" else "DESC"
    ordering = f"created_at {order}, domain {order}, post_id {order}"

    if filters.sort == "relevance":
        ordering = relevance_priority + "reader_score ASC, " + ordering

    if filters.cursor:
        created_at, domain, post_id = decode_cursor(filters)
        params.update(cursor_time=created_at.astimezone(dt.UTC).replace(
            tzinfo=None,
        ).isoformat(sep=" "), cursor_domain=domain,
                      cursor_id=post_id)
        operator = ">" if filters.sort == "oldest" else "<"
        conditions.append(
            f"(created_at, domain, post_id) {operator} "
            "(toDateTime64(%(cursor_time)s, 6, 'UTC'), "
            "%(cursor_domain)s, %(cursor_id)s)",
        )

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    prefix = "WITH " + ", ".join(expressions) + " " if expressions else ""
    query = (
        f"{prefix}SELECT {POST_COLUMNS}, {score} AS reader_score "
        "FROM posts FINAL"
        f"{where} ORDER BY {ordering} LIMIT %(limit)s"
    )
    params["limit"] = filters.limit + 1

    return query, params


def _guarded_distance(lengths: list[str]) -> str:
    valid = " AND ".join(lengths)
    distances = [
        "cosineDistance("
        f"if({valid}, {column}, %(embedding)s), %(embedding)s) * {weight}"
        for column, weight in (
            ("title_embedding", 0.65), ("html_embedding", 0.35),
        )
    ]
    # Guard the inputs too: ClickHouse may eagerly evaluate both if branches.

    return f"if({valid}, " + " + ".join(distances) + ", 2.0)"


def _conditions(
    filters: ReaderFilters, sources: tuple[SourceConfig, ...],
) -> tuple[list[str], dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {}

    if filters.domains:
        conditions.append("domain IN %(domains)s")
        params["domains"] = list(filters.domains)

    if filters.tags:
        # Tags select source membership, independent of article content.
        domains = sorted({
            source.domain for source in sources
            if set(filters.tags).intersection(source.tags)
        })
        conditions.append("domain IN %(tag_domains)s" if domains else "0")
        params["tag_domains"] = domains

    if filters.author:
        conditions.append(
            "lowerUTF8(trimBoth(author)) = lowerUTF8(%(author)s)"
            if filters.author_match == "exact"
            else "positionCaseInsensitiveUTF8(author, %(author)s) > 0",
        )
        params["author"] = filters.author

    if filters.after:
        conditions.append("created_at >= toDateTime64(%(after)s, 3, 'UTC')")
        params["after"] = filters.after.strftime("%Y-%m-%d %H:%M:%S")

    if filters.before:
        conditions.append("created_at < toDateTime64(%(before)s, 3, 'UTC')")
        params["before"] = (filters.before + dt.timedelta(days=1)).strftime(
            "%Y-%m-%d %H:%M:%S",
        )

    return conditions, params


def _word_conditions(
    query: str, conditions: list[str], params: dict[str, Any],
) -> str:
    scores: list[str] = []

    for index, term in enumerate(dict.fromkeys(query.split())):
        key = f"term_{index}"
        params[key] = term
        conditions.append(
            "positionCaseInsensitiveUTF8(concat(title, ' ', "
            f"decodeHTMLComponent(extractTextFromHTML(html))), %({key})s) > 0",
        )
        scores.append(
            f"(positionCaseInsensitiveUTF8(title, %({key})s) > 0)",
        )

    # All terms must match; prefer articles matching more terms in the title.
    return "-toInt32(" + " + ".join(scores) + ")"
