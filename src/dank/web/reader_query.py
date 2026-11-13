"""Validated reader filters and parameterized queries, independent of HTTP."""

from __future__ import annotations

import datetime as dt
from typing import Any

from dank.api.posts.model import (
    ReaderFilters,
    decode_cursor as decode_cursor,
    encode_cursor as encode_cursor,
)
from dank.api.sources.model import source_kind
from dank.config import SourceConfig
from dank.web.reader_accounts import X_ACCOUNT_SQL, X_DOMAINS
from dank.web.reader_blogs import BLOG_CONDITION, EXTERNAL_BLOG_CONDITION
from dank.web.reader_flairs import (
    FLAIR_WHITESPACE_PATTERN,
    LATEST_REDDIT_FLAIRS_SQL,
    REDDIT_COMMUNITY_SQL as REDDIT_COMMUNITY_SQL,
)
from dank.web.reader_links import HN_LINKED_POSTS_SQL, RESOLVED_HN_POSTS_SQL
from dank.web.reader_x_home import home_listing

POST_COLUMNS = (
    "domain, post_id, url, author, title, html, created_at, updated_at, "
    "source, categories"
)


def build_query(
    filters: ReaderFilters, sources: tuple[SourceConfig, ...],
    embedding: tuple[float, ...] | None = None,
    *, paginate: bool = True, snapshot_time: str | None = None,
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
    table = "posts FINAL"
    columns = POST_COLUMNS

    if filters.x_feed:
        table, home_params, home_condition = home_listing(
            filters, snapshot_time,
        )
        params.update(home_params)
        columns += ", recommended_at, recommendation_position"
        position_order = "DESC" if filters.sort == "oldest" else "ASC"
        ordering = (
            f"recommended_at {order}, "
            f"recommendation_position {position_order}, "
            f"domain {order}, post_id {order}"
        )

        conditions.extend([home_condition] if home_condition else [])

    if filters.sort == "relevance":
        ordering = relevance_priority + "reader_score ASC, " + ordering

    if filters.cursor and not filters.x_feed:
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

    if filters.kind in {"", "all"} or (
        filters.kind == "stories" and not filters.blog_feed
        and not filters.domains
    ):
        resolved = RESOLVED_HN_POSTS_SQL

        if filters.kind == "stories" or "stories" in filters.platforms:
            target_conditions, _ = _collection_conditions(filters, sources)
            # A canonical target must belong to this collection before its
            # source-feed wrapper can disappear from the collection.
            resolved += (
                " AND (target.domain, target.post_id) IN "
                "(SELECT domain, post_id FROM posts FINAL WHERE "
                + " AND ".join(target_conditions) + ")"
            )

        conditions.append("(domain, post_id) NOT IN (" + resolved + ")")

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    prefix = "WITH " + ", ".join(expressions) + " " if expressions else ""
    pagination = f" ORDER BY {ordering} LIMIT %(limit)s" if paginate else ""
    query = (
        f"{prefix}SELECT {columns}, {score} AS reader_score "
        f"FROM {table}"
        f"{where}{pagination}"
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


def _collection_conditions(
    filters: ReaderFilters, sources: tuple[SourceConfig, ...],
) -> tuple[list[str], dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {}

    if filters.kind == "stories" and filters.blog_feed == "external":
        conditions.append(EXTERNAL_BLOG_CONDITION)
    elif filters.kind == "stories" and filters.blog_feed == "sources":
        params["blog_domains"] = sorted({
            source.domain for source in sources
            if source_kind(source.domain) in {"rss", "hacker_news"}
        })
        conditions.append(
            "domain IN %(blog_domains)s" if params["blog_domains"] else "0",
        )
    elif filters.kind in {"reddit", "youtube", "stories", "x"}:
        conditions.append(_platform_condition(filters.kind, sources, params))
    elif filters.kind == "hacker_news":
        conditions.append("domain = 'news.ycombinator.com'")
        ranking = filters.hn_filter or "top50"

        if ranking != "all":
            # Eligibility is captured from the complete hckrnews day, before
            # collection or search narrows it. Match the processed version.
            conditions.append(
                "(post_id, toUnixTimestamp64Milli(updated_at)) IN "
                "(SELECT post_id, toUnixTimestamp64Milli(scraped_at) "
                "FROM raw_posts WHERE domain = 'news.ycombinator.com' "
                "AND has(JSONExtract(payload, 'hacker_news', 'filters', "
                "'Array(String)'), %(hn_filter)s))",
            )
            params["hn_filter"] = ranking

    if filters.platforms:
        conditions.append("(" + " OR ".join(
            _platform_condition(platform, sources, params)
            for platform in filters.platforms
        ) + ")")

    identity_conditions, identity_params = _identity_conditions(filters)
    conditions.extend(identity_conditions)
    params.update(identity_params)

    return conditions, params


def _identity_conditions(
    filters: ReaderFilters,
) -> tuple[list[str], dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {}

    if filters.categories:
        conditions.append("hasAny(categories.name, %(categories)s)")
        params["categories"] = list(filters.categories)

    if filters.account:
        conditions.append(f"{X_ACCOUNT_SQL} = %(account)s")
        params["account"] = filters.account

    if filters.community:
        conditions.append(f"{REDDIT_COMMUNITY_SQL} = %(community)s")
        params["community"] = filters.community

    if filters.flairs:
        conditions.append(
            "post_id IN (SELECT post_id FROM "
            f"({LATEST_REDDIT_FLAIRS_SQL}) WHERE flair IN %(flairs)s)",
        )
        params["flairs"] = list(filters.flairs)
        params["flair_whitespace"] = FLAIR_WHITESPACE_PATTERN

    return conditions, params


def _platform_condition(
    platform: str, sources: tuple[SourceConfig, ...], params: dict[str, Any],
) -> str:
    if platform == "stories":
        return "(" + BLOG_CONDITION + ")"

    if platform == "x":
        params["x_domains"] = list(X_DOMAINS)

        return "domain IN %(x_domains)s"

    if platform == "hacker_news":
        # All keeps one canonical item, including publishers discovered on HN.
        return (
            "(domain = 'news.ycombinator.com' OR (domain, post_id) IN ("
            + HN_LINKED_POSTS_SQL + "))"
        )

    return {
        "reddit": "domain = 'reddit.com'",
        "youtube": "domain = 'youtube.com'",
    }[platform]


def _conditions(
    filters: ReaderFilters, sources: tuple[SourceConfig, ...],
) -> tuple[list[str], dict[str, Any]]:
    conditions, params = _collection_conditions(filters, sources)

    if (filters.kind == "x" and filters.x_view == "posts"
            and not filters.x_feed):
        # Missing metadata is not proof of a reply.
        conditions.append(
            "(domain, post_id, toUnixTimestamp64Milli(updated_at)) NOT IN "
            "(SELECT domain, post_id, toUnixTimestamp64Milli(scraped_at) "
            "FROM raw_posts WHERE domain IN %(x_domains)s "
            "AND match(JSONExtractString(payload, 'legacy', "
            "'in_reply_to_status_id_str'), '^[0-9]{1,20}$') "
            "AND JSONExtractString(payload, 'legacy', "
            "'in_reply_to_status_id_str') != post_id)",
        )

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
