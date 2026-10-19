"""Resolve saved destinations and incoming links without network fetches."""

from __future__ import annotations

import datetime as dt
import pathlib
from typing import Any

from dank.html_utils import html_text
from dank.links import (
    LinkPost,
    is_original_source,
    normalized_url,
    post_index_rows,
)
from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.web.app import (
    _sanitize_html,  # pyright: ignore[reportPrivateUsage]
    _summarize_html,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_excerpt import search_excerpt

# Suppress aggregator records only after an original source is collected.
RESOLVED_HN_POSTS_SQL = """
SELECT DISTINCT l.domain, l.post_id
FROM post_links AS l FINAL
INNER JOIN (SELECT domain, post_id, source, updated_at,
    hex(SHA256(html)) AS content_hash FROM posts FINAL) AS origin
    ON origin.domain = l.domain AND origin.post_id = l.post_id
    AND origin.updated_at = l.observed_at
    AND origin.content_hash = l.content_hash
INNER JOIN post_urls AS u FINAL ON u.url = l.target_url
INNER JOIN (SELECT domain, post_id, url, source, updated_at
    FROM posts FINAL) AS target
    ON target.domain = u.domain AND target.post_id = u.post_id
    AND u.observed_at <= target.updated_at
WHERE origin.source IN ('hacker_news', 'rss') AND l.kind = 'primary'
    AND (target.source IN ('x', 'reddit', 'youtube', 'hacker_news')
        OR replaceRegexpOne(target.domain, '^www[.]', '')
            = domainWithoutWWW(target.url))
    AND (target.domain, target.post_id) != (l.domain, l.post_id)
""".strip()


async def ensure_link_storage(client: ClickHouseClient) -> None:
    schema = pathlib.Path(__file__).parents[1] / "storage/links.sql"

    for statement in schema.read_text().split(";"):
        if statement.strip():
            await client.execute(statement)

    result = await client.fetch_json(
        "SELECT version FROM link_index_versions FINAL WHERE version = 2",
    )

    if result.rows:
        return

    cursor = ("", "")

    while True:
        result = await client.fetch_json(
            "SELECT domain, post_id, url, html, source, updated_at "
            "FROM posts FINAL WHERE (domain, post_id) > %(cursor)s "
            "ORDER BY domain, post_id LIMIT 500", {"cursor": cursor},
        )

        if not result.rows:
            break

        identities: list[dict[str, object]] = []
        links: list[dict[str, object]] = []

        for row in result.rows:
            observed = parse_datetime(row.get("updated_at"))

            if observed is None:
                continue

            own, outgoing = post_index_rows(LinkPost(
                str(row["domain"]), str(row["post_id"]), str(row["url"]),
                str(row["html"]), str(row["source"]), observed,
            ))
            identities.extend(own)
            links.extend(outgoing)

        await client.insert_rows("post_urls", identities)
        await client.insert_rows("post_links", links)
        last = result.rows[-1]
        cursor = (str(last["domain"]), str(last["post_id"]))

    await client.insert_rows("link_index_versions", [{
        "version": 2, "completed_at": dt.datetime.now(dt.UTC),
    }])


def _reference(row: dict[str, Any], prefix: str) -> dict[str, str]:
    return {
        "domain": str(row[f"{prefix}domain"]),
        "id": str(row[f"{prefix}id"]),
        "url": str(row[f"{prefix}url"]),
        "title": str(row[f"{prefix}title"]),
        "source": str(row[f"{prefix}source"]),
    }


async def add_post_links(
    client: ClickHouseClient, posts: list[dict[str, Any]], query: str = "",
) -> None:
    if not posts:
        return

    keys = [(str(post["domain"]), str(post["id"])) for post in posts]
    incoming = await client.fetch_json(
        "SELECT *, count() OVER (PARTITION BY target_domain, target_id) "
        "AS link_count FROM (SELECT DISTINCT u.domain AS target_domain, "
        "u.post_id AS target_id, origin.domain AS origin_domain, "
        "origin.post_id AS origin_id, origin.url AS origin_url, "
        "origin.title AS origin_title, origin.source AS origin_source, "
        "origin.created_at AS origin_created_at "
        "FROM post_urls AS u FINAL "
        "INNER JOIN post_links AS l FINAL ON l.target_url = u.url "
        "INNER JOIN (SELECT domain, post_id, url, title, source, "
        "created_at, updated_at, hex(SHA256(html)) AS content_hash "
        "FROM posts FINAL) AS origin "
        "ON origin.domain = l.domain AND origin.post_id = l.post_id "
        "AND origin.updated_at = l.observed_at "
        "AND origin.content_hash = l.content_hash "
        "INNER JOIN (SELECT domain, post_id, updated_at FROM posts FINAL) "
        "AS target ON target.domain = u.domain AND target.post_id = u.post_id "
        "AND u.observed_at <= target.updated_at "
        "WHERE (u.domain, u.post_id) IN %(link_post_keys)s "
        "AND (origin.domain, origin.post_id) != (u.domain, u.post_id)) "
        "ORDER BY origin_created_at DESC, origin_domain, origin_id "
        "LIMIT 50 BY target_domain, target_id", {"link_post_keys": keys},
    )
    by_key = {(str(post["domain"]), str(post["id"])): post for post in posts}

    for row in incoming.rows:
        post = by_key.get((str(row["target_domain"]), str(row["target_id"])))

        if post is not None:
            post.setdefault("links", []).append(_reference(row, "origin_"))
            post["links_count"] = int(row["link_count"])

    await _add_publisher_excerpts(client, posts, query)
    hn_keys = [key for key in keys if key[0] == "news.ycombinator.com"]

    if not hn_keys:
        return

    destinations = await client.fetch_json(
        "SELECT l.domain AS origin_domain, l.post_id AS origin_id, "
        "target.domain AS target_domain, target.post_id AS target_id, "
        "target.url AS target_url, target.title AS target_title, "
        "target.source AS target_source, target.html AS target_html "
        "FROM post_links AS l FINAL "
        "INNER JOIN (SELECT domain, post_id, updated_at, "
        "hex(SHA256(html)) AS content_hash FROM posts FINAL) "
        "AS origin ON origin.domain = l.domain "
        "AND origin.post_id = l.post_id "
        "AND origin.updated_at = l.observed_at "
        "AND origin.content_hash = l.content_hash "
        "INNER JOIN post_urls AS u FINAL ON u.url = l.target_url "
        "INNER JOIN (SELECT domain, post_id, url, title, source, html, "
        "updated_at FROM posts "
        "FINAL) AS target ON target.domain = u.domain "
        "AND target.post_id = u.post_id "
        "AND u.observed_at <= target.updated_at "
        "WHERE l.kind = 'primary' AND (l.domain, l.post_id) "
        "IN %(hn_link_keys)s AND (target.domain, target.post_id) "
        "!= (l.domain, l.post_id) "
        "AND (target.source IN ('x', 'reddit', 'youtube', 'hacker_news') "
        "OR replaceRegexpOne(target.domain, '^www[.]', '') "
        "= domainWithoutWWW(target.url)) "
        "ORDER BY (target.domain = domainWithoutWWW(target.url)) DESC, "
        "(target.source != 'linked_article') DESC, target.domain, "
        "target.post_id LIMIT 1 BY l.domain, l.post_id",
        {"hn_link_keys": hn_keys},
    )

    for row in destinations.rows:
        post = by_key.get((str(row["origin_domain"]), str(row["origin_id"])))

        if post is not None:
            reference = _reference(row, "target_")

            if row["target_source"] in {"rss", "linked_article"}:
                _publisher_excerpt(reference, row.get("target_html"), query)

            post["linked_post"] = reference


def _publisher_excerpt(
    reference: dict[str, str], raw_html: object, query: str,
) -> None:
    body = _sanitize_html(str(raw_html or ""))
    excerpt = search_excerpt(body, query) or _summarize_html(body)

    if excerpt:
        reference["excerpt"] = excerpt


def _publisher_destination(post: dict[str, Any]) -> str | None:
    """Borrow only for absent body text or a bare URL, never citations."""
    source = post.get("source")

    if source not in {"reddit", "x", "rss", "linked_article"}:
        return None

    text = html_text(str(post.get("html") or "")).strip()
    destination = normalized_url(text)

    if text and destination is None:
        return None

    if source == "x":
        content: dict[str, Any] = post.get("x") or {}

        if content.get("quote"):
            return None

        links = {url for link in content.get("links", [])
                 if (url := normalized_url(str(link.get("url") or "")))}

        if not destination and len(links) == 1:
            destination = next(iter(links))

    if source == "rss" and not text:
        url = str(post.get("url") or "")

        if not is_original_source(str(post["domain"]), url):
            destination = normalized_url(url)

    if destination == normalized_url(str(post.get("url") or "")):
        return destination if source == "rss" else None

    return destination


async def _add_publisher_excerpts(
    client: ClickHouseClient, posts: list[dict[str, Any]], query: str,
) -> None:
    candidates = [(post, destination) for post in posts
                  if (destination := _publisher_destination(post))]

    if not candidates:
        return

    result = await client.fetch_json(
        "SELECT u.url AS lookup_url, target.domain AS domain, "
        "target.post_id AS id, target.url AS url, target.title AS title, "
        "target.source AS source, target.html AS html "
        "FROM post_urls AS u FINAL "
        "INNER JOIN (SELECT domain, post_id, url, title, source, html, "
        "updated_at FROM posts FINAL "
        "WHERE source IN ('rss', 'linked_article')) "
        "AS target ON target.domain = u.domain AND target.post_id = u.post_id "
        "AND u.observed_at <= target.updated_at "
        "WHERE u.url IN %(publisher_excerpt_urls)s AND target.html != '' "
        "AND replaceRegexpOne(target.domain, '^www[.]', '') "
        "= domainWithoutWWW(target.url) "
        "ORDER BY (target.source != 'linked_article') DESC, "
        "target.domain, target.post_id LIMIT 1 BY u.url",
        {"publisher_excerpt_urls": sorted({url for _, url in candidates})},
    )
    references: dict[str, dict[str, str]] = {}

    for row in result.rows:
        reference = _reference(row, "")
        _publisher_excerpt(reference, row.get("html"), query)

        if reference.get("excerpt"):
            references[str(row["lookup_url"])] = reference

    for post, destination in candidates:
        reference = references.get(destination)

        if reference is None or (
            reference["domain"], reference["id"],
        ) == (post["domain"], post["id"]):
            continue

        post["linked_post"] = reference

        if post.get("source") == "x" and post.get("x"):
            _fill_x_preview(post["x"], destination, reference)


def _fill_x_preview(
    content: dict[str, Any], destination: str, reference: dict[str, str],
) -> None:
    links = content.setdefault("links", [])
    matches = [link for link in links
               if normalized_url(str(link.get("url") or "")) == destination]

    if not matches:
        links.append({
            "url": destination, "title": reference["title"],
            "description": reference["excerpt"], "image_url": None,
        })

    for link in matches:
        if not str(link.get("description") or "").strip():
            link["description"] = reference["excerpt"]
