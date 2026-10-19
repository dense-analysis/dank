"""Collection history for an article and the post that points to it."""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from typing import Any
from urllib.parse import urlsplit

from dank.links import is_platform_url, normalized_url
from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.web.app import PostRow
from dank.youtube import payload_object

MAX_FIRST_SOURCES = 5


def _article_url(post: dict[str, Any]) -> str | None:
    source = post.get("source")
    value = (
        payload_object(post.get("reddit")).get("external_url")
        or payload_object(post.get("hacker_news")).get("external_url")
        or payload_object(post.get("linked_post")).get("url")
    )

    if not value and source in {"rss", "linked_article"}:
        value = post.get("url")

    url = normalized_url(str(value or ""))

    return url if url and not is_platform_url(url) else None


def _label(domain: str, source: str, url: str, *, unknown: bool) -> str:
    if unknown:
        return "Saved article (collector unknown)"

    if source == "reddit":
        path = urlsplit(url).path.split("/")

        if len(path) > 2 and path[1] == "r":
            return "r/" + path[2]

    return {
        "reddit": "Reddit", "hacker_news": "Hacker News",
        "x": "X", "youtube": "YouTube",
    }.get(source, domain.removeprefix("www."))


def _origin(
    domain: str, identifier: str, source: str, url: str,
    timestamp: object, *, kind: str = "reference", unknown: bool = False,
) -> dict[str, str] | None:
    observed = parse_datetime(timestamp)

    if observed is None:
        return None

    return {
        "domain": domain, "id": identifier, "source": source, "url": url,
        "label": _label(domain, source, url, unknown=unknown),
        "observed_at": observed.isoformat(), "kind": kind,
    }


def _capture_origin(
    row: dict[str, Any], prefix: str, *, allow_unknown: bool,
) -> dict[str, str] | None:
    origin = payload_object(row.get(prefix + "origin"))

    if origin.get("domain") and origin.get("post_id"):
        observed = parse_datetime(origin.get("observed_at"))
        captured = parse_datetime(row.get(prefix + "at"))

        if observed and captured and observed <= captured:
            return _origin(
                str(origin["domain"]), str(origin["post_id"]),
                str(origin.get("source") or ""), str(origin.get("url") or ""),
                observed,
            )

    source = str(row.get(prefix + "source") or "")

    if source == "linked_article" and not allow_unknown:
        return None

    return _origin(
        str(row["domain"]), str(row["post_id"]), source,
        str(row.get(prefix + "url") or ""), row.get(prefix + "at"),
        kind="article", unknown=source == "linked_article",
    )


def _earliest(origins: list[dict[str, str]]) -> list[dict[str, str]]:
    if not origins:
        return []

    first = min(dt.datetime.fromisoformat(item["observed_at"])
                for item in origins)
    unique = {
        (item["domain"], item["id"]): item for item in origins
        if dt.datetime.fromisoformat(item["observed_at"]) == first
    }

    return sorted(unique.values(), key=lambda item: (
        item["label"], item["domain"], item["id"],
    ))[:MAX_FIRST_SOURCES]


async def _articles(
    client: ClickHouseClient, urls: list[str],
) -> dict[str, dict[str, Any]]:
    result = await client.fetch_json(
        "SELECT u.url AS lookup_url, p.domain AS domain, p.post_id AS id, "
        "p.url AS url, p.title AS title, p.source AS source, "
        "notEmpty(trimBoth(p.html)) AS available, p.updated_at AS updated_at "
        "FROM post_urls AS u FINAL "
        "INNER JOIN (SELECT domain, post_id, url, title, source, html, "
        "updated_at FROM posts FINAL "
        "WHERE source IN ('rss','linked_article')) "
        "AS p ON p.domain=u.domain AND p.post_id=u.post_id "
        "WHERE u.url IN %(provenance_urls)s AND u.observed_at <= p.updated_at "
        "AND replaceRegexpOne(p.domain, '^www[.]', '')"
        "=domainWithoutWWW(p.url) "
        "ORDER BY notEmpty(trimBoth(p.html)) DESC, "
        "(p.source != 'linked_article') DESC, "
        "p.domain, p.post_id LIMIT 1 BY u.url", {"provenance_urls": urls},
    )

    return {str(row["lookup_url"]): row for row in result.rows}


async def _aliases(
    client: ClickHouseClient, articles: dict[str, dict[str, Any]],
) -> dict[tuple[str, str], set[str]]:
    keys = sorted({(str(row["domain"]), str(row["id"]))
                   for row in articles.values()})
    aliases: dict[tuple[str, str], set[str]] = defaultdict(set)

    if not keys:
        return aliases

    result = await client.fetch_json(
        "SELECT u.url AS url, u.domain AS domain, u.post_id AS post_id "
        "FROM post_urls AS u FINAL INNER JOIN (SELECT domain, post_id, "
        "updated_at FROM posts FINAL) AS p ON p.domain=u.domain "
        "AND p.post_id=u.post_id WHERE (u.domain,u.post_id) "
        "IN %(provenance_article_keys)s AND u.observed_at <= p.updated_at",
        {"provenance_article_keys": keys},
    )

    for row in result.rows:
        if url := normalized_url(str(row["url"])):
            aliases[str(row["domain"]), str(row["post_id"])].add(url)

    return aliases


async def _captures(
    client: ClickHouseClient, versions: dict[tuple[str, str], dt.datetime],
) -> dict[tuple[str, str], dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {"provenance_capture_keys": sorted(versions)}

    for index, (key, version) in enumerate(sorted(versions.items())):
        clauses.append(
            f"((domain,post_id) = %(capture_key_{index})s AND scraped_at <= "
            f"parseDateTime64BestEffort(%(capture_before_{index})s,3))",
        )
        params[f"capture_key_{index}"] = key
        params[f"capture_before_{index}"] = version.isoformat()

    if not clauses:
        return {}

    success = (
        "notEmpty(JSONExtractString(payload,'page_html')) AND "
        "JSONExtractString(payload,'page_fetch_status') "
        "NOT IN ('failed','unsupported')"
    )
    result = await client.fetch_json(
        "SELECT domain, post_id, min(scraped_at) AS first_at, "
        "argMin(url,scraped_at) AS first_url, "
        "argMin(source,scraped_at) AS first_source, "
        "argMaxIf(post_created_at,scraped_at,post_created_at IS NOT NULL) "
        "AS published_at, "
        "argMin(JSONExtractRaw(payload,'collection_origin'),scraped_at) "
        "AS first_origin, countIf(" + success + ") AS successful_captures, "
        "minIf(scraped_at," + success + ") AS collected_at, "
        "argMinIf(url,scraped_at," + success + ") AS collected_url, "
        "argMinIf(source,scraped_at," + success + ") AS collected_source, "
        "argMinIf(JSONExtractRaw(payload,'collection_origin'),scraped_at,"
        + success + ") AS collected_origin FROM raw_posts "
        "WHERE (domain,post_id) IN %(provenance_capture_keys)s AND ("
        + " OR ".join(clauses) + ") GROUP BY domain,post_id", params,
    )
    captures: dict[tuple[str, str], dict[str, Any]] = {}

    for row in result.rows:
        key = (str(row["domain"]), str(row["post_id"]))
        first = parse_datetime(row.get("first_at"))
        version = versions.get(key)

        if first is None or version is None or first > version:
            continue

        collected = parse_datetime(row.get("collected_at"))

        if collected and collected > version:
            row = {**row, "successful_captures": 0}

        captures[key] = row

    return captures


async def _observations(
    client: ClickHouseClient, urls: list[str],
    post_keys: list[tuple[str, str]],
) -> dict[str, list[dict[str, str]]]:
    selection = (
        "SELECT target_url,domain,post_id,url,source, "
        "min(observed_at) AS first_observed_at FROM post_reference_evidence "
        "FINAL WHERE target_url IN %(provenance_observation_urls)s "
        "AND kind='post' "
    )
    grouping = " GROUP BY target_url,domain,post_id,url,source "
    result = await client.fetch_json(
        "(" + selection + grouping
        + "ORDER BY first_observed_at,domain,post_id LIMIT 5 BY target_url) "
        "UNION ALL (" + selection
        + "AND (domain,post_id) IN %(provenance_post_keys)s " + grouping + ")",
        {"provenance_observation_urls": urls,
         "provenance_post_keys": post_keys},
    )
    observations: dict[str, list[dict[str, str]]] = defaultdict(list)

    for row in result.rows:
        origin = _origin(
            str(row["domain"]), str(row["post_id"]), str(row["source"]),
            str(row["url"]), row.get("first_observed_at"),
        )

        if origin:
            observations[str(row["target_url"])].append(origin)

    return observations



def _publication(
    post: dict[str, Any], own: dict[str, Any],
) -> dt.datetime | None:
    match post.get("source"):
        case "hacker_news":
            value = payload_object(post.get("hacker_news")).get("submitted_at")
        case "reddit" | "x":
            # Raw dates stay nullable instead of falling back to capture time.
            value = own.get("published_at")
        case _:
            value = post.get("published_at")

    published = parse_datetime(value)
    captured = parse_datetime(own.get("first_at"))

    if published and captured and published > captured:
        return None

    return published


def _independent_origin(
    origin: dict[str, str], post: dict[str, Any],
) -> bool:
    if (origin["domain"], origin["id"]) == (post["domain"], post["id"]):
        return False

    origin_url = normalized_url(origin["url"])

    if origin_url and origin_url == normalized_url(str(post.get("url") or "")):
        return False

    if (origin["source"] == post.get("source")
            and origin["source"] in {"reddit", "x", "hacker_news", "youtube"}
            and origin["id"] == post["id"]):
        return False

    # Unattributed legacy pages cannot name an independent discovering source.
    return bool(origin["domain"] and origin["id"]) and origin["source"] in {
        "rss", "reddit", "x", "hacker_news", "youtube",
    }


def _publication_discovery(
    post: dict[str, Any], own: dict[str, Any],
    origins: list[dict[str, str]],
) -> dict[str, Any]:
    published = _publication(post, own)
    earlier = _earliest([
        origin for origin in origins
        if published and dt.datetime.fromisoformat(origin["observed_at"])
        < published and _independent_origin(origin, post)
    ])

    return {
        "post_published_at": published.isoformat() if published else None,
        "discovered_before_publication": bool(earlier) if published else None,
        "earlier_discovered_at": (
            earlier[0]["observed_at"] if earlier else None
        ),
        "earlier_discovered_from": earlier,
    }


def _info(
    post: dict[str, Any], url: str, article: dict[str, Any] | None,
    origins: list[dict[str, str]],
    captures: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    own = captures.get((str(post["domain"]), str(post["id"])), {})
    own_first = parse_datetime(own.get("first_at"))
    captured: dict[str, Any] = {}

    if article:
        captured = captures.get(
            (str(article["domain"]), str(article["id"])), {},
        )

        if captured and (origin := _capture_origin(
            captured, "first_", allow_unknown=True,
        )):
            origins.append(origin)

    own_links = [parse_datetime(origin["observed_at"]) for origin in origins
                 if (origin["domain"], origin["id"])
                 == (post["domain"], post["id"])]
    link_first = min((stamp for stamp in own_links if stamp), default=None)

    if post.get("source") in {"rss", "linked_article"}:
        link_first = link_first or own_first

    first = _earliest(origins)
    first_at = first[0]["observed_at"] if first else None
    collected = (
        parse_datetime(captured.get("collected_at"))
        if captured.get("successful_captures") else None
    )
    status = "unavailable"

    if article and article.get("available"):
        status = "collected" if collected else "incomplete"

    return {
        "article_url": url,
        "article": {key: str(article[key]) for key in (
            "domain", "id", "url", "title", "source",
        )} if article else None,
        "post_first_seen_at": own_first.isoformat() if own_first else None,
        "link_first_seen_at": link_first.isoformat() if link_first else None,
        "first_seen_at": first_at, "first_seen_from": first,
        "article_collected_at": collected.isoformat() if collected else None,
        "collected_via": _capture_origin(
            captured, "collected_", allow_unknown=False,
        ) if collected else None,
        "seen_before_post": (
            dt.datetime.fromisoformat(first_at) < link_first
            if first_at and link_first else None
        ),
        "article_status": status, "coverage": "saved_history",
        **_publication_discovery(post, own, origins),
    }


async def add_collection_info(
    client: ClickHouseClient, posts: list[dict[str, Any]], rows: list[PostRow],
) -> None:
    candidates = [(post, url) for post in posts if (url := _article_url(post))]

    if not candidates:
        return

    urls = sorted({url for _, url in candidates})
    articles = await _articles(client, urls)
    aliases = await _aliases(client, articles)
    candidate_keys = {(str(post["domain"]), str(post["id"]))
                      for post, _ in candidates}
    versions = {
        (row.domain, row.post_id): row.updated_at.replace(
            tzinfo=row.updated_at.tzinfo or dt.UTC,
        ) for row in rows if (row.domain, row.post_id) in candidate_keys
    }

    for article in articles.values():
        key = (str(article["domain"]), str(article["id"]))
        version = parse_datetime(article.get("updated_at"))

        if version:
            versions[key] = min(version, versions.get(key, version))

    captures = await _captures(client, versions)
    observed = await _observations(client, sorted(
        set(urls) | {url for values in aliases.values() for url in values},
    ), [(str(post["domain"]), str(post["id"])) for post, _ in candidates])

    for post, url in candidates:
        article = articles.get(url)
        related = {url}

        if article:
            related |= aliases.get(
                (str(article["domain"]), str(article["id"])), set(),
            )

        origins = [origin for alias in related
                   for origin in observed.get(alias, [])]
        info = _info(post, url, article, origins, captures)
        version = versions.get((str(post["domain"]), str(post["id"])))
        own_first = parse_datetime(info["post_first_seen_at"])

        if version and own_first and own_first > version.replace(
            tzinfo=version.tzinfo or dt.UTC,
        ):
            info.update(post_first_seen_at=None, seen_before_post=None)

        if info["first_seen_at"] or info["post_first_seen_at"] or article:
            post["collection_info"] = info
