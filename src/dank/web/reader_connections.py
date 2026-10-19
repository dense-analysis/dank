"""One-hop source connections with archived, navigable link evidence."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from aiohttp import web
from multidict import MultiMapping

from dank.links import normalized_url
from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.youtube import canonical_video_id

MAX_CONNECTIONS = 100
POST_FIELDS = "domain, post_id AS id, url, title, source"
CONNECTION_SOURCES = {"reddit", "hacker_news", "x", "youtube", "rss"}
SOURCE_ALIASES = {
    "twitter": "x", "twitter.com": "x", "x.com": "x", "reddit.com": "reddit",
    "news.ycombinator.com": "hacker_news", "hckrnews.com": "hacker_news",
    "youtube.com": "youtube",
}


def _context(query: MultiMapping[str]) -> dict[str, str]:
    source = query.get("source", "").strip().lower().removeprefix("www.")
    source = SOURCE_ALIASES.get(source, source)
    community = query.get("community", "").strip().lower()

    if source and source not in CONNECTION_SOURCES:
        raise ValueError("Invalid connection source")

    if community and (source != "reddit"
                      or not re.fullmatch(r"[a-z0-9_]{2,21}", community)):
        raise ValueError(
            "community requires source=reddit and a community name",
        )

    return {key: value for key, value in (("source", source),
                                         ("community", community)) if value}


def _parameters(query: MultiMapping[str], *, single: bool) -> dict[str, Any]:
    allowed = {"domain", "id"}

    if single:
        allowed |= {"target", "kind", "reference_id", "observed_at"}
    else:
        allowed |= {"source", "community"}

    if set(query) - allowed or any(len(query.getall(k)) != 1 for k in query):
        raise ValueError("Unknown or repeated connection parameter")

    params: dict[str, Any] = {"domain": query.get("domain", "").strip(),
                              "id": query.get("id", "").strip()}

    if any(not params[key] or len(params[key]) > 2000 for key in params):
        raise ValueError(
            "domain and id are required and limited to 2000 characters",
        )

    if not single:
        params.update(_context(query))

        return params

    target = query.get("target", "")
    canonical = normalized_url(target) if len(target) <= 8192 else None
    kind = query.get("kind", "")
    reference_id = query.get("reference_id", "")

    if (canonical is None or kind not in {"post", "comment"}
            or len(reference_id) > 2000
            or (kind == "comment" and not reference_id)
            or (kind == "post" and reference_id)):
        raise ValueError("A valid target, kind and reference_id are required")

    params.update(target=canonical, kind=kind, reference_id=reference_id)
    observed = query.get("observed_at", "")

    if observed:
        stamp = parse_datetime(observed)

        if stamp is None:
            raise ValueError("Invalid observed_at timestamp")

        params["observed_at"] = stamp.isoformat()

    return params


def connection_query(*, incoming: bool = False, single: bool = False,
                     observed: bool = False, related: bool = False,
                     source: bool = False, community: bool = False) -> str:
    conditions = ["e.observed_at <= origin.updated_at"]
    target_join = ""
    target_columns = ""

    if related:
        # One bounded expansion from collected publisher articles. Their saved
        # aliases establish shared identity; never infer a match from titles.
        target_join = (
            "INNER JOIN (SELECT u.url AS related_url, "
            "p.domain AS related_domain, "
            "p.post_id AS related_id, p.url AS related_post_url, "
            "p.title AS related_title, p.source AS related_source "
            "FROM post_urls AS u FINAL INNER JOIN (SELECT domain, post_id, "
            "url, title, source, updated_at FROM posts FINAL) AS p "
            "ON p.domain = u.domain AND p.post_id = u.post_id "
            "WHERE (p.domain, p.post_id) IN %(related_targets)s "
            "AND u.observed_at <= p.updated_at ORDER BY p.domain, p.post_id "
            "LIMIT 1 BY u.url) AS linked ON linked.related_url = e.target_url "
        )
        target_columns = ", linked.* "
        conditions += [
            "(e.domain, e.post_id) != (%(domain)s, %(id)s)",
            "(e.domain, e.post_id) NOT IN %(related_targets)s",
        ]
    elif incoming:
        conditions += [
            "e.target_url IN (SELECT u.url FROM post_urls AS u FINAL "
            "INNER JOIN (SELECT domain, post_id, updated_at FROM posts FINAL) "
            "AS p ON p.domain = u.domain AND p.post_id = u.post_id "
            "WHERE u.domain = %(domain)s AND u.post_id = %(id)s "
            "AND u.observed_at <= p.updated_at)",
            "(e.domain, e.post_id) != (%(domain)s, %(id)s)",
        ]
    else:
        conditions += [
            "e.domain = %(domain)s", "e.post_id = %(id)s",
            "NOT ((origin.source != 'rss' OR "
            "replaceRegexpOne(origin.domain, '^www[.]', '') "
            "= domainWithoutWWW(origin.url)) AND e.target_url IN ("
            "SELECT url FROM post_urls FINAL WHERE domain = %(domain)s "
            "AND post_id = %(id)s AND observed_at <= (SELECT updated_at "
            "FROM posts FINAL WHERE domain = %(domain)s "
            "AND post_id = %(id)s LIMIT 1)))",
        ]

    if source:
        conditions.append(
            "(origin.source = %(source)s OR "
            "(%(source)s = 'rss' AND origin.source = 'linked_article') OR "
            "(%(source)s = 'x' AND (origin.source = 'twitter' OR "
            "replaceRegexpOne(lowerUTF8(origin.domain), '^www[.]', '') "
            "IN ('x.com', 'twitter.com'))))",
        )

    if community:
        conditions.append(
            "lowerUTF8(extract(origin.url, '/r/([^/]+)/comments/')) "
            "= %(community)s",
        )

    if single:
        conditions += ["e.target_url = %(target)s", "e.kind = %(kind)s",
                       "e.reference_id = %(reference_id)s"]

    if observed:
        conditions.append(
            "e.observed_at = parseDateTime64BestEffort(%(observed_at)s, 3)",
        )

    return (
        "SELECT *, count() OVER () AS connection_count FROM ("
        "SELECT e.domain AS domain, e.post_id AS post_id, "
        "e.target_url AS target_url, e.kind AS kind, "
        "e.reference_id AS reference_id, e.reference_url AS reference_url, "
        "e.author AS author, e.matched_url AS matched_url, "
        "e.excerpt AS excerpt, e.observed_at AS observed_at, "
        "origin.url AS origin_url, origin.title AS origin_title, "
        "origin.source AS origin_source " + target_columns +
        "FROM post_reference_evidence AS e FINAL "
        "INNER JOIN (SELECT domain, post_id, url, title, source, updated_at "
        "FROM posts FINAL) AS origin ON origin.domain = e.domain "
        "AND origin.post_id = e.post_id " + target_join + "WHERE "
        + " AND ".join(conditions)
        + " ORDER BY e.observed_at DESC, e.excerpt DESC "
        "LIMIT 1 BY e.domain, e.post_id, e.target_url, e.kind, "
        "e.reference_id) "
        "ORDER BY observed_at DESC, domain, post_id, target_url, kind, "
        "reference_id LIMIT %(limit)s"
    )


async def _post(client: ClickHouseClient,
                params: dict[str, Any]) -> dict[str, Any] | None:
    result = await client.fetch_json(
        f"SELECT {POST_FIELDS} FROM posts FINAL "
        "WHERE domain = %(domain)s AND post_id = %(id)s LIMIT 1", params,
    )

    return result.rows[0] if result.rows else None


async def _targets(client: ClickHouseClient,
                   urls: list[str],
                   origin: tuple[str, str]) -> dict[str, dict[str, str]]:
    if not urls:
        return {}

    result = await client.fetch_json(
        "SELECT u.url AS matched_target, p.domain AS domain, "
        "p.post_id AS id, p.url AS url, p.title AS title, p.source AS source "
        "FROM post_urls AS u FINAL INNER JOIN (SELECT domain, post_id, "
        "url, title, source, html, updated_at FROM posts FINAL) AS p "
        "ON p.domain = u.domain AND p.post_id = u.post_id "
        "WHERE u.url IN %(urls)s AND u.observed_at <= p.updated_at "
        "AND (p.domain, p.post_id) != %(origin)s "
        "AND (p.source NOT IN ('rss', 'linked_article') OR notEmpty(p.html)) "
        "ORDER BY (replaceRegexpOne(p.domain, '^www[.]', '') "
        "= domainWithoutWWW(p.url)) DESC, "
        "(p.source != 'linked_article') DESC, p.domain, p.post_id "
        "LIMIT 1 BY u.url", {"urls": urls, "origin": origin},
    )

    return {str(row["matched_target"]): {
        key: str(row[key])
        for key in ("domain", "id", "url", "title", "source")
    } for row in result.rows}


async def _videos(
    client: ClickHouseClient, urls: list[str],
) -> dict[str, str]:
    ids = sorted({video for url in urls if (video := canonical_video_id(url))})

    if not ids:
        return {}

    result = await client.fetch_json(
        "SELECT video_id, argMax(title, (notEmpty(title), observed_at)) "
        "AS title FROM ("
        "SELECT video_id, '' AS title, observed_at "
        "FROM youtube_reference_evidence WHERE video_id IN %(ids)s UNION ALL "
        "SELECT video_id, JSONExtractString(payload, 'title') AS title, "
        "observed_at FROM youtube_video_metadata "
        "WHERE video_id IN %(ids)s UNION ALL "
        "SELECT substring(item_id, 7) AS video_id, "
        "JSONExtractString(payload, 'title') AS title, observed_at "
        "FROM youtube_discoveries WHERE item_id IN %(items)s) "
        "GROUP BY video_id",
        {"ids": ids, "items": ["video:" + video for video in ids]},
    )

    return {str(row["video_id"]): str(row["title"]) for row in result.rows}


def _connection(row: dict[str, Any], *, incoming: bool,
                target: dict[str, Any] | None,
                videos: dict[str, str]) -> dict[str, Any]:
    observed = parse_datetime(row["observed_at"])
    identity = tuple(str(row[key]) for key in (
        "domain", "post_id", "target_url", "kind", "reference_id",
    ))
    video = canonical_video_id(str(row["target_url"]))

    return {
        "id": hashlib.sha256(json.dumps(identity).encode()).hexdigest(),
        "direction": "incoming" if incoming else "outgoing",
        "origin": {"domain": str(row["domain"]), "id": str(row["post_id"]),
                   "url": str(row["origin_url"]),
                   "title": str(row["origin_title"]),
                   "source": str(row["origin_source"])},
        "target": {"url": str(row["target_url"]), "post": target,
                   "title": (target["title"] if target else "")
                   or videos.get(video or "", "") or str(row["target_url"]),
                   "youtube_item_id": (
                       "video:" + video if video and video in videos else None
                   )},
        "evidence": {
            **{key: str(row[key]) for key in (
                "domain", "post_id", "kind", "reference_id", "reference_url",
                "author", "matched_url", "excerpt",
            )},
            "observed_at": observed.isoformat() if observed else "",
        },
    }


async def connections(client: ClickHouseClient,
                      params: dict[str, Any]) -> dict[str, Any] | None:
    root = await _post(client, params)

    if root is None:
        return None

    params = {**params, "limit": MAX_CONNECTIONS}
    scope = {"source": bool(params.get("source")),
             "community": bool(params.get("community"))}
    incoming = await client.fetch_json(
        connection_query(incoming=True, **scope), params,
    )
    outgoing = await client.fetch_json(connection_query(), params)
    urls = sorted({str(row["target_url"]) for row in outgoing.rows})
    targets = await _targets(
        client, urls, (params["domain"], params["id"]),
    )
    videos = await _videos(client, urls)
    related_targets = sorted({
        (target["domain"], target["id"])
        for row in outgoing.rows
        if row["kind"] == "post"
        and (target := targets.get(str(row["target_url"])))
        and target["source"] in {"rss", "linked_article"}
    })
    related_rows: list[dict[str, Any]] = []

    if related_targets:
        related_result = await client.fetch_json(
            connection_query(related=True, **scope),
            {**params, "related_targets": related_targets},
        )
        related_rows = related_result.rows

    related: list[dict[str, Any]] = []

    for row in related_rows:
        target = {key: str(row["related_" + key])
                  for key in ("domain", "id", "title", "source")}
        target["url"] = str(row["related_post_url"])
        connection = _connection(row, incoming=False, target=target, videos={})
        connection["direction"] = "related"
        related.append(connection)

    return {
        "root": root,
        "incoming": [_connection(row, incoming=True, target=root, videos={})
                     for row in incoming.rows],
        "outgoing": [_connection(
            row, incoming=False, target=targets.get(str(row["target_url"])),
            videos=videos,
        ) for row in outgoing.rows],
        "context": {key: params[key] for key in ("source", "community")
                    if key in params},
        "related": related,
        "related_count": (int(related_rows[0]["connection_count"])
                          if related_rows else 0),
        "incoming_count": (
            int(incoming.rows[0]["connection_count"]) if incoming.rows else 0
        ),
        "outgoing_count": (
            int(outgoing.rows[0]["connection_count"]) if outgoing.rows else 0
        ),
        "limit": MAX_CONNECTIONS, "coverage": "saved_evidence",
    }


async def handle_connections(request: web.Request) -> web.Response:
    try:
        params = _parameters(request.query, single=False)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    data = await connections(request.app["clickhouse"], params)

    if data is None:
        return web.json_response({"error": "Article not found"}, status=404)

    return web.json_response(data)


async def handle_connection(request: web.Request) -> web.Response:
    try:
        params = _parameters(request.query, single=True)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    client: ClickHouseClient = request.app["clickhouse"]
    result = await client.fetch_json(
        connection_query(single=True, observed="observed_at" in params),
        {**params, "limit": 1},
    )

    if not result.rows:
        return web.json_response(
            {"error": "Saved evidence not found"}, status=404,
        )

    targets = await _targets(
        client, [params["target"]], (params["domain"], params["id"]),
    )
    videos = await _videos(client, [params["target"]])

    return web.json_response(_connection(
        result.rows[0], incoming=False, target=targets.get(params["target"]),
        videos=videos,
    ))
