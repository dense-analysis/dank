"""What a connection request may ask for. Rule violations are InvalidInput."""

from __future__ import annotations

import re
from typing import Any, NamedTuple

from dank.errors import InvalidInput
from dank.links import normalized_url
from dank.model import parse_datetime

CONNECTION_SOURCES = {"reddit", "hacker_news", "x", "youtube", "rss"}
SOURCE_ALIASES = {
    "twitter": "x", "twitter.com": "x", "x.com": "x", "reddit.com": "reddit",
    "news.ycombinator.com": "hacker_news", "hckrnews.com": "hacker_news",
    "youtube.com": "youtube",
}

# JSON-shaped projections from dank.web.reader_connections, as with posts.
type ConnectionPayload = dict[str, Any]
type NeighbourhoodPayload = dict[str, Any]


class ConnectionScope(NamedTuple):
    """A post's neighbourhood, optionally narrowed to where it was opened."""

    domain: str
    id: str
    source: str = ""
    community: str = ""


class ConnectionKey(NamedTuple):
    """One piece of saved evidence."""

    domain: str
    id: str
    target: str
    kind: str
    reference_id: str
    observed_at: str = ""


def connection_scope(
    domain: str, post_id: str, source: str = "", community: str = "",
) -> ConnectionScope:
    source = source.strip().lower().removeprefix("www.")
    source = SOURCE_ALIASES.get(source, source)
    community = community.strip().lower()

    if source and source not in CONNECTION_SOURCES:
        raise InvalidInput("Invalid connection source")

    if community and (source != "reddit"
                      or not re.fullmatch(r"[a-z0-9_]{2,21}", community)):
        raise InvalidInput(
            "community requires source=reddit and a community name",
        )

    return ConnectionScope(domain, post_id, source, community)


def connection_key(
    domain: str,
    post_id: str,
    target: str,
    kind: str,
    reference_id: str = "",
    observed_at: str = "",
) -> ConnectionKey:
    canonical = normalized_url(target) if len(target) <= 8192 else None

    if (canonical is None or kind not in {"post", "comment"}
            or len(reference_id) > 2000
            or (kind == "comment" and not reference_id)
            or (kind == "post" and reference_id)):
        raise InvalidInput(
            "A valid target, kind and reference_id are required",
        )

    if not observed_at:
        return ConnectionKey(domain, post_id, canonical, kind, reference_id)

    stamp = parse_datetime(observed_at)

    if stamp is None:
        raise InvalidInput("Invalid observed_at timestamp")

    return ConnectionKey(
        domain, post_id, canonical, kind, reference_id, stamp.isoformat(),
    )
