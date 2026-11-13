"""Wire contract for the connection routes.

As with posts, a field with a None default may be absent: builders validate
from dicts, and the route excludes what wasn't set.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import StringConstraints

from dank.api.connections.model import (
    ConnectionKey,
    ConnectionScope,
    connection_key,
    connection_scope,
)
from dank.api.schemas import ApiModel

BoundedId = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=2000),
]


class ConnectionsQuery(ApiModel):
    domain: BoundedId
    id: BoundedId
    source: str = ""
    community: str = ""

    def scope(self) -> ConnectionScope:
        return connection_scope(
            self.domain, self.id, self.source, self.community,
        )


class ConnectionQuery(ApiModel):
    domain: BoundedId
    id: BoundedId
    target: str
    kind: Literal["post", "comment"]
    reference_id: str = ""
    observed_at: str = ""

    def key(self) -> ConnectionKey:
        return connection_key(
            self.domain, self.id, self.target, self.kind, self.reference_id,
            self.observed_at,
        )


class ConnectionPostOut(ApiModel):
    domain: str
    id: str
    url: str
    title: str
    source: str


class ConnectionTargetOut(ApiModel):
    url: str
    post: ConnectionPostOut | None
    title: str
    youtube_item_id: str | None


class ConnectionEvidenceOut(ApiModel):
    domain: str
    post_id: str
    kind: Literal["post", "comment"]
    reference_id: str
    reference_url: str
    author: str
    matched_url: str
    excerpt: str
    observed_at: str


class ConnectionOut(ApiModel):
    id: str
    direction: Literal["incoming", "outgoing", "related"]
    origin: ConnectionPostOut
    target: ConnectionTargetOut
    evidence: ConnectionEvidenceOut


class ConnectionContextOut(ApiModel):
    source: (
        Literal["reddit", "hacker_news", "x", "youtube", "rss"] | None
    ) = None
    community: str | None = None


class ConnectionsResponse(ApiModel):
    root: ConnectionPostOut
    incoming: list[ConnectionOut]
    outgoing: list[ConnectionOut]
    related: list[ConnectionOut]
    context: ConnectionContextOut
    related_count: int
    incoming_count: int
    outgoing_count: int
    limit: int
    coverage: Literal["saved_evidence"]
