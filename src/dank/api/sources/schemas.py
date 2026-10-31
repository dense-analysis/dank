"""Wire contract for /api/reader/sources.

Pydantic stays at this edge. OpenAPI, and later the reader's generated
TypeScript types, come from these models.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

from dank.api.schemas import ApiModel
from dank.api.sources.model import (
    Overview,
    RedditActivity,
    SourceSummary,
    XActivity,
)
from dank.web.reader_source_icons import source_icon_url


class XAccountOut(ApiModel):
    handle: str
    name: str
    icon_url: str | None
    total_posts: int | None
    count: int


class FlairOut(ApiModel):
    name: str
    count: int


class PostTotalOut(ApiModel):
    count: int
    source: str
    updated_at: str
    captured_at: str


class CommunityOut(ApiModel):
    name: str
    count: int
    icon_url: str | None
    flairs: list[FlairOut]
    post_total: PostTotalOut | None


class SourceBase(ApiModel):
    domain: str
    name: str
    icon_url: str | None
    configured: bool
    tags: list[str]
    count: int


class FeedSourceOut(SourceBase):
    kind: Literal["rss", "hacker_news", "youtube"]


class XSourceOut(SourceBase):
    kind: Literal["x"]
    accounts: list[XAccountOut]
    for_you_count: int


class RedditSourceOut(SourceBase):
    kind: Literal["reddit"]
    communities: list[CommunityOut]
    flairs: list[FlairOut]


# `kind` picks the variant, so OpenAPI and the generated TypeScript know
# that only X sources have accounts.
SourceOut = Annotated[
    FeedSourceOut | XSourceOut | RedditSourceOut,
    Field(discriminator="kind"),
]


class SourcesResponse(ApiModel):
    sources: list[SourceOut]
    total_posts: int
    blog_external_count: int

    @classmethod
    def from_overview(cls, overview: Overview) -> SourcesResponse:
        return cls(
            sources=[source_out(source) for source in overview.sources],
            total_posts=overview.total_posts,
            blog_external_count=overview.blog_external_count,
        )


def source_out(source: SourceSummary) -> SourceOut:
    """Flatten a summary into its kind's variant.

    Icon URLs point at an HTTP route, so they are added here rather than
    in the domain. Nested NamedTuples such as XAccount are read by
    attribute (from_attributes in ApiModel).
    """
    base: dict[str, object] = {
        "kind": source.kind,
        "domain": source.domain,
        "name": source.name,
        "icon_url": source_icon_url(source.domain),
        "configured": source.configured,
        "tags": source.tags,
        "count": source.count,
    }

    match source.activity:
        case XActivity(accounts=accounts, for_you_count=for_you_count):
            return XSourceOut.model_validate(
                base | {"accounts": accounts, "for_you_count": for_you_count},
            )
        case RedditActivity(communities=communities, flairs=flairs):
            return RedditSourceOut.model_validate(
                base | {"communities": communities, "flairs": flairs},
            )
        case None:
            return FeedSourceOut.model_validate(base)
