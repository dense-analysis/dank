"""Wire contract for /api/reader/refresh.

Scope fields are absent rather than null, so declare them with a None default
and answer with response_model_exclude_unset. See Projections in docs/api.md.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import Field

from dank.api.schemas import ApiModel


class FilteredScope(ApiModel):
    """Every scope but a single post can narrow to domains and tags."""

    domains: list[str] | None = None
    tags: list[str] | None = None


class AllScope(FilteredScope):
    kind: Literal["all"]
    platforms: (
        list[Literal["rss", "x", "reddit", "hacker_news", "youtube"]] | None
    ) = None


class RssScope(FilteredScope):
    kind: Literal["rss"]
    blog_feed: Literal["sources", "external"] | None = None


class RedditScope(FilteredScope):
    kind: Literal["reddit"]
    community: str | None = None


class XScope(FilteredScope):
    kind: Literal["x"]
    account: str | None = None
    feed_id: str | None = None


class HackerNewsScope(FilteredScope):
    kind: Literal["hacker_news"]


class YouTubeScope(FilteredScope):
    kind: Literal["youtube"]
    channel_id: str | None = None
    feed_id: str | None = None
    playlist_id: str | None = None


class PostScope(ApiModel):
    kind: Literal["post"]
    domain: str
    post_id: str


# `kind` picks the variant, so OpenAPI and the generated TypeScript know which
# details each scope can have. Rules across fields stay in the domain.
RefreshScope = Annotated[
    AllScope
    | RssScope
    | RedditScope
    | XScope
    | HackerNewsScope
    | YouTubeScope
    | PostScope,
    Field(discriminator="kind"),
]


class RefreshRequest(ApiModel):
    scope: RefreshScope

    def scope_value(self) -> dict[str, Any]:
        """The scope as sent, for the domain's rules to read."""
        return self.scope.model_dump(exclude_unset=True)


class RefreshJobOut(ApiModel):
    id: str
    scope: RefreshScope
    label: str
    status: Literal[
        "queued",
        "running",
        "processing",
        "completed",
        "partial",
        "failed",
        "interrupted",
    ]
    message: str
    created_at: str
    finished_at: str | None
    posts_saved: int
    run_id: str | None


class RefreshJobsResponse(ApiModel):
    jobs: list[RefreshJobOut]


class RefreshJobResponse(ApiModel):
    job: RefreshJobOut
