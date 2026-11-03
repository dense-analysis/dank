"""Wire contract for the post browsing routes.

Query models check a request's shape; filter rules that involve more than
one field stay in the domain, with the wording the reader shows.

Response models describe the projection in dank.web.reader_posts. A field
with a None default may be absent: builders validate from dicts, so an absent
key stays unset, and routes use response_model_exclude_unset. A field without
a default is always present, even when it's null.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BeforeValidator, ConfigDict, Field, StringConstraints

from dank.api.posts.model import (
    AUTHOR_PAGE_LIMIT,
    AuthorGroup,
    AuthorsPage,
    FilterInput,
    PostPayload,
    PostsPage,
    ReaderFilters,
    parse_limit,
    reader_filters,
)
from dank.api.schemas import ApiModel

HackerNewsRanking = Literal["top10", "top20", "top50", "all"]
ConversationStatus = Literal[
    "captured", "partial", "failed", "disabled", "not_collected",
]

# Pydantic would take limit=5.0 as an integer, which has never been accepted.
WholeNumberText = BeforeValidator(parse_limit)
# Dates stay text so that only exact YYYY-MM-DD passes the domain's rule.
DateText = Annotated[str, Field(json_schema_extra={"format": "date"})]


class FilterQuery(ApiModel):
    """The feed filters. Repeat domain, tag, platform and flair to widen."""

    q: str = ""
    mode: Literal["words", "meaning", "combined"] = "words"
    sort: Literal["newest", "oldest", "relevance"] = "newest"
    domain: tuple[str, ...] = ()
    tag: tuple[str, ...] = ()
    platform: tuple[str, ...] = ()
    flair: tuple[str, ...] = ()
    author: str = ""
    author_match: Literal["contains", "exact"] = "contains"
    after: DateText = ""
    before: DateText = ""
    cursor: str = ""
    kind: Literal[
        "", "all", "stories", "reddit", "x", "youtube", "hacker_news",
    ] = ""
    community: str = ""
    account: str = ""
    x_feed: Literal["", "for_you"] = ""
    # Sending these empty is an error, so absent has to be its own value.
    hn_filter: HackerNewsRanking | None = None
    x_view: Literal["posts", "replies"] | None = None
    blog_feed: Literal["sources", "external"] | None = None
    limit: int

    def _input(self) -> FilterInput:
        return FilterInput(
            q=self.q,
            mode=self.mode,
            sort=self.sort,
            domains=self.domain,
            tags=self.tag,
            platforms=self.platform,
            flairs=self.flair,
            author=self.author,
            author_match=self.author_match,
            after=self.after,
            before=self.before,
            limit=self.limit,
            cursor=self.cursor,
            kind=self.kind,
            community=self.community,
            account=self.account,
            x_feed=self.x_feed,
            hn_filter=self.hn_filter,
            x_view=self.x_view,
            blog_feed=self.blog_feed,
        )


class PostsQuery(FilterQuery):
    limit: Annotated[int, Field(ge=1, le=100), WholeNumberText] = 30

    def filters(self) -> ReaderFilters:
        return reader_filters(self._input())


class XAuthorsQuery(FilterQuery):
    limit: Annotated[
        int, Field(ge=1, le=AUTHOR_PAGE_LIMIT), WholeNumberText,
    ] = 20

    def filters(self) -> ReaderFilters:
        # This cursor walks authors, so it isn't a filter's post cursor.
        return reader_filters(self._input()._replace(cursor=""))


Required = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1),
]


class PostQuery(ApiModel):
    # Unlike the feed, this route has always ignored other keys.
    model_config = ConfigDict(extra="ignore")

    domain: Required
    id: Required


class MediaOut(ApiModel):
    url: str
    content_type: str


class PostReferenceOut(ApiModel):
    domain: str
    id: str
    url: str
    title: str
    source: str


class LinkedPostOut(PostReferenceOut):
    excerpt: str | None = None


class CollectionOriginOut(ApiModel):
    domain: str
    id: str
    source: str
    url: str
    label: str
    observed_at: str
    kind: Literal["reference", "article"]


class CollectionInfoOut(ApiModel):
    article_url: str
    article: PostReferenceOut | None
    post_first_seen_at: str | None
    link_first_seen_at: str | None
    first_seen_at: str | None
    first_seen_from: list[CollectionOriginOut]
    article_collected_at: str | None
    collected_via: CollectionOriginOut | None
    seen_before_post: bool | None
    article_status: Literal["collected", "incomplete", "unavailable"]
    coverage: Literal["saved_history"]
    post_published_at: str | None
    discovered_before_publication: bool | None
    earlier_discovered_at: str | None
    earlier_discovered_from: list[CollectionOriginOut]


class RedditVideoOut(ApiModel):
    url: str
    source_url: str
    poster_url: str | None
    remote: bool
    hls_url: str | None
    fallback_url: str | None


class RedditImageOut(ApiModel):
    url: str
    source_url: str


class RedditCommentOut(ApiModel):
    id: str
    parent_id: str
    author: str
    body: str
    body_html: str
    score: int | None
    # Reddit sends whole and fractional seconds; both pass through as sent.
    created_utc: int | float | None
    permalink: str
    is_op: bool
    depth: int


class RedditCommentCaptureOut(ApiModel):
    status: Literal[
        "captured", "disabled", "failed", "deferred", "not_captured",
    ]
    sort: str
    limit: int
    total_available: int | None
    has_more: bool
    comments: list[RedditCommentOut]
    captured_at: str | None = None
    latest_attempt_status: Literal["disabled", "failed", "deferred"] | None = (
        None
    )


class RedditOut(ApiModel):
    community: str
    score: int | None
    comments: int | None
    flair: str | None
    icon_url: str | None
    video: RedditVideoOut | None
    videos: list[RedditVideoOut]
    external_url: str | None = None
    images: list[RedditImageOut] | None = None


class RedditDetailOut(RedditOut):
    comment_capture: RedditCommentCaptureOut


class HackerNewsOut(ApiModel):
    points: int | None
    comments: int | None
    discussion_url: str
    external_url: str | None
    day: str
    rank: int | None
    submitted_at: str | None


class HackerNewsCommentCaptureOut(ApiModel):
    status: Literal["captured", "partial", "failed", "disabled"]
    captured: int
    total: int | None
    reason: str | None


class HackerNewsCommentOut(ApiModel):
    id: str
    parent_id: str
    author: str
    html: str
    created_at: str | None
    depth: int


class HackerNewsDetailOut(HackerNewsOut):
    comment_capture: HackerNewsCommentCaptureOut
    comments_items: list[HackerNewsCommentOut]


class YouTubeChannelOut(ApiModel):
    id: str
    name: str
    url: str


class YouTubeOut(ApiModel):
    kind: str
    video_id: str | None
    channel: YouTubeChannelOut
    duration_text: str
    views_text: str
    thumbnail_url: str | None
    video_url: str | None


class XLinkOut(ApiModel):
    url: str
    title: str
    description: str
    image_url: str | None


class XMediaOut(ApiModel):
    type: Literal["image", "video", "gif"]
    url: str
    poster_url: str | None
    alt: str
    remote: bool
    aspect_ratio: int | float | None = None
    source_url: str | None = None


class XReplyToOut(ApiModel):
    id: str
    domain: str
    url: str
    is_self: bool | None
    collected: bool


class XQuoteOut(ApiModel):
    id: str
    domain: str
    url: str
    collected: bool
    post: CapturedPostOut | None = None


class CapturedXOut(ApiModel):
    """X fields of a post known only from another post's archive."""

    handle: str
    name: str
    icon_url: str | None
    links: list[XLinkOut]
    media: list[XMediaOut]
    reply_count: int | None
    quote: XQuoteOut | None = None


class CapturedPostOut(ApiModel):
    id: str
    domain: str
    url: str
    author: str
    title: str
    html: str
    excerpt: str
    source: str
    created_at: str
    thumbnail: str | None
    media: list[MediaOut]
    x: CapturedXOut


class XConversationNodeOut(CapturedPostOut):
    parent_id: str | None
    depth: int
    missing_parent: bool
    is_author: bool
    collected: bool | None = None


class XConversationOut(ApiModel):
    status: ConversationStatus
    captured_at: str | None
    has_more: bool
    reason: str | None
    reported_replies: int | None
    limit: int
    captured_count: int
    context: list[XConversationNodeOut]
    replies: list[XConversationNodeOut]
    latest_attempt_status: ConversationStatus | None = None
    latest_attempt_at: str | None = None
    local_posts_count: int | None = None
    local_posts_limited: bool | None = None


class XOut(ApiModel):
    handle: str
    name: str
    icon_url: str | None
    links: list[XLinkOut]
    replies_status: ConversationStatus
    replies_captured_count: int
    reply_count: int | None
    repost_count: int | None
    like_count: int | None
    view_count: int | None
    captured_at: str | None
    media: list[XMediaOut] | None = None
    conversation_id: str | None = None
    reply_to: XReplyToOut | None = None
    quote: XQuoteOut | None = None


class XDetailOut(XOut):
    conversation: XConversationOut


class PostBase(ApiModel):
    id: str
    domain: str
    url: str
    author: str
    title: str
    excerpt: str
    html: str
    collection_incomplete: bool
    created_at: str
    source: str
    thumbnail: str | None
    media: list[MediaOut]
    published_at: str | None = None
    links: list[PostReferenceOut] | None = None
    links_count: int | None = None
    linked_post: LinkedPostOut | None = None
    collection_info: CollectionInfoOut | None = None
    youtube: YouTubeOut | None = None


class PostOut(PostBase):
    x: XOut | None = None
    reddit: RedditOut | None = None
    hacker_news: HackerNewsOut | None = None


class PostDetailOut(PostBase):
    x: XDetailOut | None = None
    reddit: RedditDetailOut | None = None
    hacker_news: HackerNewsDetailOut | None = None


class PostsResponse(ApiModel):
    posts: list[PostOut]
    next_cursor: str | None
    limited: bool

    @classmethod
    def from_page(cls, page: PostsPage) -> PostsResponse:
        return cls(
            posts=_posts(page.posts),
            next_cursor=page.next_cursor,
            limited=page.limited,
        )


class XAuthorOut(ApiModel):
    handle: str
    name: str
    icon_url: str | None
    count: int
    posts: list[PostOut]

    @classmethod
    def from_group(cls, group: AuthorGroup) -> XAuthorOut:
        return cls(
            handle=group.handle,
            name=group.name,
            icon_url=group.icon_url,
            count=group.count,
            posts=_posts(group.posts),
        )


class XAuthorsResponse(ApiModel):
    authors: list[XAuthorOut]
    next_cursor: str | None
    total: int
    unattributed_posts: int

    @classmethod
    def from_page(cls, page: AuthorsPage) -> XAuthorsResponse:
        return cls(
            authors=[XAuthorOut.from_group(group) for group in page.authors],
            next_cursor=page.next_cursor,
            total=page.total,
            unattributed_posts=page.unattributed_posts,
        )


def _posts(payloads: tuple[PostPayload, ...]) -> list[PostOut]:
    return [PostOut.model_validate(payload) for payload in payloads]
