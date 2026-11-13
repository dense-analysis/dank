/**
 * Test builders for the API's shapes. Each fills every field the schema
 * requires with a neutral value, so a test states only what it is about and
 * its data can't drift from what the server sends. Override any field.
 */
import type { components } from "./api.gen";
import type { RefreshJob } from "./refresh";
import type {
  CollectionInfo,
  HackerNewsPost,
  Post,
  RedditCommunity,
  RedditPost,
  RedditVideo,
  Source,
  XAccount,
  XConversation,
  XConversationNode,
  XPost,
} from "./types";
import type { YouTubeItem } from "./youtubeCollection";

type Schemas = components["schemas"];

export const FIXTURE_TIME = "2026-10-08T10:00:00Z";

export function post(fields: Partial<Post> = {}): Post {
  return {
    id: "post",
    domain: "example.com",
    url: "https://example.com/post",
    author: "",
    title: "",
    excerpt: "",
    html: "",
    collection_incomplete: false,
    created_at: FIXTURE_TIME,
    source: "rss",
    thumbnail: null,
    media: [],
    ...fields,
  };
}

export function xPost(fields: Partial<XPost> = {}): XPost {
  return {
    handle: "account",
    name: "Account",
    icon_url: null,
    links: [],
    replies_status: "not_collected",
    replies_captured_count: 0,
    reply_count: null,
    repost_count: null,
    like_count: null,
    view_count: null,
    captured_at: null,
    ...fields,
  };
}

export function capturedPost(
  fields: Partial<Schemas["CapturedPostOut"]> = {},
): Schemas["CapturedPostOut"] {
  return {
    id: "captured",
    domain: "x.com",
    url: "https://x.com/account/status/1",
    author: "",
    title: "",
    html: "",
    excerpt: "",
    source: "x",
    created_at: FIXTURE_TIME,
    thumbnail: null,
    media: [],
    x: capturedX(),
    ...fields,
  };
}

export function capturedX(
  fields: Partial<Schemas["CapturedXOut"]> = {},
): Schemas["CapturedXOut"] {
  return {
    handle: "account",
    name: "Account",
    icon_url: null,
    links: [],
    media: [],
    reply_count: null,
    ...fields,
  };
}

export function xConversation(
  fields: Partial<XConversation> = {},
): XConversation {
  return {
    status: "captured",
    captured_at: FIXTURE_TIME,
    has_more: false,
    reason: null,
    reported_replies: null,
    limit: 50,
    captured_count: 0,
    context: [],
    replies: [],
    ...fields,
  };
}

export function conversationNode(
  fields: Partial<XConversationNode> = {},
): XConversationNode {
  return {
    ...capturedPost(),
    parent_id: null,
    depth: 0,
    missing_parent: false,
    is_author: false,
    ...fields,
  };
}

export function xAccount(fields: Partial<XAccount> = {}): XAccount {
  return {
    handle: "account",
    name: "Account",
    icon_url: null,
    total_posts: null,
    count: 0,
    ...fields,
  };
}

export function redditPost(fields: Partial<RedditPost> = {}): RedditPost {
  return {
    community: "",
    score: null,
    comments: null,
    flair: null,
    icon_url: null,
    video: null,
    videos: [],
    ...fields,
  };
}

export function redditVideo(fields: Partial<RedditVideo> = {}): RedditVideo {
  return {
    url: "",
    source_url: "",
    poster_url: null,
    remote: true,
    hls_url: null,
    fallback_url: null,
    ...fields,
  };
}

export function redditCommunity(
  fields: Partial<RedditCommunity> = {},
): RedditCommunity {
  return {
    name: "community",
    count: 0,
    icon_url: null,
    flairs: [],
    post_total: null,
    ...fields,
  };
}

export function hackerNewsPost(
  fields: Partial<HackerNewsPost> = {},
): HackerNewsPost {
  return {
    points: null,
    comments: null,
    discussion_url: "https://news.ycombinator.com/item?id=1",
    external_url: null,
    day: "2026-10-08",
    rank: null,
    submitted_at: null,
    ...fields,
  };
}

export function collectionInfo(
  fields: Partial<CollectionInfo> = {},
): CollectionInfo {
  return {
    article_url: "https://example.com/article",
    article: null,
    post_first_seen_at: null,
    link_first_seen_at: null,
    first_seen_at: null,
    first_seen_from: [],
    article_collected_at: null,
    collected_via: null,
    seen_before_post: null,
    article_status: "unavailable",
    coverage: "saved_history",
    post_published_at: null,
    discovered_before_publication: null,
    earlier_discovered_at: null,
    earlier_discovered_from: [],
    ...fields,
  };
}

type FeedSource = Extract<Source, { kind: "rss" | "hacker_news" | "youtube" }>;
type XSource = Extract<Source, { kind: "x" }>;
type RedditSource = Extract<Source, { kind: "reddit" }>;

export function feedSource(fields: Partial<FeedSource> = {}): FeedSource {
  return {
    domain: "example.com",
    name: "example.com",
    icon_url: null,
    configured: true,
    tags: [],
    count: 0,
    kind: "rss",
    ...fields,
  };
}

export function xSource(fields: Partial<XSource> = {}): XSource {
  return {
    domain: "x.com",
    name: "X",
    icon_url: null,
    configured: true,
    tags: [],
    count: 0,
    kind: "x",
    accounts: [],
    for_you_count: 0,
    ...fields,
  };
}

export function redditSource(fields: Partial<RedditSource> = {}): RedditSource {
  return {
    domain: "reddit.com",
    name: "reddit.com",
    icon_url: null,
    configured: true,
    tags: [],
    count: 0,
    kind: "reddit",
    communities: [],
    flairs: [],
    ...fields,
  };
}

export function youtubeItem(fields: Partial<YouTubeItem> = {}): YouTubeItem {
  return {
    id: "video:dQw4w9WgXcQ",
    kind: "video",
    video_id: "dQw4w9WgXcQ",
    url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    title: "",
    text: "",
    channel: { id: "", name: "", url: "" },
    thumbnail_url: null,
    thumbnail_variants: [],
    video_url: null,
    duration_text: "",
    views_text: "",
    published_text: "",
    observed_at: FIXTURE_TIME,
    metadata_capture: {
      status: "disabled",
      provider: "",
      observed_at: "",
      retry_after: "",
      available: false,
      captured_at: "",
    },
    detail_capture: { status: "disabled" },
    discoveries: [],
    references: [],
    reference_count: 0,
    title_history: [],
    ...fields,
  };
}

export function refreshJob(fields: Partial<RefreshJob> = {}): RefreshJob {
  return {
    id: "job",
    scope: { kind: "all" },
    label: "all sources",
    status: "queued",
    message: "",
    created_at: FIXTURE_TIME,
    finished_at: null,
    posts_saved: 0,
    run_id: null,
    ...fields,
  };
}
