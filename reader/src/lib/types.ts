export interface RedditComment {
  id: string;
  parent_id: string;
  author: string;
  body_html: string;
  body: string;
  score: number | null;
  created_utc: number | null;
  permalink: string;
  is_op: boolean;
  depth: number;
}

export interface RedditCommentCapture {
  status: "captured" | "disabled" | "failed" | "deferred" | "not_captured";
  sort: string;
  limit: number;
  total_available: number | null;
  has_more: boolean;
  comments: RedditComment[];
  captured_at?: string;
  latest_attempt_status?: "disabled" | "failed" | "deferred";
}

export interface RedditVideo {
  url: string;
  source_url?: string | null;
  poster_url: string | null;
  remote: boolean;
  hls_url?: string | null;
  fallback_url?: string | null;
}

export interface RedditImage {
  url: string;
  source_url: string;
}

export interface RedditPost {
  community: string;
  score: number | null;
  comments: number | null;
  flair: string | null;
  icon_url?: string | null;
  comment_capture?: RedditCommentCapture | null;
  video?: RedditVideo | null;
  videos?: RedditVideo[];
  images?: RedditImage[];
}

export interface RedditFlair {
  name: string;
  count: number;
}

export interface CommunityPostTotal {
  count: number;
  source: "arctic_shift";
  updated_at: string;
  captured_at: string;
}

export interface RedditCommunity {
  name: string;
  count: number;
  icon_url?: string | null;
  flairs?: RedditFlair[];
  post_total?: CommunityPostTotal | null;
}

export interface PostReference {
  excerpt?: string;
  id: string;
  domain: string;
  url: string;
  title: string;
  source: string;
}

export interface CollectionOrigin {
  domain: string;
  id: string;
  source: string;
  url: string;
  label: string;
  observed_at: string;
  kind: "reference" | "article";
}

export interface CollectionInfo {
  article_url: string;
  article: PostReference | null;
  article_status: "collected" | "incomplete" | "unavailable";
  post_first_seen_at: string | null;
  link_first_seen_at: string | null;
  first_seen_at: string | null;
  first_seen_from: CollectionOrigin[];
  article_collected_at: string | null;
  collected_via: CollectionOrigin | null;
  seen_before_post: boolean | null;
  post_published_at?: string | null;
  discovered_before_publication?: boolean | null;
  earlier_discovered_at?: string | null;
  earlier_discovered_from?: CollectionOrigin[];
  coverage: "saved_history";
}

export interface YouTubePost {
  kind: "video" | "short" | "post";
  video_id: string | null;
  channel: { id: string; name: string; url: string };
  duration_text: string;
  views_text: string;
  thumbnail_url: string | null;
  video_url: string | null;
}

export interface Post {
  id: string;
  domain: string;
  url: string;
  author: string;
  title: string;
  excerpt: string;
  html: string;
  collection_incomplete?: boolean;
  collection_info?: CollectionInfo | null;
  published_at?: string | null;
  linked_post?: PostReference | null;
  links?: PostReference[];
  links_count?: number;
  created_at: string;
  source: string;
  reddit?: RedditPost | null;
  hacker_news?: HackerNewsPost | null;
  x?: XPost | null;
  youtube?: YouTubePost | null;
  thumbnail: string | null;
  media: { url: string; content_type: string }[];
}

export interface Source {
  configured?: boolean;
  icon_url?: string | null;
  kind?: "rss" | "x" | "reddit" | "youtube" | "hacker_news";
  accounts?: XAccount[];
  communities?: RedditCommunity[];
  flairs?: RedditFlair[];
  domain: string;
  name: string;
  tags: string[];
  count: number;
}

export interface XProfile {
  handle: string;
  name: string;
  icon_url?: string | null;
}

export interface XAccount extends XProfile {
  count: number;
  total_posts?: number | null;
}

export interface XLink {
  url: string;
  title: string;
  description: string;
  image_url: string | null;
}

export interface XMedia {
  type: "image" | "video" | "gif";
  url: string;
  poster_url: string | null;
  alt: string;
  remote: boolean;
  source_url?: string;
  aspect_ratio?: number;
}

export interface XReplyTo {
  id: string;
  domain: string;
  url: string;
  is_self: boolean | null;
  collected: boolean;
}

export interface XQuote {
  id: string;
  domain: string;
  url: string;
  collected: boolean;
  post?: Post;
}

export interface XPost extends XProfile {
  quote?: XQuote;
  reply_to?: XReplyTo;
  conversation_id?: string;
  links?: XLink[];
  media?: XMedia[];
  reply_count?: number | null;
  replies_status?: XConversationStatus;
  replies_captured_count?: number;
  repost_count?: number | null;
  like_count?: number | null;
  view_count?: number | null;
  captured_at?: string | null;
  conversation?: XConversation;
}

export type XConversationStatus =
  | "captured"
  | "partial"
  | "failed"
  | "disabled"
  | "not_collected";

export interface XConversationNode extends Post {
  parent_id: string | null;
  depth: number;
  is_author: boolean;
  missing_parent: boolean;
  collected?: boolean;
}

export interface XConversation {
  status: XConversationStatus;
  captured_at: string | null;
  has_more: boolean;
  reason: string | null;
  reported_replies: number | null;
  limit: number;
  context: XConversationNode[];
  replies: XConversationNode[];
  local_posts_count?: number;
  local_posts_limited?: boolean;
  latest_attempt_status?: XConversationStatus;
  latest_attempt_at?: string | null;
}

export type HackerNewsFilter = "top10" | "top20" | "top50" | "all";

export interface HackerNewsPost {
  submitted_at?: string | null;
  points: number | null;
  comments: number | null;
  discussion_url: string;
  external_url: string | null;
  day: string;
  rank: number | null;
  comment_capture?: {
    status: "captured" | "partial" | "disabled" | "failed" | "not_captured";
    captured: number;
    total: number | null;
    reason: string | null;
  };
  comments_items?: {
    id: string;
    parent_id: string;
    author: string;
    html: string;
    created_at: string | null;
    depth: number;
  }[];
}

export type FeedKind =
  | "all"
  | "stories"
  | "x"
  | "reddit"
  | "youtube"
  | "hacker_news";
export type FeedPlatform = Exclude<FeedKind, "all">;

export type Section = FeedKind | "sources" | "settings";

export interface Filters {
  kind: FeedKind;
  blogFeed: "" | "sources" | "external";
  platforms: FeedPlatform[];
  hnFilter: HackerNewsFilter;
  xView: "posts" | "replies";
  community: string;
  account: string;
  q: string;
  sort: "newest" | "oldest" | "relevance";
  domains: string[];
  tags: string[];
  flairs: string[];
  author: string;
  authorExact: boolean;
  after: string;
  before: string;
}

export interface PostPage {
  posts: Post[];
  next_cursor: string | null;
  limited: boolean;
}
