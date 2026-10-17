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

export interface RedditPost {
  community: string;
  score: number | null;
  comments: number | null;
  flair: string | null;
  icon_url?: string | null;
  comment_capture?: RedditCommentCapture | null;
  video?: RedditVideo | null;
  videos?: RedditVideo[];
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

export interface Post {
  id: string;
  domain: string;
  url: string;
  author: string;
  title: string;
  excerpt: string;
  html: string;
  collection_incomplete?: boolean;
  created_at: string;
  source: string;
  reddit?: RedditPost | null;
  x?: XPost | null;
  thumbnail: string | null;
  media: { url: string; content_type: string }[];
}

export interface Source {
  kind?: "rss" | "x" | "reddit" | "youtube";
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

export interface XPost extends XProfile {
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
  latest_attempt_status?: XConversationStatus;
  latest_attempt_at?: string | null;
}

export type FeedKind = "all" | "stories" | "x" | "reddit" | "youtube";
export type Section = FeedKind | "sources" | "settings";

export interface Filters {
  kind: FeedKind;
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
