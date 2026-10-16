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
  status: "captured" | "disabled" | "failed" | "not_captured";
  sort: string;
  limit: number;
  total_available: number | null;
  has_more: boolean;
  comments: RedditComment[];
  captured_at?: string;
  latest_attempt_status?: "disabled" | "failed";
}

export interface RedditVideo {
  url: string;
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

export interface RedditCommunity {
  name: string;
  count: number;
  icon_url?: string | null;
  flairs?: RedditFlair[];
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
  thumbnail: string | null;
  media: { url: string; content_type: string }[];
}

export interface Source {
  kind?: "rss" | "x" | "reddit";
  communities?: RedditCommunity[];
  flairs?: RedditFlair[];
  domain: string;
  name: string;
  tags: string[];
  count: number;
}

export interface Filters {
  kind: "stories" | "reddit";
  community: string;
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
