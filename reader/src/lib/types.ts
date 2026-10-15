export interface RedditPost {
  community: string;
  score: number | null;
  comments: number | null;
  flair: string | null;
}

export interface RedditCommunity {
  name: string;
  count: number;
}

export interface Post {
  id: string;
  domain: string;
  url: string;
  author: string;
  title: string;
  excerpt: string;
  html: string;
  created_at: string;
  source: string;
  reddit?: RedditPost | null;
  thumbnail: string | null;
  media: { url: string; content_type: string }[];
}

export interface Source {
  kind?: "rss" | "x" | "reddit";
  communities?: RedditCommunity[];
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
