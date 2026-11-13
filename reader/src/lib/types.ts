/**
 * The reader's names for the server's shapes. Every one comes from
 * api.gen.ts, which `make openapi` generates from the FastAPI schema, so a
 * contract change can't drift between Python and TypeScript. Only the reader's
 * own state (filters, sections) is declared here.
 */
import type { components } from "./api.gen";

type Schemas = components["schemas"];

export type RedditComment = Schemas["RedditCommentOut"];
export type RedditCommentCapture = Schemas["RedditCommentCaptureOut"];
export type RedditVideo = Schemas["RedditVideoOut"];
export type RedditImage = Schemas["RedditImageOut"];
/** A feed post's Reddit fields; an opened post also has its comments. */
export type RedditPost = Schemas["RedditOut"] &
  Partial<Pick<Schemas["RedditDetailOut"], "comment_capture">>;
export type RedditFlair = Schemas["FlairOut"];
export type CommunityPostTotal = Schemas["PostTotalOut"];
export type RedditCommunity = Schemas["CommunityOut"];

/** A collected post another one links to; linked posts carry an excerpt. */
export type PostReference = Schemas["LinkedPostOut"];
export type CollectionOrigin = Schemas["CollectionOriginOut"];
export type CollectionInfo = Schemas["CollectionInfoOut"];
export type YouTubePost = Schemas["YouTubeOut"];
export type CategoryAssignment = Schemas["CategoryAssignmentOut"];

/** A feed post, or one opened on its own, whose platform fields add detail. */
export type Post = Omit<Schemas["PostOut"], "x" | "reddit" | "hacker_news"> & {
  x?: XPost | null;
  reddit?: RedditPost | null;
  hacker_news?: HackerNewsPost | null;
};

export type SavedSource = Schemas["SourceOut"];
export type SourceSettings = Schemas["SourceListResponse"];
export type SourcesPage = Schemas["SourcesResponse"];
export type Source = SourcesPage["sources"][number];

export type XProfile = Pick<Schemas["XOut"], "handle" | "name" | "icon_url">;
export type XAccount = Schemas["XAccountOut"];
export type XLink = Schemas["XLinkOut"];
export type XMedia = Schemas["XMediaOut"];
export type XReplyTo = Schemas["XReplyToOut"];
export type XQuote = Schemas["XQuoteOut"];
/** A feed post's X fields; an opened post also has its conversation. */
export type XPost = Schemas["XOut"] &
  Partial<Pick<Schemas["XDetailOut"], "conversation">>;
export type XConversationStatus = Schemas["XOut"]["replies_status"];
export type XConversationNode = Schemas["XConversationNodeOut"];
export type XConversation = Schemas["XConversationOut"];
/** A post saved inside a quote or a conversation, with its X fields. */
export type CapturedPost = Schemas["CapturedPostOut"];
/** Any post an X card can show: from a feed, a quote or a conversation. */
export type XCardPost = Post | CapturedPost;

export type HackerNewsFilter = Schemas["HackerNewsFilter"];
/** A feed story's fields; an opened story also has its discussion. */
export type HackerNewsPost = Schemas["HackerNewsOut"] &
  Partial<
    Pick<Schemas["HackerNewsDetailOut"], "comment_capture" | "comments_items">
  >;

export type PostPage = Schemas["PostsResponse"];

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
  xFeed: "" | "for_you";
  xTab: "posts" | "authors";
  community: string;
  account: string;
  q: string;
  sort: "newest" | "oldest" | "relevance";
  domains: string[];
  tags: string[];
  flairs: string[];
  categories: string[];
  author: string;
  authorExact: boolean;
  after: string;
  before: string;
}
