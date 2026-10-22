import { ImageOff } from "lucide-react";
import type { ReactNode } from "react";
import { useMemo, useState } from "react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import { useRedditContent } from "../hooks/useRedditContent";
import { hasArticleContent } from "../lib/article";
import { sourceKind, sourceLabel } from "../lib/collections";
import { savedPublisherExcerpt } from "../lib/postExcerpt";
import {
  displayedRedditImages,
  redditImageKey,
  withoutRedditImageUrls,
} from "../lib/redditImages";
import { postKey } from "../lib/storage";
import type { FeedKind, Filters, Post } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { AuthorLink } from "./AuthorLink";
import { CollectionBadge } from "./CollectionBadge";
import { CommunityAvatar } from "./CommunityAvatar";
import { FeedSourceBadge } from "./FeedSourceBadge";
import { HighlightedText } from "./HighlightedText";
import { IdentityAvatar } from "./IdentityAvatar";
import { NavigableCard } from "./NavigableCard";
import { PostFooter } from "./PostFooter";
import { PostTime } from "./PostTime";
import { ReaderLink } from "./ReaderLink";
import {
  RedditCommunityLink,
  RedditFlair,
  RedditMetadata,
} from "./RedditMetadata";
import { RedditVideo } from "./RedditVideo";
import { SourceLink, sourceName } from "./SourceLink";

export { dateLabel } from "../lib/timestamps";
export { safeUrl } from "../lib/urls";

export function readingTime(post: Post) {
  return Math.max(
    1,
    Math.ceil(
      (post.html.replace(/<[^>]*>/g, " ").split(/\s+/).length || 1) / 230,
    ),
  );
}

export function PostTimestamp({ post }: { post: Post }) {
  const date =
    post.published_at === undefined ? post.created_at : post.published_at;

  return date ? (
    <PostTime date={date} />
  ) : (
    <span>Publication date unavailable</span>
  );
}

export function SourceMark({ domain }: { domain: string }) {
  const blog = ["stories", "hacker_news"].includes(sourceKind(domain));

  return (
    <IdentityAvatar
      name={sourceName(domain)}
      kind="source"
      iconUrl={
        blog
          ? `/api/reader/source-icon?${new URLSearchParams({ domain })}`
          : undefined
      }
    />
  );
}
export function PostCard({
  post,
  links,
  read,
  open,
  href,
  selectSource,
  sourceFeed,
  query,
  filters,
  selectAuthor,
  selectCommunity,
  selectFlair,
}: {
  post: Post;
  links?: ReactNode;
  read: boolean;
  open: () => void;
  href: string;
  selectSource: (domain: string) => void;
  sourceFeed?: FeedKind;
  query: string;
  filters: Filters;
  selectAuthor: (author: string) => void;
  selectCommunity: (community: string) => void;
  selectFlair: (community: string, flair: string) => void;
}) {
  const { preferences } = useReaderPreferences();
  const [imageFailed, setImageFailed] = useState(false);
  const { videos, html, excerpt, onUnavailable } = useRedditContent(post);
  const hasContent = useMemo(() => hasArticleContent(html), [html]);
  const hasReadableContent = hasContent || videos.length > 0;
  const thumbnail = post.thumbnail && safeUrl(post.thumbnail);
  const imageKeys = useMemo(
    () =>
      displayedRedditImages(
        post.reddit?.images ?? [],
        new Set(
          !imageFailed && !videos.length && thumbnail
            ? [redditImageKey(thumbnail) ?? ""]
            : [],
        ),
        post.id,
      ),
    [post.reddit?.images, post.id, imageFailed, videos.length, thumbnail],
  );
  const summary =
    savedPublisherExcerpt(
      post,
      preferences.hideRawImageLinks
        ? withoutRedditImageUrls(excerpt, imageKeys)
        : excerpt,
    ) ||
    (!videos.length && (!post.reddit || !thumbnail) && hasContent
      ? "Open to read this story."
      : "");
  const fromHackerNews = post.links?.some(
    (link) =>
      link.source === "hacker_news" || link.domain === "news.ycombinator.com",
  );
  const communityContext =
    filters.kind === "reddit" &&
    !!filters.community &&
    post.reddit?.community.toLowerCase() === filters.community.toLowerCase();
  return (
    <NavigableCard
      className={`post-card ${post.reddit ? "reddit-card" : ""} ${read ? "is-read" : ""}`}
    >
      <div className="post-meta">
        {!communityContext && (
          <>
            {post.reddit ? (
              <>
                <CommunityAvatar
                  community={post.reddit.community}
                  iconUrl={post.reddit.icon_url}
                />
                <RedditCommunityLink
                  community={post.reddit.community}
                  select={selectCommunity}
                />
              </>
            ) : (
              <>
                <SourceMark domain={post.domain} />
                <SourceLink
                  domain={post.domain}
                  select={selectSource}
                  kind={sourceFeed}
                />
              </>
            )}
            <span className="meta-dot">·</span>
          </>
        )}
        {post.reddit && (
          <RedditFlair reddit={post.reddit} selectFlair={selectFlair} />
        )}
        <PostTimestamp post={post} />
        {(!communityContext ||
          !hasReadableContent ||
          post.collection_incomplete) && (
          <span className="post-labels">
            {!communityContext &&
              (filters.kind === "all" ? (
                <FeedSourceBadge
                  source={
                    post.reddit
                      ? "reddit"
                      : fromHackerNews
                        ? "hacker_news"
                        : "stories"
                  }
                  label={
                    fromHackerNews && !post.reddit
                      ? "via Hacker News"
                      : undefined
                  }
                />
              ) : (
                <span className="source-kind">
                  {post.source === "x" || post.source === "reddit"
                    ? "Post"
                    : "Article"}
                </span>
              ))}
            {(!hasReadableContent || post.collection_incomplete) && (
              <CollectionBadge incomplete={hasContent} />
            )}
          </span>
        )}
      </div>
      <div className="post-main">
        <ReaderLink
          href={href}
          className="post-open"
          data-card-link
          preserveTextSelection
          draggable={false}
          onNavigate={open}
          aria-label={`Read ${post.title || "Untitled post"}`}
        >
          <h2>
            <HighlightedText
              text={post.title || "Untitled post"}
              query={query}
            />
          </h2>
          {summary && (
            <p>
              <HighlightedText text={summary} query={query} />
            </p>
          )}
        </ReaderLink>
        {thumbnail && videos.length === 0 && (
          <ReaderLink
            href={href}
            className="thumbnail"
            onNavigate={open}
            aria-label={`Open image and story: ${post.title}`}
            tabIndex={-1}
          >
            {imageFailed ? (
              <ImageOff size={24} />
            ) : (
              <img
                src={thumbnail}
                alt=""
                loading="lazy"
                onError={() => setImageFailed(true)}
              />
            )}
          </ReaderLink>
        )}
      </div>
      {videos.map((video) => (
        <RedditVideo
          key={video.url}
          video={video}
          originalUrl={post.url}
          positionScope={postKey(post)}
          onUnavailable={onUnavailable}
        />
      ))}
      <PostFooter
        post={post}
        readerHref={href}
        read={read}
        source={{ url: post.url, label: `Open original: ${post.title}` }}
        byline={
          post.reddit && (
            <AuthorLink
              author={post.author}
              fallback={sourceLabel(post.domain)}
              filters={filters}
              select={selectAuthor}
            />
          )
        }
      >
        {post.reddit ? (
          <RedditMetadata reddit={post.reddit} />
        ) : (
          <div className="post-footer-details">
            <AuthorLink
              author={post.author}
              fallback={sourceLabel(post.domain)}
              filters={filters}
              select={selectAuthor}
            />
            {hasContent && (
              <>
                <span className="meta-dot">·</span>
                <span>{readingTime(post)} min read</span>
              </>
            )}
          </div>
        )}
      </PostFooter>
      {links}
    </NavigableCard>
  );
}
