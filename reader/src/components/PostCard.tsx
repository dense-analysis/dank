import { Check, ExternalLink, ImageOff } from "lucide-react";
import type { ReactNode } from "react";
import { useMemo, useState } from "react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import { useRedditContent } from "../hooks/useRedditContent";
import { hasArticleContent } from "../lib/article";
import {
  displayedRedditImages,
  redditImageKey,
  withoutRedditImageUrls,
} from "../lib/redditImages";
import { postKey } from "../lib/storage";
import type { Filters, Post } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { AuthorLink } from "./AuthorLink";
import { CollectionBadge } from "./CollectionBadge";
import { CommunityAvatar } from "./CommunityAvatar";
import { HighlightedText } from "./HighlightedText";
import { ReaderLink } from "./ReaderLink";
import {
  RedditCommunityLink,
  RedditFlair,
  RedditMetadata,
} from "./RedditMetadata";
import { RedditVideo } from "./RedditVideo";
import { SourceLink, sourceName } from "./SourceLink";

export { safeUrl } from "../lib/urls";

export function readingTime(post: Post) {
  return Math.max(
    1,
    Math.ceil(
      (post.html.replace(/<[^>]*>/g, " ").split(/\s+/).length || 1) / 230,
    ),
  );
}
export function dateLabel(date: string) {
  const value = new Date(date);
  return Number.isNaN(value.getTime())
    ? "Date unavailable"
    : new Intl.DateTimeFormat(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hourCycle: "h23",
      }).format(value);
}
export function SourceMark({ domain }: { domain: string }) {
  return (
    <span
      className={`source-mark tone-${domain.length % 5}`}
      aria-hidden="true"
    >
      {sourceName(domain).slice(0, 1).toUpperCase()}
    </span>
  );
}
export function PostCard({
  post,
  links,
  read,
  open,
  href,
  selectSource,
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
    (preferences.hideRawImageLinks
      ? withoutRedditImageUrls(excerpt, imageKeys)
      : excerpt) ||
    (!videos.length && (!post.reddit || !thumbnail) && hasContent
      ? "Open to read this story."
      : "");
  const communityContext =
    filters.kind === "reddit" &&
    !!filters.community &&
    post.reddit?.community.toLowerCase() === filters.community.toLowerCase();
  return (
    <article
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
                <SourceLink domain={post.domain} select={selectSource} />
              </>
            )}
            <span className="meta-dot">·</span>
          </>
        )}
        {post.reddit && (
          <RedditFlair reddit={post.reddit} selectFlair={selectFlair} />
        )}
        <time dateTime={post.created_at}>{dateLabel(post.created_at)}</time>
        {(!communityContext ||
          !hasReadableContent ||
          post.collection_incomplete) && (
          <span className="post-labels">
            {!communityContext && (
              <span className="source-kind">
                {post.source === "x" || post.source === "reddit"
                  ? "Post"
                  : "Article"}
              </span>
            )}
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
      <div className="post-bottom">
        <AuthorLink
          author={post.author}
          fallback={post.domain}
          filters={filters}
          select={selectAuthor}
        />
        {post.reddit && <RedditMetadata reddit={post.reddit} />}
        {!post.reddit && hasContent && (
          <>
            <span className="meta-dot">·</span>
            <span>{readingTime(post)} min read</span>
          </>
        )}
        {read && (
          <span className="read-indicator">
            <Check size={12} /> Read
          </span>
        )}
        <div className="post-actions">
          <a
            href={safeUrl(post.url)}
            target="_blank"
            rel="noreferrer"
            className="icon-button"
            aria-label={`Open original: ${post.title}`}
            title="Open original"
          >
            <ExternalLink size={15} />
          </a>
        </div>
      </div>
      {links}
    </article>
  );
}
