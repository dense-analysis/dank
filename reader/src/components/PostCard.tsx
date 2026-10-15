import { Check, ExternalLink, ImageOff } from "lucide-react";
import { useMemo, useState } from "react";
import { hasArticleContent } from "../lib/article";
import type { Filters, Post } from "../lib/types";
import { AuthorLink } from "./AuthorLink";
import { CollectionBadge } from "./CollectionBadge";
import { CommunityAvatar } from "./CommunityAvatar";
import { HighlightedText } from "./HighlightedText";
import { ReaderLink } from "./ReaderLink";
import { RedditCommunityLink, RedditMetadata } from "./RedditMetadata";
import { SourceLink, sourceName } from "./SourceLink";
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
export function safeUrl(url: string) {
  try {
    const parsed = new URL(url, window.location.origin);
    return ["http:", "https:"].includes(parsed.protocol)
      ? parsed.href
      : undefined;
  } catch {
    return undefined;
  }
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
  read,
  open,
  href,
  selectSource,
  query,
  filters,
  selectAuthor,
  selectCommunity,
}: {
  post: Post;
  read: boolean;
  open: () => void;
  href: string;
  selectSource: (domain: string) => void;
  query: string;
  filters: Filters;
  selectAuthor: (author: string) => void;
  selectCommunity: (community: string) => void;
}) {
  const [imageFailed, setImageFailed] = useState(false);
  const hasContent = useMemo(() => hasArticleContent(post.html), [post.html]);
  const thumbnail = post.thumbnail && safeUrl(post.thumbnail);
  return (
    <article
      className={`post-card ${post.reddit ? "reddit-card" : ""} ${read ? "is-read" : ""}`}
    >
      <div className="post-meta">
        {post.reddit ? (
          <CommunityAvatar
            community={post.reddit.community}
            iconUrl={post.reddit.icon_url}
          />
        ) : (
          <SourceMark domain={post.domain} />
        )}
        {post.reddit ? (
          <RedditCommunityLink
            community={post.reddit.community}
            select={selectCommunity}
          />
        ) : (
          <SourceLink domain={post.domain} select={selectSource} />
        )}
        <span className="meta-dot">·</span>
        <time dateTime={post.created_at}>{dateLabel(post.created_at)}</time>
        <span className="post-labels">
          <span className="source-kind">
            {post.source === "x" || post.source === "reddit"
              ? "Post"
              : "Article"}
          </span>
          {(!hasContent || post.collection_incomplete) && (
            <CollectionBadge incomplete={hasContent} />
          )}
        </span>
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
          {(post.excerpt || hasContent) && (
            <p>
              <HighlightedText
                text={post.excerpt || "Open to read this story."}
                query={query}
              />
            </p>
          )}
        </ReaderLink>
        {thumbnail && (
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
    </article>
  );
}
