import { Bookmark, Check, ExternalLink, ImageOff } from "lucide-react";
import { useState } from "react";
import type { Post } from "../lib/types";

export function sourceName(domain: string) {
  return domain.replace(/^www\./, "").replace(/\.(com|org|net|co\.uk|io)$/, "");
}
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
        month: "short",
        day: "numeric",
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
  saved,
  read,
  open,
  toggleSave,
}: {
  post: Post;
  saved: boolean;
  read: boolean;
  open: () => void;
  toggleSave: () => void;
}) {
  const [imageFailed, setImageFailed] = useState(false);
  const thumbnail = post.thumbnail && safeUrl(post.thumbnail);
  return (
    <article className={`post-card ${read ? "is-read" : ""}`}>
      <div className="post-meta">
        <SourceMark domain={post.domain} />
        <strong title={post.domain}>{sourceName(post.domain)}</strong>
        <span className="meta-dot">·</span>
        <time dateTime={post.created_at}>{dateLabel(post.created_at)}</time>
        <span className="source-kind">
          {post.source === "x" ? "Post" : "Article"}
        </span>
      </div>
      <div className="post-main">
        <button
          type="button"
          className="post-open"
          onClick={open}
          aria-label={`Read ${post.title || "Untitled post"}`}
        >
          <h2>{post.title || "Untitled post"}</h2>
          <p>{post.excerpt || "Open to read this story."}</p>
        </button>
        {thumbnail && (
          <button
            type="button"
            className="thumbnail"
            onClick={open}
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
          </button>
        )}
      </div>
      <div className="post-bottom">
        <span className="author">{post.author || post.domain}</span>
        <span className="meta-dot">·</span>
        <span>{readingTime(post)} min read</span>
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
          <button
            type="button"
            className={`icon-button ${saved ? "is-saved" : ""}`}
            onClick={toggleSave}
            aria-label={`${saved ? "Unsave" : "Save"} ${post.title}`}
            aria-pressed={saved}
            title={saved ? "Remove bookmark" : "Save for later"}
          >
            <Bookmark size={17} fill={saved ? "currentColor" : "none"} />
          </button>
        </div>
      </div>
    </article>
  );
}
