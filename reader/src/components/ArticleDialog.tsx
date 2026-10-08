import { useQuery } from "@tanstack/react-query";
import DOMPurify from "dompurify";
import { ArrowLeft, Bookmark, ExternalLink, Minus, Plus } from "lucide-react";
import { useState } from "react";
import { fetchPost } from "../lib/api";
import type { Post } from "../lib/types";
import { Modal } from "./Modal";
import {
  dateLabel,
  readingTime,
  SourceMark,
  safeUrl,
  sourceName,
} from "./PostCard";

export function ArticleDialog({
  domain,
  id,
  initial,
  saved,
  close,
  toggleSave,
}: {
  domain: string;
  id: string;
  initial?: Post;
  saved: boolean;
  close: () => void;
  toggleSave: (post: Post) => void;
}) {
  const [fontSize, setFontSize] = useState(19);
  const {
    data: post,
    isPending,
    error,
    refetch,
  } = useQuery({
    queryKey: ["post", domain, id],
    queryFn: ({ signal }) => fetchPost(domain, id, signal),
    initialData: initial,
  });
  return (
    <Modal onClose={close} label="Article reader" className="article-dialog">
      <div className="reader-toolbar">
        <button type="button" className="back-button" onClick={close}>
          <ArrowLeft size={17} />
          Back to stories
        </button>
        <div className="reader-type">
          <button
            type="button"
            className="icon-button"
            aria-label="Smaller article text"
            disabled={fontSize <= 16}
            onClick={() => setFontSize(fontSize - 1)}
          >
            <Minus size={14} />
          </button>
          <span aria-hidden="true">Aa</span>
          <button
            type="button"
            className="icon-button"
            aria-label="Larger article text"
            disabled={fontSize >= 26}
            onClick={() => setFontSize(fontSize + 1)}
          >
            <Plus size={14} />
          </button>
        </div>
        {post && (
          <button
            type="button"
            className={`icon-button ${saved ? "is-saved" : ""}`}
            aria-label={saved ? "Remove bookmark" : "Save article"}
            aria-pressed={saved}
            onClick={() => toggleSave(post)}
          >
            <Bookmark size={18} fill={saved ? "currentColor" : "none"} />
          </button>
        )}
      </div>
      {isPending && (
        <div className="empty-state" role="status">
          Opening your story…
        </div>
      )}
      {error && (
        <div className="empty-state" role="alert">
          <h2>We couldn’t open this story</h2>
          <p>{error.message}</p>
          <button
            type="button"
            className="primary-button"
            onClick={() => void refetch()}
          >
            Try again
          </button>
        </div>
      )}
      {post && (
        <div className="article-inner">
          <div className="post-meta">
            <SourceMark domain={post.domain} />
            <strong>{sourceName(post.domain)}</strong>
            <span className="meta-dot">·</span>
            <span>{dateLabel(post.created_at)}</span>
          </div>
          <h1>{post.title || "Untitled post"}</h1>
          <div className="article-byline">
            <span>{post.author || post.domain}</span>
            <span> {readingTime(post)} min read</span>
          </div>
          <a
            className="original-link"
            href={safeUrl(post.url)}
            target="_blank"
            rel="noreferrer"
          >
            Read at {post.domain}
            <ExternalLink size={13} />
          </a>
          {/* Stored articles are untrusted; sanitize again at the rendering boundary. */}
          <div
            className="article-body"
            style={{ fontSize }}
            ref={(element) => {
              if (!element) return;
              element.innerHTML = DOMPurify.sanitize(post.html, {
                USE_PROFILES: { html: true },
                FORBID_TAGS: ["form", "input", "button", "iframe", "style"],
                FORBID_ATTR: ["style", "id", "name"],
              });
              for (const link of element.querySelectorAll("a")) {
                link.target = "_blank";
                link.rel = "noopener noreferrer";
              }
              for (const image of element.querySelectorAll("img")) {
                image.loading = "lazy";
                image.referrerPolicy = "no-referrer";
              }
            }}
          />
          {!post.html.trim() && (
            <p className="article-body">
              The full article wasn’t collected. You can read it at the original
              source above.
            </p>
          )}
          {post.media.some((asset) =>
            /^(video|audio)\//.test(asset.content_type),
          ) && (
            <section className="article-media" aria-label="Attached media">
              {post.media
                .filter(
                  (asset) =>
                    /^(video|audio)\//.test(asset.content_type) &&
                    safeUrl(asset.url),
                )
                .map((asset) =>
                  asset.content_type.startsWith("video/") ? (
                    <video
                      key={asset.url}
                      controls
                      preload="none"
                      src={safeUrl(asset.url)}
                      aria-label="Article video"
                    >
                      <track kind="captions" />
                    </video>
                  ) : (
                    <audio
                      key={asset.url}
                      controls
                      preload="none"
                      src={safeUrl(asset.url)}
                      aria-label="Article audio"
                    >
                      <track kind="captions" />
                    </audio>
                  ),
                )}
            </section>
          )}
          <div className="article-end">
            <span>YOU’VE REACHED THE END</span>
            <button type="button" className="secondary-button" onClick={close}>
              <ArrowLeft size={15} />
              Back to your reading
            </button>
          </div>
        </div>
      )}
    </Modal>
  );
}
