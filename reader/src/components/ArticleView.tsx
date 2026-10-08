import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, ExternalLink, Minus, Plus } from "lucide-react";
import { useEffect } from "react";
import { useReaderPreferences } from "../hooks/useReaderPreferences";
import { fetchPost } from "../lib/api";
import type { Filters, Post } from "../lib/types";
import { ArticleBody } from "./ArticleBody";
import { AuthorLink } from "./AuthorLink";
import { dateLabel, readingTime, SourceMark, safeUrl } from "./PostCard";
import { SourceLink } from "./SourceLink";

export function ArticleView({
  domain,
  id,
  initial,
  close,
  selectSource,
  markRead,
  filters,
  selectAuthor,
}: {
  domain: string;
  id: string;
  initial?: Post;
  close: () => void;
  selectSource: (domain: string) => void;
  markRead: (post: Post) => void;
  filters: Filters;
  selectAuthor: (author: string) => void;
}) {
  const { preferences, updatePreferences, preferencesError } =
    useReaderPreferences();
  const { fontSize } = preferences;
  function setFontSize(size: number) {
    updatePreferences({ ...preferences, fontSize: size });
  }
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
  useEffect(() => {
    if (post) markRead(post);
  }, [post, markRead]);
  return (
    <main id="main" className="article-page" aria-label="Article reader">
      <div className="reader-toolbar">
        <button
          type="button"
          className="back-button"
          onClick={close}
          data-page-focus
        >
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
      </div>
      {preferencesError && (
        <div className="error-banner" role="alert">
          Browser storage is unavailable or full. Text size changes will last
          until this page closes.
        </div>
      )}
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
            <SourceLink domain={post.domain} select={selectSource} />
            <span className="meta-dot">·</span>
            <time dateTime={post.created_at}>{dateLabel(post.created_at)}</time>
          </div>
          <h1>{post.title || "Untitled post"}</h1>
          <div className="article-byline">
            <AuthorLink
              author={post.author}
              fallback={post.domain}
              filters={filters}
              select={selectAuthor}
            />
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
          <ArticleBody html={post.html} />
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
    </main>
  );
}
