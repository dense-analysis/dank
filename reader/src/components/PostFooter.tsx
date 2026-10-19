import { Check, ExternalLink, Eye } from "lucide-react";
import { type ReactNode, useId, useState } from "react";
import type { Post } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { CollectionInfoContent, collectionInfoTitle } from "./CollectionInfo";

export function PostFooter({
  children,
  byline,
  summary,
  read,
  source,
  post,
  readerHref,
}: {
  children: ReactNode;
  byline?: ReactNode;
  summary?: ReactNode;
  read: boolean;
  post?: Post;
  readerHref?: string;
  source?: { url: string; label: string; title?: string };
}) {
  const original = source && safeUrl(source.url);
  const [infoOpen, setInfoOpen] = useState(false);
  const infoId = useId();
  const info = post?.collection_info;

  return (
    <footer className="post-footer">
      {byline && <div className="post-footer-byline">{byline}</div>}
      <div className="post-bottom social-post-actions">
        {children}
        <div className="post-utilities">
          {read && (
            <span className="read-indicator" title="Read">
              <Check size={13} aria-hidden="true" />
              <span className="visually-hidden">Read</span>
            </span>
          )}
          {info && (
            <button
              type="button"
              className={`icon-button collection-info-button${info.seen_before_post ? " has-earlier-discovery" : ""}`}
              aria-label="Collection info"
              title={collectionInfoTitle(info)}
              aria-expanded={infoOpen}
              aria-controls={infoId}
              onClick={() => setInfoOpen(!infoOpen)}
            >
              <Eye size={16} aria-hidden="true" />
            </button>
          )}
          {original && (
            <a
              className="icon-button"
              href={original}
              target="_blank"
              rel="noreferrer"
              aria-label={source.label}
              title={source.title ?? "Open original"}
            >
              <ExternalLink size={16} aria-hidden="true" />
            </a>
          )}
        </div>
      </div>
      {summary && <div className="post-footer-summary">{summary}</div>}
      {info && infoOpen && post && (
        <section
          id={infoId}
          className="collection-info-card"
          aria-label="Collection info"
          data-card-control
        >
          <h3>Collection info</h3>
          <CollectionInfoContent
            post={post}
            info={info}
            readerHref={readerHref}
          />
        </section>
      )}
    </footer>
  );
}
