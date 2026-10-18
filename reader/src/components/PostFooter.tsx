import { Check, ExternalLink } from "lucide-react";
import type { ReactNode } from "react";
import { safeUrl } from "../lib/urls";

export function PostFooter({
  children,
  byline,
  summary,
  read,
  source,
}: {
  children: ReactNode;
  byline?: ReactNode;
  summary?: ReactNode;
  read: boolean;
  source?: { url: string; label: string; title?: string };
}) {
  const original = source && safeUrl(source.url);

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
    </footer>
  );
}
