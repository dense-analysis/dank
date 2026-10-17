import { ArrowUp, Check, ExternalLink, MessageCircle } from "lucide-react";
import { calendarDate } from "../lib/dateRanges";
import { hackerNewsFilters } from "../lib/hackerNews";
import type { HackerNewsFilter, HackerNewsPost, Post } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { HighlightedText } from "./HighlightedText";
import { PlatformIcon } from "./PlatformIcon";
import { ReaderLink } from "./ReaderLink";

export function HackerNewsFilters({
  value,
  change,
}: {
  value: HackerNewsFilter;
  change: (value: HackerNewsFilter) => void;
}) {
  return (
    <div className="hn-filters">
      <fieldset aria-label="Hacker News daily selection">
        <legend className="visually-hidden">Hacker News daily selection</legend>
        {hackerNewsFilters.map((filter) => (
          <button
            type="button"
            className={value === filter.value ? "selected" : ""}
            key={filter.value}
            aria-pressed={value === filter.value}
            onClick={() => change(filter.value)}
          >
            {filter.label}
          </button>
        ))}
      </fieldset>
      <p>
        Filters apply to collected stories, using hckr news daily score cutoffs.
      </p>
    </div>
  );
}

export function HackerNewsDay({ day }: { day: string }) {
  return (
    <h2 className="hn-day">
      <time dateTime={day}>{calendarDate(day)}</time>
      <span>UTC</span>
    </h2>
  );
}

export function HackerNewsMetrics({ story }: { story: HackerNewsPost }) {
  return (
    <span className="hn-metrics">
      <span title="Points at collection">
        <ArrowUp size={13} aria-hidden="true" />
        {story.points?.toLocaleString() ?? "—"}
        <span className="visually-hidden"> points</span>
      </span>
      <span title="Comments reported at collection">
        <MessageCircle size={13} aria-hidden="true" />
        {story.comments?.toLocaleString() ?? "—"}
        <span className="visually-hidden"> comments</span>
      </span>
    </span>
  );
}

function linkedHost(url: string | null) {
  const safe = url && safeUrl(url);
  if (!safe) return "";

  try {
    return new URL(safe).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

export function HackerNewsCard({
  post,
  read,
  open,
  href,
  query,
}: {
  post: Post;
  read: boolean;
  open: () => void;
  href: string;
  query: string;
}) {
  const story = post.hacker_news;
  if (!story) return null;
  const externalUrl = story.external_url && safeUrl(story.external_url);
  const discussionUrl = safeUrl(story.discussion_url);

  return (
    <article className={`post-card hn-card ${read ? "is-read" : ""}`}>
      <div className="post-meta">
        <PlatformIcon platform="hacker_news" size={16} />
        <span>Hacker News</span>
        {linkedHost(story.external_url) && (
          <>
            <span className="meta-dot">·</span>
            <span className="hn-host">{linkedHost(story.external_url)}</span>
          </>
        )}
      </div>
      <ReaderLink
        href={href}
        className="post-open"
        onNavigate={open}
        aria-label={`Read ${post.title || "Untitled post"}`}
      >
        <h2>
          <HighlightedText text={post.title || "Untitled post"} query={query} />
        </h2>
        {post.excerpt && (
          <p>
            <HighlightedText text={post.excerpt} query={query} />
          </p>
        )}
      </ReaderLink>
      <div className="post-bottom hn-card-footer">
        <HackerNewsMetrics story={story} />
        {post.author && <span className="hn-author">by {post.author}</span>}
        {read && (
          <span className="read-indicator">
            <Check size={12} /> Read
          </span>
        )}
        <div className="hn-card-links">
          {discussionUrl && (
            <a
              href={discussionUrl}
              target="_blank"
              rel="noreferrer"
              aria-label={`Discussion on Hacker News: ${post.title}`}
            >
              Discussion
            </a>
          )}
          {externalUrl && (
            <a
              href={externalUrl}
              target="_blank"
              rel="noreferrer"
              aria-label={`Read original: ${post.title}`}
            >
              Original <ExternalLink size={12} aria-hidden="true" />
            </a>
          )}
        </div>
      </div>
    </article>
  );
}
