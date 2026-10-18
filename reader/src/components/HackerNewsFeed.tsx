import { ArrowUp, MessageCircle } from "lucide-react";
import type { ReactNode } from "react";
import { calendarDate } from "../lib/dateRanges";
import { hackerNewsExcerpt, hackerNewsFilters } from "../lib/hackerNews";
import type {
  Filters,
  HackerNewsFilter,
  HackerNewsPost,
  Post,
} from "../lib/types";
import { safeUrl } from "../lib/urls";
import { AuthorLink } from "./AuthorLink";
import { Count } from "./Count";
import { HighlightedText } from "./HighlightedText";
import { NavigableCard } from "./NavigableCard";
import { PlatformIcon } from "./PlatformIcon";
import { PostFooter } from "./PostFooter";
import { PostTime } from "./PostTime";
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
    <h2 className="hn-day" title="Front-page day on hckr news (UTC)">
      <time dateTime={day}>{calendarDate(day)}</time>
      <span>UTC</span>
    </h2>
  );
}

export function HackerNewsTimestamp({ story }: { story: HackerNewsPost }) {
  return story.submitted_at ? (
    <PostTime date={story.submitted_at} title="Submitted to Hacker News" />
  ) : (
    <span>Submission time unavailable</span>
  );
}

export function HackerNewsMetrics({
  story,
  navigation,
}: {
  story: HackerNewsPost;
  navigation?: { href: string; open: () => void; label: string };
}) {
  const comments = (
    <>
      <MessageCircle size={16} aria-hidden="true" />
      <Count value={story.comments} />
      <span className="visually-hidden"> comments</span>
    </>
  );

  return (
    <span className="hn-metrics social-engagement">
      <span className="social-metric" title="Points at collection">
        <ArrowUp size={16} aria-hidden="true" />
        <Count value={story.points} />
        <span className="visually-hidden"> points</span>
      </span>
      {navigation ? (
        <ReaderLink
          className="social-metric"
          href={navigation.href}
          onNavigate={navigation.open}
          aria-label={navigation.label}
          title="Comments reported at collection"
        >
          {comments}
        </ReaderLink>
      ) : (
        <span className="social-metric" title="Comments reported at collection">
          {comments}
        </span>
      )}
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
  links,
  discussionHref,
  openDiscussion,
  read,
  open,
  href,
  query,
  filters,
  selectAuthor,
}: {
  post: Post;
  links?: ReactNode;
  discussionHref: string;
  openDiscussion: () => void;
  read: boolean;
  open: () => void;
  href: string;
  query: string;
  filters: Filters;
  selectAuthor: (author: string) => void;
}) {
  const story = post.hacker_news;
  if (!story) return null;
  const externalUrl = story.external_url && safeUrl(story.external_url);
  const discussionUrl = safeUrl(story.discussion_url);
  const originalUrl = externalUrl || discussionUrl;
  const excerpt = hackerNewsExcerpt(
    post.excerpt,
    story.external_url,
    post.html,
  );

  return (
    <NavigableCard className={`post-card hn-card ${read ? "is-read" : ""}`}>
      <div className="post-meta">
        <PlatformIcon platform="hacker_news" size={16} />
        <span>Hacker News</span>
        {linkedHost(story.external_url) && (
          <>
            <span className="meta-dot">·</span>
            <span className="hn-host">{linkedHost(story.external_url)}</span>
          </>
        )}
        <span className="meta-dot">·</span>
        <HackerNewsTimestamp story={story} />
      </div>
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
          <HighlightedText text={post.title || "Untitled post"} query={query} />
        </h2>
        {excerpt && (
          <p>
            <HighlightedText text={excerpt} query={query} />
          </p>
        )}
      </ReaderLink>
      <PostFooter
        read={read}
        byline={
          <AuthorLink
            author={post.author}
            fallback="Hacker News"
            filters={filters}
            select={selectAuthor}
          />
        }
        source={
          originalUrl
            ? {
                url: originalUrl,
                label: `Read original: ${post.title}`,
              }
            : undefined
        }
      >
        <HackerNewsMetrics
          story={story}
          navigation={
            discussionUrl
              ? {
                  href: discussionHref,
                  open: openDiscussion,
                  label: `Discussion on Hacker News: ${post.title}`,
                }
              : undefined
          }
        />
      </PostFooter>
      {links}
    </NavigableCard>
  );
}
