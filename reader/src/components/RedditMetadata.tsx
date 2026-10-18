import { ArrowUp, MessageCircle } from "lucide-react";
import { filterParams } from "../lib/navigation";
import { defaultFilters } from "../lib/storage";
import type { RedditPost } from "../lib/types";
import { Count } from "./Count";
import { ReaderLink } from "./ReaderLink";

export function RedditCommunityLink({
  community,
  select,
}: {
  community: string;
  select: (community: string) => void;
}) {
  return (
    <ReaderLink
      className="source-link"
      href={`/reader/?${filterParams({ ...defaultFilters, kind: "reddit", community })}`}
      onNavigate={() => select(community)}
      aria-label={`Posts from r/${community}`}
    >
      r/{community}
    </ReaderLink>
  );
}

export function RedditFlair({
  reddit,
  selectFlair,
}: {
  reddit: RedditPost;
  selectFlair: (community: string, flair: string) => void;
}) {
  const { community, flair } = reddit;

  if (!flair) return null;

  return (
    <ReaderLink
      className="reddit-flair"
      href={`/reader/?${filterParams({ ...defaultFilters, kind: "reddit", community, flairs: [flair] })}`}
      onNavigate={() => selectFlair(community, flair)}
      aria-label={`Posts from r/${community} with flair ${flair}`}
      title={`Show ${flair} posts from r/${community}`}
    >
      {flair}
    </ReaderLink>
  );
}

export function RedditMetadata({ reddit }: { reddit: RedditPost }) {
  return (
    <span className="reddit-metadata social-engagement">
      {reddit.score !== null && (
        <span
          className="reddit-score social-metric"
          title="Score when collected"
        >
          <ArrowUp size={16} aria-hidden="true" />
          <span className="visually-hidden">Score: </span>
          <Count value={reddit.score} />
        </span>
      )}
      {reddit.comments !== null && (
        <span className="social-metric" title="Comment count when collected">
          <MessageCircle size={16} aria-hidden="true" />
          <span className="visually-hidden">Comments: </span>
          <Count value={reddit.comments} />
        </span>
      )}
    </span>
  );
}
