import { ArrowUp, MessageCircle } from "lucide-react";
import { filterParams } from "../lib/navigation";
import { defaultFilters } from "../lib/storage";
import type { RedditPost } from "../lib/types";
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

export function RedditMetadata({
  reddit,
  selectFlair,
}: {
  reddit: RedditPost;
  selectFlair: (community: string, flair: string) => void;
}) {
  const { community, flair } = reddit;

  return (
    <span className="reddit-metadata">
      {reddit.score !== null && (
        <span className="reddit-score" title="Score when collected">
          <ArrowUp size={13} aria-hidden="true" />
          <span className="visually-hidden">Score: </span>
          {reddit.score.toLocaleString()}
        </span>
      )}
      {reddit.comments !== null && (
        <span title="Comment count when collected">
          <MessageCircle size={13} aria-hidden="true" />
          {reddit.comments.toLocaleString()} comments
        </span>
      )}
      {flair && (
        <ReaderLink
          className="reddit-flair"
          href={`/reader/?${filterParams({ ...defaultFilters, kind: "reddit", community, flairs: [flair] })}`}
          onNavigate={() => selectFlair(community, flair)}
          aria-label={`Posts from r/${community} with flair ${flair}`}
          title={`Show ${flair} posts from r/${community}`}
        >
          {flair}
        </ReaderLink>
      )}
    </span>
  );
}
