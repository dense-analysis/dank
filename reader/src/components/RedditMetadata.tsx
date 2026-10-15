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

export function RedditMetadata({ reddit }: { reddit: RedditPost }) {
  return (
    <span className="reddit-metadata">
      {reddit.score !== null && (
        <span title="Score when collected">
          <ArrowUp size={13} aria-hidden="true" />
          {reddit.score.toLocaleString()} points
        </span>
      )}
      {reddit.comments !== null && (
        <span title="Comment count when collected">
          <MessageCircle size={13} aria-hidden="true" />
          {reddit.comments.toLocaleString()} comments
        </span>
      )}
      {reddit.flair && <span className="reddit-flair">{reddit.flair}</span>}
    </span>
  );
}
