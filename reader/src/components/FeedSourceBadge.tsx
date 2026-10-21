import { Rss } from "lucide-react";
import type { FeedKind } from "../lib/types";
import { PlatformIcon } from "./PlatformIcon";
import "../universal-feed.css";

export function FeedSourceBadge({
  source,
  label,
}: {
  source: Exclude<FeedKind, "all">;
  label?: string;
}) {
  const names = {
    stories: "Article",
    x: "X",
    reddit: "Reddit",
    hacker_news: "Hacker News",
    youtube: "YouTube",
  };

  return (
    <span className={`feed-source-badge feed-source-${source}`}>
      {source === "stories" ? (
        <Rss size={12} aria-hidden="true" />
      ) : (
        <PlatformIcon platform={source} size={13} />
      )}
      {label ?? names[source]}
    </span>
  );
}
