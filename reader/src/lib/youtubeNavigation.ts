import type { YouTubeFeed, YouTubeSources } from "./youtubeCollection";

const newsOrder = [
  "top_stories",
  "business",
  "world",
  "sports",
  "entertainment",
];
const newsLabels: Record<string, string> = {
  top_stories: "Top stories",
  business: "Business",
  world: "World",
  sports: "Sports",
  entertainment: "Entertainment",
};

export function youtubeFeedLabel(feed: YouTubeFeed) {
  if (feed.kind === "home") return "Recommendations";
  if (feed.kind === "news")
    return newsLabels[feed.tab] ?? feed.name.replace(/^News\s*[·-]\s*/, "");
  if (feed.kind === "hype") {
    const region = feed.region.toUpperCase();
    if (["GB", "UK", "UNITED KINGDOM"].includes(region)) return "In UK";
    if (["US", "USA", "UNITED STATES"].includes(region)) return "In USA";

    return feed.region ? `In ${feed.region}` : "Top 100";
  }

  return feed.name;
}

export function youtubeGroupFeeds(
  sources: YouTubeSources | undefined,
  kind: "news" | "hype",
) {
  const feeds = sources?.feeds.filter((feed) => feed.kind === kind) ?? [];
  if (kind === "news")
    feeds.sort((a, b) => {
      const position = (tab: string) =>
        newsOrder.includes(tab) ? newsOrder.indexOf(tab) : newsOrder.length;

      return position(a.tab) - position(b.tab);
    });
  if (kind === "hype")
    feeds.sort(
      (a, b) =>
        Number(youtubeFeedLabel(b) === "In UK") -
        Number(youtubeFeedLabel(a) === "In UK"),
    );

  return feeds;
}
