import type { RedditFlair, RedditPost, Source } from "./types";

export function availableFlairs(
  sources: Source[],
  community: string,
): RedditFlair[] {
  const counts = new Map<string, number>();

  for (const source of sources) {
    if (source.kind !== "reddit") continue;

    const flairs = community
      ? source.communities?.find(
          (item) => item.name.toLowerCase() === community.toLowerCase(),
        )?.flairs
      : source.flairs;

    for (const flair of flairs ?? []) {
      counts.set(flair.name, (counts.get(flair.name) ?? 0) + flair.count);
    }
  }

  return [...counts]
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

export function redditVideos(reddit?: RedditPost | null) {
  return reddit?.videos?.length
    ? reddit.videos
    : reddit?.video
      ? [reddit.video]
      : [];
}
