import type { FeedKind, Section, Source, XAccount } from "./types";

export function sourceLabel(name: string): string {
  return name.replace(/^www\./i, "");
}

export const feedLabels: Record<FeedKind, string> = {
  all: "All",
  stories: "Blogs",
  x: "X",
  reddit: "Reddit",
  youtube: "YouTube",
  hacker_news: "Hacker News",
};

export function sectionFromView(view: string | null): Section {
  switch (view) {
    case "stories":
    case "x":
    case "reddit":
    case "youtube":
    case "hacker_news":
    case "sources":
    case "settings":
      return view;
    default:
      return "all";
  }
}

export function sourceKind(domain: string): Exclude<FeedKind, "all"> {
  switch (domain.toLowerCase().replace(/^www\./, "")) {
    case "news.ycombinator.com":
    case "hckrnews.com":
      return "hacker_news";
    case "youtube.com":
      return "youtube";
    case "reddit.com":
      return "reddit";
    case "x.com":
    case "twitter.com":
      return "x";
    default:
      return "stories";
  }
}

export function sourceFeedKind(domain: string): FeedKind {
  return sourceKind(domain);
}

export function sourcesForFeed(sources: Source[], kind: FeedKind): Source[] {
  return sources.filter((source) => {
    const collection = source.kind === "rss" ? "stories" : source.kind;

    return (
      kind === "all" ||
      (kind === "stories"
        ? ["stories", "hacker_news"].includes(
            collection ?? sourceKind(source.domain),
          )
        : (collection ?? sourceKind(source.domain)) === kind)
    );
  });
}

export function collectedAccounts(sources: Source[]): XAccount[] {
  const accounts = new Map<string, XAccount>();

  for (const source of sourcesForFeed(sources, "x")) {
    for (const account of source.accounts ?? []) {
      const handle = account.handle.toLowerCase();
      const existing = accounts.get(handle);
      accounts.set(handle, {
        ...account,
        handle,
        count: (existing?.count ?? 0) + account.count,
        name:
          existing && existing.name.toLowerCase() !== handle
            ? existing.name
            : account.name,
        icon_url: existing?.icon_url ?? account.icon_url,
        // The profile total belongs to the account, not each stored domain.
        total_posts:
          source.domain === "x.com"
            ? (account.total_posts ?? existing?.total_posts)
            : (existing?.total_posts ?? account.total_posts),
      });
    }
  }

  return [...accounts.values()].sort((a, b) =>
    a.handle.localeCompare(b.handle),
  );
}
