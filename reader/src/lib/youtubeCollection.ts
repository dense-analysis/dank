import { apiGet } from "./api";
import type { components } from "./api.gen";
import { formatCount } from "./counts";

type Schemas = components["schemas"];

export type YouTubeItem = Schemas["YouTubeItemOut"];
export type YouTubeKind = YouTubeItem["kind"];
export type YouTubeDiscovery = Schemas["DiscoveryOut"];
export type YouTubeReference = Schemas["VideoReferenceOut"];
export type YouTubeTitleVersion = Schemas["TitleVersionOut"];
export type YouTubeTitlePair = Schemas["ThumbnailTitleOut"];
export type YouTubeThumbnailVariant = Schemas["ThumbnailVariantOut"];
export type YouTubeFeed = Schemas["YouTubeFeedOut"];
export type YouTubeSources = Schemas["YouTubeSourcesResponse"];
export type YouTubePage = Schemas["YouTubeResponse"];
export interface YouTubeFilters {
  channel_scope?: "configured";
  sort?: "newest" | "oldest" | "most-linked";
  feed: string;
  channel: string;
  kind: "" | YouTubeKind;
  q: string;
}

export function fetchYouTubeSources(
  signal?: AbortSignal,
): Promise<YouTubeSources> {
  return apiGet("/api/reader/youtube/sources", { signal });
}

export function fetchYouTubeItems(
  filters: YouTubeFilters,
  cursor?: string,
  signal?: AbortSignal,
  id?: string,
  limit = 30,
): Promise<YouTubePage> {
  const query = new URLSearchParams({ limit: String(limit) });
  for (const [key, value] of Object.entries(filters)) {
    if (value) query.set(key, value);
  }
  if (cursor) query.set("cursor", cursor);
  if (id) query.set("id", id);

  return apiGet("/api/reader/youtube", { query, signal });
}

export function youtubeFilters(params: URLSearchParams): YouTubeFilters {
  const kind = params.get("format") ?? "";
  const sort = params.get("sort");

  return {
    ...(params.get("feed") === "linked"
      ? {
          sort:
            sort === "oldest" || sort === "most-linked"
              ? sort
              : ("newest" as const),
        }
      : {}),
    ...(params.get("channel_scope") === "configured"
      ? { channel_scope: "configured" as const }
      : {}),
    feed: params.get("feed") ?? "",
    channel: params.get("channel") ?? "",
    kind: kind === "video" || kind === "short" || kind === "post" ? kind : "",
    q: (params.get("q") ?? "").trim(),
  };
}
export function itemDiscovery(item: YouTubeItem, feed: string) {
  return (
    item.discoveries.find((discovery) => discovery.feed_url === feed) ??
    item.discoveries[0]
  );
}
export function youtubeShelves(
  items: YouTubeItem[],
  feed: string,
  channel = false,
) {
  const shelves = new Map<
    string,
    { title: string; context: string; kind: YouTubeKind; items: YouTubeItem[] }
  >();
  for (const item of items) {
    // Home history stays chronological even when older shelf labels differ.
    const discovery =
      feed === "linked" || feed === "https://www.youtube.com/" || channel
        ? undefined
        : itemDiscovery(item, feed);
    const title =
      item.kind === "short"
        ? "Shorts"
        : item.kind === "post"
          ? "Community posts"
          : discovery?.shelf ||
            (feed === "linked" ? "From your feeds" : "Videos");
    const context =
      item.kind === "video" && discovery
        ? [discovery.feed_name, discovery.region].filter(Boolean).join(" · ")
        : "";
    const key = `${item.kind}:${context}:${title}`;
    const shelf = shelves.get(key) ?? {
      title,
      context,
      kind: item.kind,
      items: [],
    };
    shelf.items.push(item);
    shelves.set(key, shelf);
  }

  const order = { video: 0, short: 1, post: 2 };
  return [...shelves.entries()].sort(
    (a, b) => order[a[1].kind] - order[b[1].kind],
  );
}

/** Collected thumbnails may only load from YouTube image hosts or our assets. */
export function youtubeImageUrl(value: string | null): string | undefined {
  if (!value) return undefined;
  try {
    const url = new URL(value, window.location.origin);
    if (url.username || url.password) return undefined;
    if (
      url.origin === window.location.origin &&
      url.pathname.startsWith("/assets/")
    )
      return url.href;
    if (url.protocol !== "https:" || url.port) return undefined;
    if (
      ["ytimg.com", "ggpht.com", "googleusercontent.com"].some(
        (host) => url.hostname === host || url.hostname.endsWith(`.${host}`),
      )
    )
      return url.href;
  } catch {
    return undefined;
  }

  return undefined;
}

export function youtubeLocalVideoUrl(
  value?: string | null,
): string | undefined {
  if (!value) return undefined;
  try {
    const url = new URL(value, window.location.origin);
    if (
      url.origin === window.location.origin &&
      url.pathname.startsWith("/assets/") &&
      !url.username &&
      !url.password
    )
      return url.href;
  } catch {
    return undefined;
  }

  return undefined;
}

/** Keep original destinations even when a feed preview truncates the link label. */
export function youtubePostLinks(
  text: string,
): { text: string; start: number; href?: string }[] {
  const segments: { text: string; start: number; href?: string }[] = [];
  let start = 0;
  for (const match of text.matchAll(/https?:\/\/[^\s<>"']+/gi)) {
    const label = match[0].replace(/[),.!?;:]+$/, "");
    if (match.index > start)
      segments.push({ text: text.slice(start, match.index), start });
    let href: string | undefined;
    try {
      const url = new URL(label);
      if (!url.username && !url.password) href = url.href;
    } catch {
      // Malformed destinations remain readable text.
    }
    segments.push({ text: label, start: match.index, href });
    start = match.index + label.length;
  }
  if (start < text.length) segments.push({ text: text.slice(start), start });

  return segments;
}

export function youtubeThumbnailSources(item: YouTubeItem): string[] {
  return [
    ...new Set(
      [
        item.thumbnail_url,
        ...(item.thumbnail_variants ?? []).map((version) => version.url),
      ]
        .map(youtubeImageUrl)
        .filter((url): url is string => !!url),
    ),
  ];
}

/** Recognize English counts only; preserve source labels we cannot interpret. */
export function formatYouTubeViews(text: string): string {
  const match = text
    .trim()
    .match(
      /^((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)\s*(thousand|million|billion|[KMB])?\s+(views?|watching(?: now)?)$/i,
    );
  if (!match) return text;
  const units: Record<string, number> = {
    k: 1_000,
    thousand: 1_000,
    m: 1_000_000,
    million: 1_000_000,
    b: 1_000_000_000,
    billion: 1_000_000_000,
  };
  const number = Number(match[1].replaceAll(",", ""));
  const unit = units[match[2]?.toLowerCase()] ?? 1;
  // Source decimals represent rounded whole counts; remove multiplication noise.
  const value = unit === 1 ? number : Math.round(number * unit);

  return Number.isSafeInteger(value)
    ? `${formatCount(value)} ${match[3]}`
    : text;
}
