import { request } from "./api";
import { isRecord } from "./validation";

export type YouTubeKind = "video" | "short" | "post";
export interface YouTubeDiscovery {
  feed_url: string;
  feed_name: string;
  feed_kind: string;
  tab: string;
  shelf: string;
  rank: number | null;
  region: string;
  playlist_id: string;
  observed_at: string;
  run_id: string;
}
export interface YouTubeReference {
  domain: string;
  post_id: string;
  url: string;
  title: string;
  source: string;
  reader_url: string;
}
export interface YouTubeItem {
  id: string;
  kind: YouTubeKind;
  video_id: string | null;
  url: string;
  title: string;
  text: string;
  channel: { id: string; name: string; url: string };
  thumbnail_url: string | null;
  video_url?: string | null;
  duration_text: string;
  views_text: string;
  published_text: string;
  observed_at: string;
  detail_capture: { status: "disabled" | "captured" | "failed" };
  discoveries: YouTubeDiscovery[];
  references: YouTubeReference[];
  reference_count: number;
}
export interface YouTubeFeed {
  id: string;
  name: string;
  kind: string;
  url: string;
  count: number;
  region: string;
  tab: string;
}
export interface YouTubeSources {
  feeds: YouTubeFeed[];
  channels: { id: string; name: string; url: string; count: number }[];
  total: number;
  linked_count: number;
}
export interface YouTubePage {
  items: YouTubeItem[];
  next_cursor: string | null;
  total: number;
}
export interface YouTubeFilters {
  feed: string;
  channel: string;
  kind: "" | YouTubeKind;
  q: string;
}

function strings(value: Record<string, unknown>, keys: string[]) {
  return keys.every((key) => typeof value[key] === "string");
}
function count(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
export function isYouTubeItem(value: unknown): value is YouTubeItem {
  return (
    isRecord(value) &&
    strings(value, [
      "id",
      "url",
      "title",
      "text",
      "duration_text",
      "views_text",
      "published_text",
      "observed_at",
    ]) &&
    ["video", "short", "post"].includes(String(value.kind)) &&
    (value.video_id === null ||
      (typeof value.video_id === "string" &&
        /^[A-Za-z0-9_-]{11}$/.test(value.video_id))) &&
    isRecord(value.channel) &&
    strings(value.channel, ["id", "name", "url"]) &&
    (value.thumbnail_url === null || typeof value.thumbnail_url === "string") &&
    (value.video_url === undefined ||
      value.video_url === null ||
      typeof value.video_url === "string") &&
    isRecord(value.detail_capture) &&
    ["disabled", "captured", "failed"].includes(
      String(value.detail_capture.status),
    ) &&
    Array.isArray(value.discoveries) &&
    value.discoveries.every(
      (item) =>
        isRecord(item) &&
        strings(item, [
          "feed_url",
          "feed_name",
          "feed_kind",
          "tab",
          "shelf",
          "region",
          "playlist_id",
          "observed_at",
          "run_id",
        ]) &&
        (item.rank === null || count(item.rank)),
    ) &&
    Array.isArray(value.references) &&
    value.references.every(
      (item) =>
        isRecord(item) &&
        strings(item, [
          "domain",
          "post_id",
          "url",
          "title",
          "source",
          "reader_url",
        ]),
    ) &&
    count(value.reference_count)
  );
}

export async function fetchYouTubeSources(
  signal?: AbortSignal,
): Promise<YouTubeSources> {
  const body = await request("/youtube/sources", signal);
  if (
    !isRecord(body) ||
    !count(body.total) ||
    !count(body.linked_count) ||
    !Array.isArray(body.feeds) ||
    !body.feeds.every(
      (item) =>
        isRecord(item) &&
        strings(item, ["id", "name", "kind", "url", "region", "tab"]) &&
        count(item.count),
    ) ||
    !Array.isArray(body.channels) ||
    !body.channels.every(
      (item) =>
        isRecord(item) &&
        strings(item, ["id", "name", "url"]) &&
        count(item.count),
    )
  ) {
    throw new Error(
      "The reader received an unexpected YouTube sources response. Please try again.",
    );
  }

  return body as unknown as YouTubeSources;
}
export async function fetchYouTubeItems(
  filters: YouTubeFilters,
  cursor?: string,
  signal?: AbortSignal,
  id?: string,
): Promise<YouTubePage> {
  const params = new URLSearchParams({ limit: "30" });
  for (const [key, value] of Object.entries(filters)) {
    if (value) params.set(key, value);
  }
  if (cursor) params.set("cursor", cursor);
  if (id) params.set("id", id);
  const body = await request(`/youtube?${params}`, signal);
  if (
    !isRecord(body) ||
    !Array.isArray(body.items) ||
    !body.items.every(isYouTubeItem) ||
    !count(body.total) ||
    (body.next_cursor !== null && typeof body.next_cursor !== "string")
  ) {
    throw new Error(
      "The reader received an unexpected YouTube feed response. Please try again.",
    );
  }

  return {
    items: body.items,
    next_cursor: body.next_cursor,
    total: body.total,
  };
}

export function youtubeFilters(params: URLSearchParams): YouTubeFilters {
  const kind = params.get("format") ?? "";

  return {
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
export function youtubeShelves(items: YouTubeItem[], feed: string) {
  const shelves = new Map<
    string,
    { title: string; context: string; kind: YouTubeKind; items: YouTubeItem[] }
  >();
  for (const item of items) {
    const discovery = feed === "linked" ? undefined : itemDiscovery(item, feed);
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

  return [...shelves.entries()];
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
