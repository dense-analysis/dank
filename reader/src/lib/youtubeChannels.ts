import { request } from "./api";
import { isRecord } from "./validation";
import {
  isYouTubeItem,
  type YouTubeFilters,
  type YouTubeItem,
} from "./youtubeCollection";

export interface YouTubeChannelGroup {
  id: string;
  name: string;
  url: string;
  configured: boolean;
  count: number;
  video_count: number;
  short_count: number;
  post_count: number;
  items: YouTubeItem[];
}
export interface YouTubeChannelPage {
  channels: YouTubeChannelGroup[];
  total: number;
  unattributed_items: number;
  next_cursor: string | null;
}
function count(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
export async function fetchYouTubeChannels(
  filters: YouTubeFilters,
  cursor?: string,
  signal?: AbortSignal,
): Promise<YouTubeChannelPage> {
  const params = new URLSearchParams({ limit: "20" });
  for (const [key, value] of Object.entries(filters)) {
    if (value) params.set(key, value);
  }
  if (cursor) params.set("cursor", cursor);
  const body = await request(`/youtube/channels?${params}`, signal);
  if (
    !isRecord(body) ||
    !Array.isArray(body.channels) ||
    !body.channels.every(
      (channel) =>
        isRecord(channel) &&
        ["id", "name", "url"].every(
          (key) => typeof channel[key] === "string",
        ) &&
        ["count", "video_count", "short_count", "post_count"].every((key) =>
          count(channel[key]),
        ) &&
        typeof channel.configured === "boolean" &&
        Array.isArray(channel.items) &&
        channel.items.every(isYouTubeItem),
    ) ||
    !count(body.total) ||
    !count(body.unattributed_items) ||
    (body.next_cursor !== null && typeof body.next_cursor !== "string")
  ) {
    throw new Error("The reader received an unexpected channel response.");
  }
  return body as unknown as YouTubeChannelPage;
}
