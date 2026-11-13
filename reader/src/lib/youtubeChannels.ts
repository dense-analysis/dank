import { apiGet } from "./api";
import type { components } from "./api.gen";
import type { YouTubeFilters } from "./youtubeCollection";

export type YouTubeChannelGroup = components["schemas"]["YouTubeChannelOut"];
export type YouTubeChannelPage =
  components["schemas"]["YouTubeChannelsResponse"];

export function fetchYouTubeChannels(
  filters: YouTubeFilters,
  cursor?: string,
  signal?: AbortSignal,
): Promise<YouTubeChannelPage> {
  const query = new URLSearchParams({ limit: "20" });
  for (const [key, value] of Object.entries(filters)) {
    if (value) query.set(key, value);
  }
  if (cursor) query.set("cursor", cursor);

  return apiGet("/api/reader/youtube/channels", { query, signal });
}
