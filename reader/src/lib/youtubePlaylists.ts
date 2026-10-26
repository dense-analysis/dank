import { request } from "./api";
import { isRecord } from "./validation";
import { isYouTubeItem, type YouTubeItem } from "./youtubeCollection";

export type YouTubePlaylistSort = "recent" | "az";

export interface YouTubePlaylist {
  id: string;
  title: string;
  url: string;
  thumbnail_url: string | null;
  channel: { id: string; name: string; url: string };
  video_count: number | null;
  collected_count: number;
  status: "complete" | "partial" | "failed" | "not_collected" | "discovered";
  complete: boolean;
  observed_at: string;
  checked_at: string;
  order: string;
  snapshot_id: string;
}
export interface YouTubePlaylistEntry extends YouTubeItem {
  playlist_position: number;
  playlist_entry_id: string;
  available: boolean;
}
export interface YouTubePlaylistPage {
  playlists: YouTubePlaylist[];
  total: number;
  next_cursor: string | null;
}
export interface YouTubePlaylistDetail {
  playlist: YouTubePlaylist;
  items: YouTubePlaylistEntry[];
  total: number;
  next_cursor: string | null;
}
function count(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
export function isYouTubePlaylist(value: unknown): value is YouTubePlaylist {
  return (
    isRecord(value) &&
    [
      "id",
      "title",
      "url",
      "observed_at",
      "checked_at",
      "order",
      "snapshot_id",
    ].every((key) => typeof value[key] === "string") &&
    (value.thumbnail_url === null || typeof value.thumbnail_url === "string") &&
    isRecord(value.channel) &&
    typeof value.channel.id === "string" &&
    typeof value.channel.name === "string" &&
    typeof value.channel.url === "string" &&
    (value.video_count === null || count(value.video_count)) &&
    count(value.collected_count) &&
    ["complete", "partial", "failed", "not_collected", "discovered"].includes(
      String(value.status),
    ) &&
    typeof value.complete === "boolean"
  );
}
function isPage(value: unknown): value is Record<string, unknown> {
  return (
    isRecord(value) &&
    count(value.total) &&
    (value.next_cursor === null || typeof value.next_cursor === "string")
  );
}
export async function fetchYouTubePlaylists(
  q = "",
  cursor?: string,
  signal?: AbortSignal,
  sort: YouTubePlaylistSort = "recent",
): Promise<YouTubePlaylistPage> {
  const params = new URLSearchParams({ limit: "30", sort });
  if (q) params.set("q", q);
  if (cursor) params.set("cursor", cursor);
  const body = await request(`/youtube/playlists?${params}`, signal);
  if (
    !isPage(body) ||
    !Array.isArray(body.playlists) ||
    !body.playlists.every(isYouTubePlaylist)
  ) {
    throw new Error(
      "The reader received an unexpected playlist response. Please try again.",
    );
  }

  return body as unknown as YouTubePlaylistPage;
}
export async function fetchYouTubePlaylist(
  id: string,
  cursor?: string,
  signal?: AbortSignal,
  q = "",
): Promise<YouTubePlaylistDetail> {
  const params = new URLSearchParams({ limit: "30" });
  if (q) params.set("q", q);
  if (cursor) params.set("cursor", cursor);
  const body = await request(
    `/youtube/playlists/${encodeURIComponent(id)}?${params}`,
    signal,
  );
  if (
    !isPage(body) ||
    !isYouTubePlaylist(body.playlist) ||
    !Array.isArray(body.items) ||
    !body.items.every(
      (entry) =>
        isRecord(entry) &&
        isYouTubeItem(entry) &&
        count(entry.playlist_position) &&
        entry.playlist_position > 0 &&
        typeof entry.playlist_entry_id === "string" &&
        typeof entry.available === "boolean",
    )
  ) {
    throw new Error(
      "The reader received an unexpected playlist video response. Please try again.",
    );
  }

  return body as unknown as YouTubePlaylistDetail;
}
