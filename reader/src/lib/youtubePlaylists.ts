import { apiGet } from "./api";
import type { components } from "./api.gen";

type Schemas = components["schemas"];

export type YouTubePlaylistSort = "recent" | "az";
export type YouTubePlaylist = Schemas["PlaylistOut"];
export type YouTubePlaylistEntry = Schemas["PlaylistItemOut"];
export type YouTubePlaylistPage = Schemas["PlaylistsResponse"];
export type YouTubePlaylistDetail = Schemas["PlaylistResponse"];

export function fetchYouTubePlaylists(
  q = "",
  cursor?: string,
  signal?: AbortSignal,
  sort: YouTubePlaylistSort = "recent",
): Promise<YouTubePlaylistPage> {
  const query = new URLSearchParams({ limit: "30", sort });
  if (q) query.set("q", q);
  if (cursor) query.set("cursor", cursor);

  return apiGet("/api/reader/youtube/playlists", { query, signal });
}

export function fetchYouTubePlaylist(
  id: string,
  cursor?: string,
  signal?: AbortSignal,
  q = "",
): Promise<YouTubePlaylistDetail> {
  const query = new URLSearchParams({ limit: "30" });
  if (q) query.set("q", q);
  if (cursor) query.set("cursor", cursor);

  return apiGet("/api/reader/youtube/playlists/{playlist_id}", {
    params: { playlist_id: id },
    query,
    signal,
  });
}
