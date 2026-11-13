import { apiPost } from "./api";
import type { components } from "./api.gen";

export type YouTubeMetadataCapture = components["schemas"]["MetadataStateOut"];
export type YouTubeMetadataResult = components["schemas"]["VideoMetadataOut"];

export function collectYouTubeMetadata(
  videoId: string,
): Promise<YouTubeMetadataResult> {
  if (!/^[A-Za-z0-9_-]{11}$/.test(videoId))
    return Promise.reject(
      new Error("This video does not have a valid YouTube ID."),
    );

  return apiPost(
    "/api/reader/youtube/metadata/{video_id}",
    {},
    { params: { video_id: videoId } },
  );
}
