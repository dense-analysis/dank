import { apiGet, apiPost } from "./api";
import type { components } from "./api.gen";

export type TranscriptSegment = components["schemas"]["TranscriptSegmentOut"];
export type YouTubeTranscript = components["schemas"]["VideoTranscriptOut"];

const TRANSCRIPT = "/api/reader/youtube/transcript/{video_id}";

export function fetchYouTubeTranscript(
  videoId: string,
  collect = false,
  signal?: AbortSignal,
): Promise<YouTubeTranscript> {
  if (!/^[A-Za-z0-9_-]{11}$/.test(videoId))
    return Promise.reject(
      new Error("This video does not have a valid YouTube ID."),
    );

  const options = { params: { video_id: videoId }, signal };

  return collect
    ? apiPost(TRANSCRIPT, {}, options)
    : apiGet(TRANSCRIPT, options);
}
