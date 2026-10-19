import { request } from "./api";
import { isRecord } from "./validation";

export interface TranscriptSegment {
  start: number;
  duration: number;
  text: string;
}

export interface YouTubeTranscript {
  video_id: string;
  status: "not_collected" | "captured" | "unavailable" | "failed";
  observed_at: string;
  retry_after: string;
  language: string;
  language_name: string;
  generated: boolean;
  segments: TranscriptSegment[];
}

export async function fetchYouTubeTranscript(
  videoId: string,
  collect = false,
  signal?: AbortSignal,
): Promise<YouTubeTranscript> {
  if (!/^[A-Za-z0-9_-]{11}$/.test(videoId))
    throw new Error("This video does not have a valid YouTube ID.");

  const result = await request(
    `/youtube/transcript/${videoId}`,
    signal,
    collect
      ? {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
        }
      : undefined,
  );
  if (
    !isRecord(result) ||
    result.video_id !== videoId ||
    !["not_collected", "captured", "unavailable", "failed"].includes(
      String(result.status),
    ) ||
    typeof result.language !== "string" ||
    typeof result.language_name !== "string" ||
    typeof result.generated !== "boolean" ||
    ![result.observed_at, result.retry_after].every(
      (date) =>
        typeof date === "string" &&
        (date === "" || Number.isFinite(Date.parse(date))),
    ) ||
    !Array.isArray(result.segments) ||
    !result.segments.every(
      (segment) =>
        isRecord(segment) &&
        typeof segment.text === "string" &&
        typeof segment.start === "number" &&
        Number.isFinite(segment.start) &&
        segment.start >= 0 &&
        typeof segment.duration === "number" &&
        Number.isFinite(segment.duration) &&
        segment.duration >= 0,
    ) ||
    (result.status === "captured" && result.segments.length === 0)
  )
    throw new Error("The reader received an unexpected transcript response.");

  return result as unknown as YouTubeTranscript;
}
