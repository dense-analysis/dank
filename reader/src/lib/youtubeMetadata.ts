import { request } from "./api";
import { isRecord } from "./validation";

export interface YouTubeMetadataCapture {
  status: "disabled" | "captured" | "failed" | "unavailable";
  provider: "youtube_oembed" | "";
  available: boolean;
  observed_at: string;
  captured_at: string;
  retry_after: string;
}

export interface YouTubeMetadataResult extends YouTubeMetadataCapture {
  video_id: string;
  cached: boolean;
}

export function isYouTubeMetadataCapture(
  value: unknown,
): value is YouTubeMetadataCapture {
  return (
    isRecord(value) &&
    ["disabled", "captured", "failed", "unavailable"].includes(
      String(value.status),
    ) &&
    (value.provider === "youtube_oembed" || value.provider === "") &&
    typeof value.available === "boolean" &&
    [value.observed_at, value.captured_at, value.retry_after].every(
      (date) =>
        typeof date === "string" &&
        (date === "" || Number.isFinite(Date.parse(date))),
    )
  );
}

export async function collectYouTubeMetadata(
  videoId: string,
): Promise<YouTubeMetadataResult> {
  if (!/^[A-Za-z0-9_-]{11}$/.test(videoId))
    throw new Error("This video does not have a valid YouTube ID.");

  const result = await request(`/youtube/metadata/${videoId}`, undefined, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
  if (
    !isYouTubeMetadataCapture(result) ||
    !isRecord(result) ||
    result.video_id !== videoId ||
    typeof result.cached !== "boolean" ||
    result.status === "disabled"
  )
    throw new Error(
      "The reader received an unexpected video details response. Please try again.",
    );

  return { ...result, video_id: videoId, cached: result.cached };
}
