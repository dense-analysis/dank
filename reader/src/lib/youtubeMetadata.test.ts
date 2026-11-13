import { afterEach, expect, it, vi } from "vitest";
import {
  collectYouTubeMetadata,
  type YouTubeMetadataResult,
} from "./youtubeMetadata";

const captured: YouTubeMetadataResult = {
  video_id: "NnV_cWeoo5Q",
  status: "captured",
  provider: "youtube_oembed",
  available: true,
  observed_at: "2026-10-17T20:00:00Z",
  captured_at: "2026-10-17T20:00:00Z",
  retry_after: "2026-10-24T20:00:00Z",
  cached: false,
};

afterEach(() => vi.unstubAllGlobals());

it("collects only a validated canonical video ID with explicit JSON intent", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValue(new Response(JSON.stringify(captured)));
  vi.stubGlobal("fetch", fetch);
  expect(await collectYouTubeMetadata("NnV_cWeoo5Q")).toEqual(captured);
  expect(fetch.mock.calls[0][0]).toBe(
    "/api/reader/youtube/metadata/NnV_cWeoo5Q",
  );
  const options = fetch.mock.calls[0][1] as RequestInit;
  expect(options.method).toBe("POST");
  expect(new Headers(options.headers).get("Content-Type")).toBe(
    "application/json",
  );
  expect(new Headers(options.headers).get("Accept")).toBe("application/json");
  await expect(collectYouTubeMetadata("https://example.com")).rejects.toThrow(
    "valid YouTube ID",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
});

it("reports collection errors", async () => {
  const fetch = vi.fn().mockResolvedValueOnce(
    new Response(JSON.stringify({ error: "Collection is unavailable." }), {
      status: 503,
    }),
  );
  vi.stubGlobal("fetch", fetch);
  await expect(collectYouTubeMetadata("NnV_cWeoo5Q")).rejects.toThrow(
    "Collection is unavailable.",
  );
});
