import { afterEach, expect, it, vi } from "vitest";
import { sourceKind, sourcesForFeed } from "./collections";
import { isSource } from "./validation";
import {
  fetchYouTubeItems,
  fetchYouTubeSources,
  isYouTubeItem,
  type YouTubeItem,
  youtubeFilters,
  youtubeImageUrl,
  youtubeShelves,
} from "./youtubeCollection";

const video: YouTubeItem = {
  id: "video:dQw4w9WgXcQ",
  kind: "video",
  video_id: "dQw4w9WgXcQ",
  url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  title: "A captured video",
  text: "",
  channel: {
    id: "channel",
    name: "Newsroom",
    url: "https://www.youtube.com/@newsroom",
  },
  thumbnail_url: "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg",
  duration_text: "3:45",
  views_text: "12K views",
  published_text: "2 hours ago",
  observed_at: "2026-10-17T16:00:00Z",
  detail_capture: { status: "disabled" },
  discoveries: [
    {
      feed_url: "https://www.youtube.com/feed/news_destination",
      feed_name: "News · Top stories",
      feed_kind: "news",
      tab: "top_stories",
      shelf: "Today in science",
      rank: 2,
      region: "GB",
      playlist_id: "",
      observed_at: "2026-10-17T16:00:00Z",
      run_id: "capture-1",
    },
  ],
  references: [],
  reference_count: 0,
};
afterEach(() => vi.unstubAllGlobals());

it("rejects malformed nested discovery and reference payloads before rendering", () => {
  expect(isYouTubeItem(video)).toBe(true);
  expect(
    isYouTubeItem({
      ...video,
      discoveries: [{ ...video.discoveries[0], rank: -1 }],
    }),
  ).toBe(false);
  expect(isYouTubeItem({ ...video, references: [{ domain: "x.com" }] })).toBe(
    false,
  );
  expect(isYouTubeItem({ ...video, channel: null })).toBe(false);
  expect(isYouTubeItem({ ...video, kind: "live" })).toBe(false);
  expect(isYouTubeItem({ ...video, video_id: "javascript:alert(1)" })).toBe(
    false,
  );
});

it("permits only YouTube-owned thumbnails and local assets", () => {
  expect(youtubeImageUrl(video.thumbnail_url)).toBe(video.thumbnail_url);
  expect(youtubeImageUrl("https://yt3.ggpht.com/community=s800")).toBeTruthy();
  expect(youtubeImageUrl("/assets/youtube/thumbnail.jpg")).toContain(
    "/assets/youtube/thumbnail.jpg",
  );
  for (const url of [
    "javascript:alert(1)",
    "https://ytimg.com.attacker.test/pixel",
    "https://attacker.test/pixel",
    "http://i.ytimg.com/a.jpg",
    "https://user:secret@i.ytimg.com/a.jpg",
    "/api/reader/sources",
  ])
    expect(youtubeImageUrl(url)).toBeUndefined();
});

it("preserves story group context while separating Shorts and community posts", () => {
  const items = [
    video,
    { ...video, id: "short", kind: "short" as const },
    { ...video, id: "post", kind: "post" as const },
  ];
  const shelves = youtubeShelves(items, "").map(([, shelf]) => shelf);
  expect(shelves.map((shelf) => shelf.title)).toEqual([
    "Today in science",
    "Shorts",
    "Community posts",
  ]);
  expect(shelves[0].context).toBe("News · Top stories · GB");
  expect(youtubeShelves([video], "linked")[0][1].title).toBe("From your feeds");
  expect(video.discoveries[0].shelf).toBe("Today in science");
});

it("retains filters in requests and supports exact canonical item lookup", async () => {
  const fetch = vi.fn().mockResolvedValue(
    new Response(
      JSON.stringify({
        items: [video],
        next_cursor: "cursor-next",
        total: 5,
      }),
      { headers: { "Content-Type": "application/json" } },
    ),
  );
  vi.stubGlobal("fetch", fetch);
  const filters = youtubeFilters(
    new URLSearchParams(
      "view=youtube&feed=linked&format=short&q=%20science%20",
    ),
  );
  const page = await fetchYouTubeItems(
    filters,
    "cursor-first",
    undefined,
    video.id,
  );
  const url = new URL(fetch.mock.calls[0][0] as string, window.location.origin);
  expect(Object.fromEntries(url.searchParams)).toMatchObject({
    feed: "linked",
    kind: "short",
    q: "science",
    cursor: "cursor-first",
    id: video.id,
    limit: "30",
  });
  expect(page.items[0]).toEqual(video);
  expect(page.next_cursor).toBe("cursor-next");
});

it("reports malformed sources and API errors instead of empty feed success", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          feeds: [],
          channels: [{ id: "missing fields" }],
          total: 0,
          linked_count: 0,
        }),
      ),
    ),
  );
  await expect(fetchYouTubeSources()).rejects.toThrow(
    "unexpected YouTube sources",
  );
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ detail: "Database unavailable" }), {
        status: 503,
      }),
    ),
  );
  await expect(
    fetchYouTubeItems({ feed: "", channel: "", kind: "", q: "" }),
  ).rejects.toThrow("Database unavailable");
});

it("recognizes generic YouTube sources without mixing them into blog filters", () => {
  const source = {
    domain: "youtube.com",
    kind: "youtube" as const,
    name: "YouTube",
    tags: [],
    count: 2,
  };
  expect(isSource(source)).toBe(true);
  expect(sourceKind("www.youtube.com")).toBe("youtube");
  expect(sourcesForFeed([source], "stories")).toEqual([]);
  expect(sourcesForFeed([source], "all")).toEqual([source]);
});

it("keeps full safe post destinations separate from preview labels", async () => {
  const { youtubePostLinks, youtubeLocalVideoUrl } = await import(
    "./youtubeCollection"
  );
  const segments = youtubePostLinks(
    "Read https://news.example.com/story?q=world. Then javascript:alert(1) and https://user:password@example.com/private",
  );
  expect(
    segments.filter((segment) => segment.href).map((segment) => segment.href),
  ).toEqual(["https://news.example.com/story?q=world"]);
  expect(segments.map((segment) => segment.text).join("")).toContain(
    "Then javascript:alert(1)",
  );
  expect(youtubeLocalVideoUrl("/assets/video.mp4")).toContain(
    "/assets/video.mp4",
  );
  expect(youtubeLocalVideoUrl("https://i.ytimg.com/video.mp4")).toBeUndefined();
  expect(youtubeLocalVideoUrl("//evil.test/assets/video.mp4")).toBeUndefined();
});
