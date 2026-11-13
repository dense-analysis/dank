import { afterEach, expect, it, vi } from "vitest";
import { feedSource, youtubeItem } from "./api.fixture";
import { sourceKind, sourcesForFeed } from "./collections";
import {
  fetchYouTubeItems,
  formatYouTubeViews,
  type YouTubeItem,
  youtubeFilters,
  youtubeImageUrl,
  youtubeShelves,
} from "./youtubeCollection";

const video: YouTubeItem = youtubeItem({
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
});

afterEach(() => vi.unstubAllGlobals());

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

it("reports API errors instead of empty feed success", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ error: "Database unavailable" }), {
        status: 503,
      }),
    ),
  );
  await expect(
    fetchYouTubeItems({ feed: "", channel: "", kind: "", q: "" }),
  ).rejects.toThrow("Database unavailable");
});

it("recognizes generic YouTube sources without mixing them into blog filters", () => {
  const source = feedSource({
    domain: "youtube.com",
    kind: "youtube",
    name: "YouTube",
    tags: [],
    count: 2,
  });
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

it("channel shelves ignore unrelated recommendation ranks and topics", () => {
  const shelf = youtubeShelves([video], "", true)[0][1];
  expect(shelf.title).toBe("Videos");
  expect(shelf.context).toBe("");
});

it.each([
  ["251 thousand views", "251K views"],
  ["4.8 million views", "4.8M views"],
  ["1.2 billion views", "1.2B views"],
  ["4.1 thousand views", "4.1K views"],
  ["12,345 views", "12.3K views"],
  ["1,866,004 views", "1.8M views"],
  ["1.25k views", "1.2K views"],
  ["1.2m views", "1.2M views"],
  ["2.5 THOUSAND views", "2.5K views"],
  ["4.8\u00a0million views", "4.8M views"],
  ["1,200 watching now", "1.2K watching now"],
  ["0 views", "0 views"],
  ["1 view", "1 view"],
  ["No views", "No views"],
  ["", ""],
  ["4,8 million views", "4,8 million views"],
  ["4,8 Mio. Aufrufe", "4,8 Mio. Aufrufe"],
  ["123.4 views", "123.4 views"],
])("formats captured count %s as %s", (source, expected) => {
  expect(formatYouTubeViews(source)).toBe(expected);
});

it("keeps recommendation history chronological across changing shelf labels", () => {
  const feed = "https://www.youtube.com/";
  const items = ["Recent", "Different shelf", "Earlier"].map(
    (title, index) => ({
      ...video,
      id: `video:${index}`,
      title,
      discoveries: [
        {
          ...video.discoveries[0],
          feed_url: feed,
          feed_kind: "home",
          feed_name: index === 2 ? "Home recommendations" : "Recommendations",
          shelf: index === 1 ? "More for you" : "Recommended",
        },
      ],
    }),
  );
  const shelves = youtubeShelves(items, feed);
  expect(shelves).toHaveLength(1);
  expect(shelves[0][1].title).toBe("Videos");
  expect(shelves[0][1].items.map((item) => item.title)).toEqual([
    "Recent",
    "Different shelf",
    "Earlier",
  ]);
  expect(items[1].discoveries[0].shelf).toBe("More for you");
});
