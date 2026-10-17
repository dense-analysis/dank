import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vitest";
import type { YouTubeItem } from "../lib/youtubeCollection";
import { YouTubeCard } from "./YouTubeCard";

const hype = "https://www.youtube.com/feed/hype";
const news = "https://www.youtube.com/feed/news_destination";
const channel = "https://www.youtube.com/@newsroom";
const item: YouTubeItem = {
  id: "video:dQw4w9WgXcQ",
  kind: "video",
  video_id: "dQw4w9WgXcQ",
  url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  title: "A captured video",
  text: "",
  channel: { id: "newsroom", name: "Newsroom", url: channel },
  thumbnail_url: null,
  duration_text: "3:45",
  views_text: "12K views",
  published_text: "2 hours ago",
  observed_at: "2026-10-17T16:00:00Z",
  detail_capture: { status: "disabled" },
  discoveries: [
    [hype, "hype"],
    [news, "news"],
    [channel, "channel"],
  ].map(([feed_url, feed_kind]) => ({
    feed_url,
    feed_kind,
    feed_name: "Captured feed",
    tab: "",
    shelf: "Videos",
    rank: 2,
    region: "GB",
    playlist_id: "",
    observed_at: "2026-10-17T16:00:00Z",
    run_id: "capture-1",
  })),
  references: [],
  reference_count: 0,
};

it.each(["", "linked", channel, news])(
  "does not turn capture order into a rank outside the selected Hype feed (%s)",
  (feed) => {
    const card = renderToStaticMarkup(
      <YouTubeCard item={item} feed={feed} open={() => {}} href="/reader/" />,
    );

    expect(card).not.toContain("yt-rank");
  },
);

it("shows the captured ranking when viewing its Hype feed", () => {
  const card = renderToStaticMarkup(
    <YouTubeCard item={item} feed={hype} open={() => {}} href="/reader/" />,
  );

  expect(card).toContain('class="yt-rank">#2');
});
