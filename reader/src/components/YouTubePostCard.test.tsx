import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ReaderPreferencesProvider } from "../hooks/useReaderPreferences";
import { defaultFilters } from "../lib/storage";
import type { Post } from "../lib/types";
import { YouTubePostCard } from "./YouTubePostCard";

vi.mock("./YouTubePlayer", () => ({
  YouTubePlayer: ({
    src,
    positionKey,
  }: {
    src: string;
    positionKey: string;
  }) => (
    <iframe src={src} title="Remote player" data-position-key={positionKey} />
  ),
}));
vi.mock("./YouTubeLocalVideo", () => ({
  YouTubeLocalVideo: ({
    src,
    positionKey,
  }: {
    src: string;
    positionKey: string;
  }) => (
    <video src={src} data-position-key={positionKey}>
      <track kind="captions" />
    </video>
  ),
}));

const post = {
  id: "video:dQw4w9WgXcQ",
  domain: "youtube.com",
  source: "youtube",
  url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  title: "A walk through the new station",
  author: "City Films",
  excerpt: "A walk through the new station",
  html: "",
  created_at: "2026-10-21T10:00:00Z",
  thumbnail: null,
  media: [],
  youtube: {
    kind: "video",
    video_id: "dQw4w9WgXcQ",
    channel: {
      id: "city-films",
      name: "City Films",
      url: "https://www.youtube.com/@cityfilms",
    },
    duration_text: "12:34",
    views_text: "12345 views",
    thumbnail_url: "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg",
    video_url: null,
  },
} satisfies Post;
const href =
  "/reader/?platform=youtube&platform=reddit&q=station&youtube_item=video%3AdQw4w9WgXcQ";
let container: HTMLDivElement;
let root: Root;
const open = vi.fn();

beforeEach(() => {
  localStorage.clear();
  open.mockClear();
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("matchMedia", () => ({
    matches: true,
    addEventListener() {},
    removeEventListener() {},
  }));
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function render(value: Post = post) {
  await act(() =>
    root.render(
      <ReaderPreferencesProvider>
        <YouTubePostCard
          post={value}
          read={false}
          href={href}
          open={open}
          query="station"
          filters={defaultFilters}
          selectAuthor={() => {}}
        />
      </ReaderPreferencesProvider>,
    ),
  );
}

it("presents video metadata without article status or starting playback", async () => {
  await render();
  expect(container.textContent).toContain("YouTube");
  expect(container.textContent).toContain("City Films");
  expect(container.textContent).toContain("12:34");
  expect(container.textContent).toContain("12.3K views");
  expect(container.querySelector("iframe, video")).toBeNull();
  expect(container.querySelector(".collection-badge")).toBeNull();
  expect(container.textContent).not.toContain("min read");
  expect(container.querySelector(".post-open p")).toBeNull();
  expect(
    container.querySelector("a[data-card-link]")?.getAttribute("href"),
  ).toBe(href);
  expect(container.querySelector("mark")?.textContent).toBe("station");
});

it("keeps inline play independent from details navigation and closes back to its poster", async () => {
  await render();
  await act(() =>
    container.querySelector<HTMLButtonElement>("button.yt-play")?.click(),
  );
  expect(open).not.toHaveBeenCalled();
  const frame = container.querySelector("iframe");
  expect(frame?.src).toContain(
    "https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ?",
  );
  expect(frame?.dataset.positionKey).toBe("youtube:dQw4w9WgXcQ");
  const close = Array.from(container.querySelectorAll("button")).find(
    (button) => button.textContent?.includes("Close video"),
  );
  await act(() => close?.click());
  expect(container.querySelector("iframe, video")).toBeNull();
  expect(container.querySelector("button.yt-play")).not.toBeNull();
  expect(open).not.toHaveBeenCalled();
  await act(() =>
    container.querySelector<HTMLAnchorElement>("a[data-card-link]")?.click(),
  );
  expect(open).toHaveBeenCalledOnce();
});

it("prefers downloaded media on demand and uses the shared video position identity", async () => {
  await render({
    ...post,
    youtube: { ...post.youtube, video_url: "/assets/station.mp4" },
  });
  expect(container.querySelector("video")).toBeNull();
  expect(container.textContent).not.toContain("Remote video");
  await act(() =>
    container.querySelector<HTMLButtonElement>("button.yt-play")?.click(),
  );
  expect(container.querySelector("video")?.getAttribute("src")).toContain(
    "/assets/station.mp4",
  );
  expect(container.querySelector("video")?.dataset.positionKey).toBe(
    "youtube:dQw4w9WgXcQ",
  );
  expect(container.querySelector("iframe")).toBeNull();
});

it("gives Shorts their portrait layout and community posts no video controls", async () => {
  await render({ ...post, youtube: { ...post.youtube, kind: "short" } });
  expect(container.querySelector(".youtube-feed-short")).not.toBeNull();
  expect(container.textContent).toContain("Short");
  await render({
    ...post,
    id: "post:community-1",
    url: "https://www.youtube.com/post/community-1",
    youtube: {
      ...post.youtube,
      kind: "post",
      video_id: null,
      duration_text: "",
      views_text: "",
    },
  });
  expect(container.querySelector(".youtube-feed-post")).not.toBeNull();
  expect(container.textContent).toContain("Community post");
  expect(container.querySelector("button.yt-play, iframe, video")).toBeNull();
  expect(
    container.querySelector(".youtube-feed-post-image img"),
  ).not.toBeNull();
});

it("keeps older collected videos usable without optional preview metadata", async () => {
  await render({ ...post, youtube: undefined });
  expect(
    container.querySelector<HTMLButtonElement>("button.yt-play")?.disabled,
  ).toBe(false);
  expect(container.querySelector("img")?.src).toBe(
    "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg",
  );
  expect(container.textContent).toContain("City Films");
});

it("rejects external download URLs and untrusted thumbnail or player hosts", async () => {
  await render({
    ...post,
    url: "https://youtube.com.evil.test/watch?v=dQw4w9WgXcQ",
    youtube: {
      ...post.youtube,
      thumbnail_url: "https://evil.test/poster.jpg",
      video_url: "https://evil.test/video.mp4",
    },
  });
  expect(container.querySelector("img, video, iframe")).toBeNull();
  expect(
    container.querySelector<HTMLButtonElement>("button.yt-play")?.disabled,
  ).toBe(true);
});
