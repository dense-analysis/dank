import { afterEach, expect, it, vi } from "vitest";
import { fetchPosts } from "./api";
import { sourceKind, sourcesForFeed } from "./collections";
import { filterParams, parseFilters } from "./navigation";
import { defaultFilters } from "./storage";
import type { HackerNewsPost, Post } from "./types";
import { isPost, isSource } from "./validation";

const story: HackerNewsPost = {
  points: 100,
  comments: 8,
  discussion_url: "https://news.ycombinator.com/item?id=123",
  external_url: "https://example.com/article",
  day: "2026-10-17",
  rank: 3,
};
const post: Post = {
  id: "123",
  domain: "news.ycombinator.com",
  source: "hacker_news",
  url: story.discussion_url,
  title: "A story",
  author: "ada",
  excerpt: "",
  html: "",
  created_at: "2026-10-17T12:00:00Z",
  thumbnail: null,
  media: [],
  hacker_news: story,
};

afterEach(() => vi.unstubAllGlobals());

it.each(["top10", "top20", "top50", "all"] as const)(
  "round-trips %s with search and dates, and sends it to the API",
  async (hnFilter) => {
    const filters = {
      ...defaultFilters,
      kind: "hacker_news" as const,
      hnFilter,
      q: "kernel",
      sort: "oldest" as const,
      after: "2026-10-01",
    };
    expect(parseFilters(filterParams(filters))).toEqual(filters);
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              posts: [post],
              next_cursor: null,
              limited: false,
            }),
          ),
      ),
    );
    await expect(fetchPosts(filters)).resolves.toMatchObject({ posts: [post] });
    const params = new URL(
      String(vi.mocked(fetch).mock.calls[0][0]),
      "https://reader.test",
    ).searchParams;
    expect(params.get("kind")).toBe("hacker_news");
    expect(params.get("hn_filter")).toBe(hnFilter);
    expect(params.has("view")).toBe(false);
  },
);

it("defaults absent and invalid selections to Top 50% and drops HN-only filters elsewhere", () => {
  expect(parseFilters(new URLSearchParams("view=hacker_news")).hnFilter).toBe(
    "top50",
  );
  expect(
    parseFilters(new URLSearchParams("view=hacker_news&hn_filter=homepage"))
      .hnFilter,
  ).toBe("top50");
  expect(
    parseFilters(new URLSearchParams("view=stories&hn_filter=all")).hnFilter,
  ).toBe("top50");
  expect(
    filterParams({ ...defaultFilters, hnFilter: "all" }).has("hn_filter"),
  ).toBe(false);
});

it("identifies Hacker News separately from blogs, including older source responses", () => {
  const source = {
    domain: "news.ycombinator.com",
    name: "Hacker News",
    tags: [],
    count: 3,
  };
  expect(sourceKind(source.domain)).toBe("hacker_news");
  expect(sourceKind("www.hckrnews.com")).toBe("hacker_news");
  expect(sourcesForFeed([source], "stories")).toEqual([]);
  expect(sourcesForFeed([source], "hacker_news")).toEqual([source]);
  expect(isSource({ ...source, kind: "hacker_news" })).toBe(true);
});

it("accepts summary metadata without comments and validates detailed capture data", () => {
  expect(isPost(post)).toBe(true);
  const comment = {
    id: "456",
    parent_id: "123",
    author: "reader",
    html: "<p>Reply</p>",
    created_at: "2026-10-17T12:00:00Z",
    depth: 0,
  };
  const detail = {
    ...story,
    comment_capture: {
      status: "partial",
      captured: 1,
      total: 8,
      reason: "Comment limit reached.",
    },
    comments_items: [comment],
  };
  expect(isPost({ ...post, hacker_news: detail })).toBe(true);

  for (const bad of [
    { points: -1 },
    { day: "2026-02-30" },
    { day: "" },
    { rank: 0 },
    { comments: "8" },
    { comment_capture: { ...detail.comment_capture, status: "invented" } },
    { comments_items: [{ ...comment, depth: -1 }] },
    { comments_items: [{ ...comment, created_at: "bad" }] },
  ]) {
    expect(isPost({ ...post, hacker_news: { ...detail, ...bad } })).toBe(false);
  }
});
