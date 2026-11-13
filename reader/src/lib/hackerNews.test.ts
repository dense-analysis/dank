import { afterEach, expect, it, vi } from "vitest";
import { fetchPosts } from "./api";
import { feedSource, hackerNewsPost, post } from "./api.fixture";
import { sourceKind, sourcesForFeed } from "./collections";
import {
  hackerNewsCaptureReason,
  hackerNewsExcerpt,
  hasHackerNewsBody,
} from "./hackerNews";
import { filterParams, parseFilters } from "./navigation";
import { defaultFilters } from "./storage";
import type { HackerNewsPost, Post } from "./types";

const story: HackerNewsPost = hackerNewsPost({
  submitted_at: "2026-10-16T12:00:00Z",
  points: 100,
  comments: 8,
  discussion_url: "https://news.ycombinator.com/item?id=123",
  external_url: "https://example.com/article",
  day: "2026-10-17",
  rank: 3,
});
const discussion: Post = post({
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
});

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
              posts: [discussion],
              next_cursor: null,
              limited: false,
            }),
          ),
      ),
    );
    await expect(fetchPosts(filters)).resolves.toMatchObject({
      posts: [discussion],
    });
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

it("includes Hacker News in Blogs while preserving its dedicated view", () => {
  const source = feedSource({
    domain: "news.ycombinator.com",
    name: "Hacker News",
    kind: "hacker_news",
    tags: [],
    count: 3,
  });
  expect(sourceKind(source.domain)).toBe("hacker_news");
  expect(sourceKind("www.hckrnews.com")).toBe("hacker_news");
  expect(sourcesForFeed([source], "stories")).toEqual([source]);
  expect(sourcesForFeed([source], "hacker_news")).toEqual([source]);
});

it.each([
  ["comment_limit", "The configured comment limit was reached."],
  [
    "timeout",
    "Comment collection timed out before the discussion was complete.",
  ],
  [
    "Comments retained from an earlier detailed scrape. comment_limit",
    "Comments retained from an earlier detailed scrape. The configured comment limit was reached.",
  ],
  [
    "Comments retained from an earlier detailed scrape. timeout",
    "Comments retained from an earlier detailed scrape. Comment collection timed out before the discussion was complete.",
  ],
  [
    "Comments retained from an earlier detailed scrape.",
    "Comments retained from an earlier detailed scrape.",
  ],
  [
    "request_failed: internal error GET https://example.com/token",
    "Comment collection could not complete.",
  ],
  [
    "Comments retained from an earlier detailed scrape. HTTP 403: internal details",
    "Comments retained from an earlier detailed scrape. Comment collection could not complete.",
  ],
  [null, "Some comments or replies were not captured."],
])(
  "explains capture reason %s without exposing collector error details",
  (reason, message) => {
    expect(hackerNewsCaptureReason(reason)).toBe(message);
  },
);

it("omits only duplicate original URLs while preserving useful self-post text and media", () => {
  const url = "https://example.com/article?a=1&b=2";
  expect(hackerNewsExcerpt(url, url)).toBe("");
  expect(
    hackerNewsExcerpt("  https://example.com  ", "https://example.com/"),
  ).toBe("");
  expect(hackerNewsExcerpt(`${url} Some context`, url)).toBe(
    `${url} Some context`,
  );
  expect(hackerNewsExcerpt(url, null)).toBe(url);
  expect(
    hasHackerNewsBody(
      `<p><a href="${url}">https://example.com/article?a=1&amp;b=2</a></p>`,
      url,
    ),
  ).toBe(false);
  expect(
    hasHackerNewsBody(`<p>${url}</p><script>malicious()</script>`, url),
  ).toBe(false);
  expect(hasHackerNewsBody("<p>Ask HN: What are you building?</p>", null)).toBe(
    true,
  );
  expect(
    hasHackerNewsBody(`<p><a href="${url}">Project details</a></p>`, url),
  ).toBe(true);
  expect(
    hasHackerNewsBody(`<p>${url}</p><p>My project description.</p>`, url),
  ).toBe(true);
  expect(
    hasHackerNewsBody(
      `<p>${url}</p><img src="https://example.com/image.png">`,
      url,
    ),
  ).toBe(true);
  expect(hasHackerNewsBody("", url)).toBe(false);
});

it("uses the full saved body to recognize a truncated URL excerpt", () => {
  const url = `https://example.com/${"long-path-segment/".repeat(30)}`;
  const excerpt = `${url.slice(0, 200)}…`;
  expect(
    hackerNewsExcerpt(excerpt, url, `<p><a href="${url}">${url}</a></p>`),
  ).toBe("");
  expect(
    hackerNewsExcerpt(
      "A useful project",
      url,
      `<p>A useful project: ${url}</p>`,
    ),
  ).toBe("A useful project");
  expect(hackerNewsExcerpt("A useful project", url, "")).toBe(
    "A useful project",
  );
});

it("uses the saved publisher excerpt only when HN has no body", () => {
  const url = "https://example.com/article";
  const fallback = "The saved publisher explains the research.";
  expect(hackerNewsExcerpt("", url, "", fallback)).toBe(fallback);
  expect(hackerNewsExcerpt(url, url, `<p>${url}</p>`, fallback)).toBe(fallback);
  expect(
    hackerNewsExcerpt("https://example…", url, `<p>${url}</p>`, fallback),
  ).toBe(fallback);
  expect(
    hackerNewsExcerpt(
      "The submitter's own explanation.",
      url,
      "<p>The submitter's own explanation.</p>",
      fallback,
    ),
  ).toBe("The submitter's own explanation.");
  expect(
    hackerNewsExcerpt(
      url,
      url,
      `<p>${url}</p><img src="https://example.com/image.png">`,
      fallback,
    ),
  ).toBe("");
  expect(hackerNewsExcerpt(url, url, `<p>${url}</p>`)).toBe("");
});
