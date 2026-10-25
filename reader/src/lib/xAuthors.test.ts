import { afterEach, expect, it, vi } from "vitest";
import { defaultFilters } from "./storage";
import { fetchXAuthors } from "./xAuthors";

afterEach(() => vi.unstubAllGlobals());

it("requests Authors with shared post filters and its own paging cursor", async () => {
  const fetch = vi.fn();
  fetch.mockResolvedValue(
    new Response(
      JSON.stringify({
        authors: [],
        total: 0,
        unattributed_posts: 0,
        next_cursor: null,
      }),
    ),
  );
  vi.stubGlobal("fetch", fetch);
  await fetchXAuthors(
    {
      ...defaultFilters,
      kind: "x",
      xFeed: "for_you",
      xTab: "authors",
      q: "space",
      account: "alice",
      after: "2026-10-01",
    },
    "author-page",
  );
  const query = new URL(String(fetch.mock.calls[0][0]), "http://localhost");
  expect(query.pathname).toBe("/api/reader/x/authors");
  expect(Object.fromEntries(query.searchParams)).toMatchObject({
    kind: "x",
    x_feed: "for_you",
    account: "alice",
    q: "space",
    mode: "combined",
    after: "2026-10-01",
    cursor: "author-page",
    limit: "20",
  });
  expect(query.searchParams.has("x_tab")).toBe(false);
  expect(query.searchParams.has("view")).toBe(false);
});

it.each([
  { handle: "../bad", name: "name", icon_url: null, count: 1, posts: [] },
  { handle: "alice", name: "name", icon_url: null, count: -1, posts: [] },
  { handle: "alice", name: "name", icon_url: null, count: 1, posts: [{}] },
])("rejects invalid author data %j", async (author) => {
  const fetch = vi.fn();
  fetch.mockResolvedValue(
    new Response(
      JSON.stringify({
        authors: [author],
        total: 1,
        unattributed_posts: 0,
        next_cursor: null,
      }),
    ),
  );
  vi.stubGlobal("fetch", fetch);
  await expect(
    fetchXAuthors({ ...defaultFilters, kind: "x", xFeed: "for_you" }),
  ).rejects.toThrow("unexpected author response");
});
