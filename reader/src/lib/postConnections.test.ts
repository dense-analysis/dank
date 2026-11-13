import { afterEach, expect, it, vi } from "vitest";
import {
  connectionContext,
  connectionNeighbor,
  connectionTargetLabel,
  fetchPostConnections,
} from "./postConnections";
import {
  connectionsFixture,
  incomingConnection,
  outgoingConnection,
  redditConnectionsFixture,
  sharedArticleConnection,
  videoConnection,
} from "./postConnections.fixture";

afterEach(() => vi.unstubAllGlobals());

it("requests connections for the exact domain and ID, forwarding cancellation", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValue(new Response(JSON.stringify(connectionsFixture)));
  vi.stubGlobal("fetch", fetch);
  const signal = new AbortController().signal;
  expect(
    await fetchPostConnections("example.com", "kernel-article", signal),
  ).toEqual(connectionsFixture);
  expect(fetch).toHaveBeenCalledWith(
    "/api/reader/connections?domain=example.com&id=kernel-article",
    expect.objectContaining({ signal }),
  );
});

it("rejects another root instead of presenting its graph", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(new Response(JSON.stringify(connectionsFixture))),
  );
  await expect(
    fetchPostConnections("elsewhere.example", "kernel-article"),
  ).rejects.toThrow("unexpected connections");
});

it("keeps upstream errors explicit", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(
        new Response(
          JSON.stringify({ error: "Connection index is unavailable." }),
          { status: 503 },
        ),
      ),
  );
  await expect(
    fetchPostConnections("example.com", "kernel-article"),
  ).rejects.toThrow("Connection index is unavailable.");
});

it("explores the neighboring post while evidence retains its original source", () => {
  expect(connectionNeighbor(incomingConnection).post).toBe(
    incomingConnection.origin,
  );
  expect(connectionNeighbor(outgoingConnection)).toBe(
    outgoingConnection.target,
  );
  expect(incomingConnection.evidence.matched_url).toContain("#security");
});

it("uses a video's authoritative title and never borrows the linking headline", () => {
  expect(connectionTargetLabel(videoConnection.target)).toBe(
    "Kernel Recipes 2026 - Security in the LLM age",
  );
  expect(connectionTargetLabel({ ...videoConnection.target, title: "" })).toBe(
    "YouTube video · NnV_cWeoo5Q",
  );
  expect(
    connectionTargetLabel({
      url: "https://example.com/unknown",
      title: "",
      post: null,
      youtube_item_id: null,
    }),
  ).toBe("https://example.com/unknown");
});

it("carries source and community context while keeping All and Blogs broad", async () => {
  expect(connectionContext({ kind: "all", community: "kernel" })).toEqual({});
  expect(connectionContext({ kind: "stories", community: "" })).toEqual({});
  expect(connectionContext({ kind: "reddit", community: "Kernel" })).toEqual({
    source: "reddit",
    community: "kernel",
  });
  expect(connectionContext({ kind: "hacker_news", community: "" })).toEqual({
    source: "hacker_news",
  });
  const fetch = vi
    .fn()
    .mockResolvedValue(new Response(JSON.stringify(redditConnectionsFixture)));
  vi.stubGlobal("fetch", fetch);
  await fetchPostConnections("reddit.com", "discussion", undefined, {
    source: "reddit",
    community: "kernel",
  });
  expect(fetch.mock.calls[0][0]).toBe(
    "/api/reader/connections?domain=reddit.com&id=discussion&source=reddit&community=kernel",
  );
});

it("explores the other post that links to a shared publisher", () => {
  expect(connectionNeighbor(sharedArticleConnection).post).toEqual(
    sharedArticleConnection.origin,
  );
});

it("rejects responses from another source context", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(
        new Response(JSON.stringify(redditConnectionsFixture)),
      ),
  );
  await expect(
    fetchPostConnections("reddit.com", "discussion", undefined, {
      source: "reddit",
      community: "other",
    }),
  ).rejects.toThrow("unexpected connections");
});
