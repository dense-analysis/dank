import { afterEach, expect, it, vi } from "vitest";
import {
  connectionContext,
  connectionNeighbor,
  connectionTargetLabel,
  fetchPostConnections,
  isPostConnections,
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
          JSON.stringify({ detail: "Connection index is unavailable." }),
          { status: 503 },
        ),
      ),
  );
  await expect(
    fetchPostConnections("example.com", "kernel-article"),
  ).rejects.toThrow("Connection index is unavailable.");
});

it("validates bounded direction counts and unique evidence identity", () => {
  expect(isPostConnections(connectionsFixture)).toBe(true);
  expect(
    isPostConnections({ ...connectionsFixture, incoming_count: 120 }),
  ).toBe(true);
  expect(isPostConnections({ ...connectionsFixture, outgoing_count: 0 })).toBe(
    false,
  );
  expect(isPostConnections({ ...connectionsFixture, limit: 1 })).toBe(false);
  expect(isPostConnections({ ...connectionsFixture, incoming_count: -1 })).toBe(
    false,
  );
  expect(
    isPostConnections({
      ...connectionsFixture,
      coverage: "inferred_similarity",
    }),
  ).toBe(false);
  expect(
    isPostConnections({
      ...connectionsFixture,
      incoming: [outgoingConnection],
    }),
  ).toBe(false);
  expect(
    isPostConnections({
      ...connectionsFixture,
      incoming_count: 2,
      incoming: [incomingConnection, incomingConnection],
    }),
  ).toBe(false);
});

it("rejects mismatched origin evidence, missing comment identity and invalid metadata", () => {
  const withEvidence = (overrides: Record<string, unknown>) => ({
    ...connectionsFixture,
    incoming: [
      {
        ...incomingConnection,
        evidence: { ...incomingConnection.evidence, ...overrides },
      },
    ],
  });
  expect(isPostConnections(withEvidence({ domain: "wrong.example" }))).toBe(
    false,
  );
  expect(isPostConnections(withEvidence({ post_id: "wrong" }))).toBe(false);
  expect(isPostConnections(withEvidence({ reference_id: "" }))).toBe(false);
  expect(isPostConnections(withEvidence({ observed_at: "yesterday" }))).toBe(
    false,
  );
  expect(isPostConnections(withEvidence({ excerpt: 10 }))).toBe(false);
  expect(
    isPostConnections({
      ...connectionsFixture,
      incoming: [
        {
          ...incomingConnection,
          target: {
            ...incomingConnection.target,
            post: outgoingConnection.target.post,
          },
        },
      ],
    }),
  ).toBe(false);
  expect(
    isPostConnections({
      ...connectionsFixture,
      outgoing: [{ ...incomingConnection, direction: "outgoing" }],
    }),
  ).toBe(false);
  expect(
    isPostConnections({
      ...connectionsFixture,
      outgoing: [
        {
          ...videoConnection,
          target: {
            ...videoConnection.target,
            youtube_item_id: "video:not-valid",
          },
        },
      ],
    }),
  ).toBe(false);
  expect(
    isPostConnections({
      ...connectionsFixture,
      outgoing: [
        {
          ...videoConnection,
          target: { ...videoConnection.target, title: false },
        },
      ],
    }),
  ).toBe(false);
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

it("validates related evidence against a saved publisher reached by the root", () => {
  expect(isPostConnections(redditConnectionsFixture)).toBe(true);
  expect(connectionNeighbor(sharedArticleConnection).post).toEqual(
    sharedArticleConnection.origin,
  );
  expect(
    isPostConnections({ ...redditConnectionsFixture, related_count: 0 }),
  ).toBe(false);
  expect(
    isPostConnections({
      ...redditConnectionsFixture,
      related: [sharedArticleConnection, sharedArticleConnection],
      related_count: 2,
    }),
  ).toBe(false);
  expect(
    isPostConnections({ ...redditConnectionsFixture, related: [null] }),
  ).toBe(false);
  expect(
    isPostConnections({
      ...redditConnectionsFixture,
      context: { source: "x", community: "kernel" },
    }),
  ).toBe(false);
  expect(
    isPostConnections({
      ...redditConnectionsFixture,
      related: [
        { ...sharedArticleConnection, target: outgoingConnection.target },
      ],
    }),
  ).toBe(false);
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
