import { afterEach, expect, it, vi } from "vitest";
import { fetchReferenceEvidence } from "./referenceEvidence";
import type { ReferenceTarget } from "./referenceFocus";

const target: ReferenceTarget = {
  kind: "comment",
  id: "comment-123",
  url: "https://publisher.example/story?q=1#section",
  videoId: "",
  saved: "2026-10-17T12:00:00Z",
};
const evidence = {
  domain: "news.ycombinator.com",
  post_id: "story",
  kind: "comment",
  reference_id: target.id,
  matched_url: target.url,
  observed_at: target.saved,
  excerpt: "Read this source.",
  reference_url: "https://news.ycombinator.com/item?id=123",
};
afterEach(() => vi.unstubAllGlobals());

it("looks up the exact archived source identity independently of graph limits", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValue(
      new Response(JSON.stringify({ evidence }), { status: 200 }),
    );
  vi.stubGlobal("fetch", fetch);
  await expect(
    fetchReferenceEvidence(evidence.domain, evidence.post_id, target),
  ).resolves.toMatchObject({
    excerpt: evidence.excerpt,
    reference_url: evidence.reference_url,
  });
  const url = new URL(fetch.mock.calls[0][0], "https://dank.test");
  expect(url.pathname).toBe("/api/reader/connection");
  expect(url.searchParams.get("target")).toBe(target.url);
  expect(url.searchParams.get("reference_id")).toBe(target.id);
  expect(url.searchParams.get("observed_at")).toBe(target.saved);
});

it.each([
  { domain: "other.test" },
  { post_id: "other-story" },
  { reference_id: "other-comment" },
  { matched_url: "https://publisher.example/story?q=2" },
  { matched_url: "https://publisher.example/story?q=1#other-section" },
  { observed_at: "2026-10-16T12:00:00Z" },
  { kind: "post" },
])("rejects unrelated archived evidence: %j", async (change) => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ evidence: { ...evidence, ...change } }), {
        status: 200,
      }),
    ),
  );
  await expect(
    fetchReferenceEvidence(evidence.domain, evidence.post_id, target),
  ).rejects.toThrow("could not be verified");
});

it("distinguishes missing archived evidence from an API error", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(new Response("null", { status: 200 })),
  );
  await expect(
    fetchReferenceEvidence(evidence.domain, evidence.post_id, target),
  ).resolves.toBeNull();
});

it("treats only a missing reference response as absent evidence", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValueOnce(
      new Response('{"error":"Saved evidence not found"}', { status: 404 }),
    )
    .mockResolvedValueOnce(
      new Response('{"error":"Database unavailable"}', { status: 503 }),
    );
  vi.stubGlobal("fetch", fetch);
  await expect(
    fetchReferenceEvidence(evidence.domain, evidence.post_id, target),
  ).resolves.toBeNull();
  await expect(
    fetchReferenceEvidence(evidence.domain, evidence.post_id, target),
  ).rejects.toThrow("Database unavailable");
});
