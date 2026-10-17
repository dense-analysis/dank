import { describe, expect, it } from "vitest";
import {
  connectionEvidenceParams,
  connectionOrigin,
  connectionReturn,
  connectionsClosedParams,
  connectionTargetParams,
} from "./connectionNavigation";
import { articleParams } from "./navigation";
import { parseReferenceTarget } from "./referenceFocus";

const graph = new URLSearchParams({
  article: "story",
  article_source: "example.com",
  connections: "graph",
  connections_direction: "incoming",
  q: "graph & sources",
});
const evidence = {
  domain: "reddit.com",
  post_id: "t3_post",
  kind: "comment" as const,
  reference_id: "t1_comment",
  matched_url: "https://example.com/story?edition=2#section",
  observed_at: "2026-10-17T12:00:00Z",
};

describe("connection traversal", () => {
  it("opens exact evidence without an expanded graph moving the highlighted passage", () => {
    const next = connectionEvidenceParams(graph, evidence);
    expect(next.get("article")).toBe("t3_post");
    expect(next.has("connections")).toBe(false);
    expect(next.has("connections_direction")).toBe(false);
    expect(parseReferenceTarget(next)).toMatchObject({
      kind: "comment",
      id: "t1_comment",
      url: evidence.matched_url,
    });
    expect(connectionReturn(next)?.toString()).toBe(graph.toString());
  });

  it("opens a neighboring graph and preserves a bounded one-hop return route", () => {
    let current = graph;
    for (let i = 0; i < 30; i++) {
      const next = connectionTargetParams(current, {
        post: { domain: "example.org", id: `story-${i}` },
        youtube_item_id: null,
      });
      if (!next) throw new Error("Missing reader destination");
      expect(next.get("connections")).toBe("graph");
      expect(next?.has("connections_direction")).toBe(false);
      expect(connectionReturn(next)?.toString()).toBe(
        connectionOrigin(current),
      );
      expect(connectionReturn(next)?.has("connections_from")).toBe(false);
      expect(next.toString().length).toBeLessThan(700);
      current = next;
    }
  });

  it("opens known YouTube entities in the existing DANK graph", () => {
    const next = connectionTargetParams(graph, {
      post: null,
      youtube_item_id: "video:NnV_cWeoo5Q",
    });
    if (!next) throw new Error("Missing video destination");
    expect(next.get("view")).toBe("youtube");
    expect(next.get("feed")).toBe("linked");
    expect(next.get("references")).toBe("graph");
    expect(next.has("article")).toBe(false);
    expect(next.has("q")).toBe(false);
    expect(connectionReturn(next)?.toString()).toBe(graph.toString());
    const source = connectionEvidenceParams(next, evidence);
    expect(connectionReturn(source)?.get("youtube_item")).toBe(
      "video:NnV_cWeoo5Q",
    );
  });

  it("does not invent reader destinations for external URLs or accept malformed fallback routes", () => {
    expect(
      connectionTargetParams(graph, { post: null, youtube_item_id: null }),
    ).toBeUndefined();
    expect(
      connectionTargetParams(graph, {
        post: null,
        youtube_item_id: "video:broken",
      }),
    ).toBeUndefined();
    for (const value of [
      "https://external.test/",
      "article=1",
      "view=youtube&youtube_item=javascript:bad",
      "a".repeat(8193),
    ])
      expect(
        connectionReturn(new URLSearchParams({ connections_from: value })),
      ).toBeUndefined();
  });

  it("ordinary article navigation and reader close clear exploration and focus state", () => {
    const source = connectionEvidenceParams(graph, evidence);
    source.set("connections", "list");
    const next = articleParams(source, { domain: "example.net", id: "other" });
    for (const key of [...source.keys()].filter(
      (key) => key.startsWith("reference_") || key.startsWith("connections"),
    ))
      expect(next.has(key)).toBe(false);
    const closed = connectionsClosedParams(source);
    expect(closed.get("q")).toBe("graph & sources");
    expect(closed.has("article")).toBe(false);
    expect(closed.has("connections_from")).toBe(false);
  });
});
