import { expect, it } from "vitest";
import {
  connectionDiscussion,
  connectionRoot,
  connectionsFixture,
  externalConnection,
  incomingConnection,
  outgoingConnection,
  redditConnectionsFixture,
  sharedArticleConnection,
  videoConnection,
} from "./postConnections.fixture";
import { connectionNodeId, postConnectionsGraph } from "./postConnectionsGraph";
import { graphTopology } from "./relationshipGraph";
import { dateLabel } from "./timestamps";
import type { CollectionInfo } from "./types";

it("draws incoming source-post-comment evidence into the root and outgoing links to their exact targets", () => {
  const { graph, rootNodeId, connectionsByNode } =
    postConnectionsGraph(connectionsFixture);
  const incomingComment = connectionNodeId(
    "comment",
    "reddit.com",
    "discussion",
    "comment-1",
  );
  const outgoingPost = connectionNodeId("post", "source.example", "research");
  expect(graph.edges).toContainEqual(
    expect.objectContaining({
      source: incomingComment,
      target: rootNodeId,
      kind: "mentions",
    }),
  );
  expect(graph.edges).toContainEqual(
    expect.objectContaining({
      source: rootNodeId,
      target: outgoingPost,
      kind: "links",
    }),
  );
  expect(connectionsByNode.get(incomingComment)).toEqual([incomingConnection]);
  expect(connectionsByNode.get(rootNodeId)).toHaveLength(4);
  expect(graph.nodes.find((node) => node.id === rootNodeId)?.kindLabel).toBe(
    "This post",
  );
});

it("keeps uncollected URLs distinct from known YouTube and collected post nodes", () => {
  const { graph, targetsByNode } = postConnectionsGraph(connectionsFixture);
  const video = graph.nodes.find((node) => node.kind === "video");
  const external = graph.nodes.find((node) => node.kind === "external");
  expect(video?.label).toBe(videoConnection.target.title);
  expect(external?.kindLabel).toBe("Not collected");
  expect(targetsByNode.get(video?.id ?? "")?.youtube_item_id).toBe(
    "video:NnV_cWeoo5Q",
  );
  expect(targetsByNode.get(external?.id ?? "")?.post).toBe(null);
});

it("shares the same comment node when one comment links to two targets", () => {
  const another = {
    ...externalConnection,
    id: "another-comment-link",
    direction: "outgoing" as const,
    target: outgoingConnection.target,
  };
  const { graph, connectionsByNode } = postConnectionsGraph({
    ...connectionsFixture,
    outgoing: [...connectionsFixture.outgoing, another],
  });
  const commentId = connectionNodeId(
    "comment",
    "example.com",
    "kernel-article",
    "publisher-comment",
  );
  expect(graph.nodes.filter((node) => node.id === commentId)).toHaveLength(1);
  expect(graph.edges.filter((edge) => edge.source === commentId)).toHaveLength(
    2,
  );
  expect(connectionsByNode.get(commentId)).toHaveLength(2);
});

it("uses tuple identities to avoid domain/post delimiter collisions and deterministic ordering", () => {
  expect(connectionNodeId("post", "a:b", "c")).not.toBe(
    connectionNodeId("post", "a", "b:c"),
  );
  const first = postConnectionsGraph(connectionsFixture).graph;
  const reversed = postConnectionsGraph({
    ...connectionsFixture,
    outgoing: [...connectionsFixture.outgoing].reverse(),
  }).graph;
  expect(first).toEqual(reversed);
  expect(new Set(first.nodes.map((node) => node.id)).size).toBe(
    first.nodes.length,
  );
});

it("filters direction without inventing edges or changing another graph's cached identity", () => {
  const all = postConnectionsGraph(connectionsFixture);
  const incoming = postConnectionsGraph(connectionsFixture, "incoming");
  const outgoing = postConnectionsGraph(connectionsFixture, "outgoing");
  expect(
    new Set([all.graph.id, incoming.graph.id, outgoing.graph.id]).size,
  ).toBe(3);
  expect(incoming.connectionsByNode.get(incoming.rootNodeId)).toEqual([
    incomingConnection,
  ]);
  expect(incoming.graph.nodes.some((node) => node.kind === "external")).toBe(
    false,
  );
  expect(
    outgoing.graph.nodes.some(
      (node) =>
        node.id === connectionNodeId("post", "reddit.com", "discussion"),
    ),
  ).toBe(false);
  const empty = postConnectionsGraph({
    ...connectionsFixture,
    incoming: [],
    outgoing: [],
  });
  expect(empty.graph.nodes).toHaveLength(1);
  expect(empty.graph.edges).toEqual([]);
});

it("changes labels without changing topology and collapses duplicate structural edges", () => {
  const base = postConnectionsGraph(connectionsFixture).graph;
  const updated = postConnectionsGraph({
    ...connectionsFixture,
    root: { ...connectionsFixture.root, title: "Updated article title" },
    outgoing: connectionsFixture.outgoing.map((connection) => ({
      ...connection,
      origin: { ...connection.origin, title: "Updated article title" },
    })),
  }).graph;
  expect(graphTopology(base)).toBe(graphTopology(updated));
  expect(new Set(base.edges.map((edge) => edge.id)).size).toBe(
    base.edges.length,
  );
});

it("describes an aggregator source as collection provenance without claiming authorship", () => {
  const origin = {
    ...incomingConnection.origin,
    domain: "feeds.example",
    source: "rss",
    url: "https://publisher.example/article",
  };
  const { graph } = postConnectionsGraph({
    ...connectionsFixture,
    incoming: [
      {
        ...incomingConnection,
        origin,
        evidence: { ...incomingConnection.evidence, domain: origin.domain },
      },
    ],
    outgoing: [],
  });
  const sourceId = connectionNodeId("source", origin.domain);
  const sourceEdges = graph.edges.filter((edge) => edge.source === sourceId);
  expect(sourceEdges).toHaveLength(1);
  expect(sourceEdges[0]).toMatchObject({
    kind: "collected",
    label: "collected",
  });
  expect(graph.edges.some((edge) => edge.label === "published")).toBe(false);
});

it("places saved publisher context before communities and posts without reversing hyperlinks", () => {
  const { graph, rootNodeId, connectionsByNode } = postConnectionsGraph(
    redditConnectionsFixture,
  );
  const article = graph.nodes.find((node) => node.kind === "article");
  const source = graph.nodes.find((node) => node.kind === "source");
  const root = graph.nodes.find((node) => node.id === rootNodeId);
  expect(article?.layer).toBeLessThan(source?.layer ?? 0);
  expect(source?.layer).toBeLessThan(root?.layer ?? 0);
  expect(source?.label).toBe("r/kernel");
  expect(graph.edges).toContainEqual(
    expect.objectContaining({
      source: rootNodeId,
      target: article?.id,
      kind: "links",
    }),
  );
  expect(graph.edges).toContainEqual(
    expect.objectContaining({
      source: connectionNodeId("post", "reddit.com", "second-discussion"),
      target: article?.id,
      kind: "links",
    }),
  );
  expect(connectionsByNode.get(article?.id ?? "")).toContainEqual(
    sharedArticleConnection,
  );
  expect(
    graph.edges.some(
      (edge) => edge.source === article?.id && edge.target === source?.id,
    ),
  ).toBe(false);
  expect(
    postConnectionsGraph({ ...redditConnectionsFixture, context: {} }).graph.id,
  ).not.toBe(graph.id);
  const outgoing = postConnectionsGraph(
    redditConnectionsFixture,
    "outgoing",
  ).graph;
  expect(
    outgoing.nodes.some((node) => node.id.includes("second-discussion")),
  ).toBe(false);
});

it("keeps Reddit communities distinct and normalizes X source aliases", () => {
  const anotherCommunity = {
    ...sharedArticleConnection,
    origin: {
      ...sharedArticleConnection.origin,
      url: "https://www.reddit.com/r/other/comments/second-discussion/",
    },
  };
  const graph = postConnectionsGraph({
    ...redditConnectionsFixture,
    context: {},
    related: [anotherCommunity],
  }).graph;
  expect(
    graph.nodes
      .filter((node) => node.kind === "source")
      .map((node) => node.label),
  ).toEqual(["r/kernel", "r/other"]);
  const xReferences = ["x.com", "www.twitter.com"].map((domain, index) => ({
    ...sharedArticleConnection,
    id: `x-${index}`,
    origin: {
      ...sharedArticleConnection.origin,
      source: "x",
      domain,
      id: `x-${index}`,
    },
    evidence: {
      ...sharedArticleConnection.evidence,
      domain,
      post_id: `x-${index}`,
    },
  }));
  const all = postConnectionsGraph({
    ...redditConnectionsFixture,
    context: {},
    related: xReferences,
  }).graph;
  expect(
    all.nodes.filter((node) => node.kind === "source" && node.label === "X"),
  ).toHaveLength(1);
});

const discoveryInfo: CollectionInfo = {
  article_url: connectionRoot.url,
  article: connectionRoot,
  article_status: "collected",
  post_first_seen_at: "2026-10-17T12:00:00Z",
  link_first_seen_at: "2026-10-17T12:00:00Z",
  first_seen_at: "2026-10-16T21:25:00Z",
  first_seen_from: [
    {
      ...connectionRoot,
      label: "example.com",
      observed_at: "2026-10-16T21:25:00Z",
      kind: "article",
    },
  ],
  article_collected_at: "2026-10-17T17:00:00Z",
  collected_via: null,
  seen_before_post: true,
  post_published_at: "2026-10-17T08:00:00Z",
  discovered_before_publication: true,
  earlier_discovered_at: "2026-10-16T21:25:00Z",
  earlier_discovered_from: [],
  coverage: "saved_history",
};

it("shows first discovery outside the community without expanding that source or changing link evidence", () => {
  const data = structuredClone(redditConnectionsFixture);
  const original = structuredClone(data);
  const base = postConnectionsGraph(data);
  const result = postConnectionsGraph(data, "all", discoveryInfo);
  const discovery = result.graph.nodes.find(
    (node) => node.kind === "discovery",
  );
  expect(discovery).toMatchObject({
    kindLabel: "First discovered",
    label: "example.com",
    detail: dateLabel("2026-10-16T21:25:00Z"),
    tone: "neutral",
  });
  expect(result.graph.nodes).toHaveLength(base.graph.nodes.length + 1);
  expect(
    result.graph.nodes
      .filter((node) => node.kind === "source")
      .map((node) => node.label),
  ).toEqual(["r/kernel"]);
  expect(
    result.graph.edges.filter((edge) => edge.kind === "discovery"),
  ).toEqual([
    expect.objectContaining({
      source: discovery?.id,
      target: connectionNodeId(
        "post",
        connectionRoot.domain,
        connectionRoot.id,
      ),
      label: "first observed",
    }),
  ]);
  expect(
    result.graph.edges.filter((edge) => edge.kind !== "discovery"),
  ).toEqual(base.graph.edges);
  expect(result.connectionsByNode).toEqual(base.connectionsByNode);
  expect(result.targetsByNode).toEqual(base.targetsByNode);
  expect(data).toEqual(original);
});

it("shows the current Reddit community and its observation time when Reddit discovered the article first", () => {
  const firstSeen = "2026-10-17T12:00:00Z";
  const { graph } = postConnectionsGraph(redditConnectionsFixture, "all", {
    ...discoveryInfo,
    first_seen_at: firstSeen,
    first_seen_from: [
      {
        ...connectionDiscussion,
        label: "r/kernel",
        observed_at: firstSeen,
        kind: "reference",
      },
    ],
    discovered_before_publication: false,
    earlier_discovered_at: null,
    earlier_discovered_from: [],
    seen_before_post: false,
  });
  expect(graph.nodes.filter((node) => node.kind === "discovery")).toEqual([
    expect.objectContaining({
      label: "r/kernel",
      detail: dateLabel(firstSeen),
      tone: "neutral",
    }),
  ]);
  expect(graph.nodes.filter((node) => node.kind === "source")).toHaveLength(1);
});

it("keeps tied discovery sources in one annotation and deduplicates repeated source labels", () => {
  const firstSource = discoveryInfo.first_seen_from[0];
  const { graph } = postConnectionsGraph(redditConnectionsFixture, "all", {
    ...discoveryInfo,
    first_seen_from: [
      firstSource,
      {
        ...connectionDiscussion,
        label: "r/kernel",
        observed_at: firstSource.observed_at,
        kind: "reference",
      },
      { ...firstSource, id: "another-capture" },
    ],
  });
  expect(graph.nodes.filter((node) => node.kind === "discovery")).toEqual([
    expect.objectContaining({ label: "example.com and r/kernel" }),
  ]);
  expect(graph.edges.filter((edge) => edge.kind === "discovery")).toHaveLength(
    1,
  );
});

it("labels missing discovery source and timestamp explicitly", () => {
  const { graph } = postConnectionsGraph(redditConnectionsFixture, "all", {
    ...discoveryInfo,
    first_seen_from: [],
    first_seen_at: null,
  });
  expect(graph.nodes.find((node) => node.kind === "discovery")).toMatchObject({
    label: "Source not recorded",
    detail: "Time not recorded",
  });
});

it("does not attach another article's provenance to a saved target that happens to share its URL", () => {
  for (const article of [
    null,
    { ...connectionRoot, domain: "other.example", id: "different-article" },
  ]) {
    const result = postConnectionsGraph(redditConnectionsFixture, "all", {
      ...discoveryInfo,
      article,
    });
    expect(result.graph.nodes.some((node) => node.kind === "discovery")).toBe(
      false,
    );
    expect(result.graph.edges.some((edge) => edge.kind === "discovery")).toBe(
      false,
    );
  }
});

it("omits discovery when direction filtering removes its article without changing other graph state", () => {
  const incoming = postConnectionsGraph(
    redditConnectionsFixture,
    "incoming",
    discoveryInfo,
  );
  expect(incoming.graph.nodes).toHaveLength(1);
  expect(incoming.graph.edges).toEqual([]);
  expect(incoming.graph.nodes[0].detail).toContain("Published");
  expect(incoming.graph.nodes[0].detail).not.toContain("first discovery");
  const outgoing = postConnectionsGraph(
    redditConnectionsFixture,
    "outgoing",
    discoveryInfo,
  );
  expect(
    outgoing.graph.nodes.filter((node) => node.kind === "discovery"),
  ).toHaveLength(1);
  expect(
    outgoing.graph.nodes.some((node) => node.id.includes("second-discussion")),
  ).toBe(false);
  expect(postConnectionsGraph(redditConnectionsFixture, "all", null)).toEqual(
    postConnectionsGraph(redditConnectionsFixture),
  );
});

it("retains the saved article's own discovery when exploring it as the graph root", () => {
  const { graph, rootNodeId } = postConnectionsGraph(
    connectionsFixture,
    "incoming",
    discoveryInfo,
  );
  const discovery = graph.nodes.find((node) => node.kind === "discovery");
  expect(discovery?.label).toBe("example.com");
  expect(
    postConnectionsGraph(connectionsFixture, "incoming", discoveryInfo).timing
      ?.publishedAt,
  ).toBe(discoveryInfo.post_published_at);
  expect(graph.edges.filter((edge) => edge.kind === "discovery")).toEqual([
    expect.objectContaining({ source: discovery?.id, target: rootNodeId }),
  ]);
});

it("attaches unavailable publisher discovery only to its exact visible destination", () => {
  const info: CollectionInfo = {
    ...discoveryInfo,
    article: null,
    article_url: externalConnection.target.url,
    article_status: "unavailable",
  };
  const { graph } = postConnectionsGraph(connectionsFixture, "outgoing", info);
  const external = graph.nodes.find((node) => node.kind === "external");
  expect(graph.edges.filter((edge) => edge.kind === "discovery")).toEqual([
    expect.objectContaining({ target: external?.id }),
  ]);
  const mismatched = postConnectionsGraph(connectionsFixture, "outgoing", {
    ...info,
    article_url: `${info.article_url}-different`,
  });
  expect(mismatched.graph.nodes.some((node) => node.kind === "discovery")).toBe(
    false,
  );
});

it("puts publication and the gap only on the current social post, with collection dates excluded", () => {
  const result = postConnectionsGraph(
    redditConnectionsFixture,
    "all",
    discoveryInfo,
  );
  const root = result.graph.nodes.find((node) => node.id === result.rootNodeId);
  expect(root?.detail).toBe(
    `Published\n${dateLabel("2026-10-17T08:00:00Z")}\n10h 35m after first discovery`,
  );
  expect(
    result.graph.nodes
      .filter((node) => node.kind === "post")
      .every((node) => node.detail === undefined),
  ).toBe(true);
  const changedCollectionTimes = postConnectionsGraph(
    redditConnectionsFixture,
    "all",
    {
      ...discoveryInfo,
      post_first_seen_at: "2026-10-20T01:00:00Z",
      link_first_seen_at: "2026-10-21T01:00:00Z",
      article_collected_at: "2026-10-22T01:00:00Z",
    },
  );
  expect(changedCollectionTimes.graph).toEqual(result.graph);
  const mismatch = postConnectionsGraph(redditConnectionsFixture, "all", {
    ...discoveryInfo,
    article: null,
  });
  expect(
    mismatch.graph.nodes.find((node) => node.kind === "root")?.detail,
  ).not.toContain("first discovery");
});

it("keeps unknown publication visible rather than substituting collection or reference timestamps", () => {
  const result = postConnectionsGraph(redditConnectionsFixture, "all", {
    ...discoveryInfo,
    post_published_at: null,
  });
  expect(
    result.graph.nodes.find((node) => node.id === result.rootNodeId)?.detail,
  ).toBe("Publication time unavailable\nGap unavailable");
  expect(
    postConnectionsGraph(
      connectionsFixture,
      "all",
      discoveryInfo,
    ).graph.nodes.find((node) => node.kind === "root")?.detail,
  ).toBeUndefined();
});
