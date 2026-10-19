import { expect, it } from "vitest";
import {
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
