import { expect, it } from "vitest";
import {
  connectionsFixture,
  externalConnection,
  incomingConnection,
  outgoingConnection,
  videoConnection,
} from "../../tests/fixtures/post-connections-data";
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
