import { expect, it } from "vitest";
import {
  compactGraphLabel,
  graphTopology,
  relationshipPositions,
} from "./relationshipGraph";
import type { YouTubeItem, YouTubeReference } from "./youtubeCollection";
import {
  referenceGraphNodeId,
  youtubeReferenceGraph,
} from "./youtubeReferenceGraph";

const reference: YouTubeReference = {
  domain: "reddit.com",
  post_id: "discussion",
  title: "A source headline, not the video title",
  url: "https://www.reddit.com/r/example/comments/discussion/",
  source: "reddit",
  reader_url: "/reader/?article=discussion&article_source=reddit.com",
  kind: "comment",
  reference_id: "first",
  reference_url:
    "https://www.reddit.com/r/example/comments/discussion/comment/first/",
  author: "First reader",
  matched_url: "https://youtu.be/NnV_cWeoo5Q?t=1468",
  excerpt: "We have seen it happen in real time.",
  observed_at: "2026-10-17T12:00:00Z",
};
const item: YouTubeItem = {
  id: "video:NnV_cWeoo5Q",
  video_id: "NnV_cWeoo5Q",
  kind: "video",
  title: "Kernel Recipes 2026 - Security in the LLM age",
  text: "",
  url: "https://www.youtube.com/watch?v=NnV_cWeoo5Q",
  channel: { id: "", name: "", url: "" },
  thumbnail_url: null,
  duration_text: "",
  views_text: "",
  published_text: "",
  observed_at: reference.observed_at,
  detail_capture: { status: "disabled" },
  discoveries: [],
  references: [reference],
  reference_count: 1,
};

it("makes two discussions converge on the video through their exact comment evidence", () => {
  const second = {
    ...reference,
    domain: "news.ycombinator.com",
    source: "hacker_news",
    author: "zdw",
  };
  const { graph, videoNodeId, referencesByNode } = youtubeReferenceGraph({
    ...item,
    references: [reference, second],
    reference_count: 2,
  });
  expect(graph.nodes.filter((node) => node.kind === "source")).toHaveLength(2);
  expect(graph.nodes.filter((node) => node.kind === "post")).toHaveLength(2);
  expect(graph.nodes.filter((node) => node.kind === "comment")).toHaveLength(2);
  expect(graph.nodes.filter((node) => node.kind === "video")).toEqual([
    expect.objectContaining({ label: item.title }),
  ]);
  const mentions = graph.edges.filter((edge) => edge.kind === "mentions");
  expect(mentions).toHaveLength(2);
  expect(mentions.every((edge) => edge.target === videoNodeId)).toBe(true);
  expect(graph.edges.filter((edge) => edge.kind === "links")).toHaveLength(0);
  expect(referencesByNode.get(mentions[0].source)).toHaveLength(1);
});

it("keeps comments distinct within a post while merging duplicate captures and source nodes", () => {
  const second = { ...reference, reference_id: "second" };
  const post = { ...reference, kind: "post" as const, reference_id: "" };
  const { graph, referencesByNode, videoNodeId } = youtubeReferenceGraph({
    ...item,
    references: [reference, second, reference, post],
  });
  expect(graph.nodes).toHaveLength(5);
  expect(graph.edges.filter((edge) => edge.kind === "collected")).toHaveLength(
    1,
  );
  expect(
    graph.edges.filter((edge) => edge.kind === "has_comment"),
  ).toHaveLength(2);
  expect(graph.edges.filter((edge) => edge.kind === "links")).toHaveLength(1);
  expect(referencesByNode.get(videoNodeId)).toHaveLength(3);
});

it("never substitutes a source headline for a missing title or invents unsaved links", () => {
  const { graph } = youtubeReferenceGraph({
    ...item,
    title: "",
    reference_count: 50,
  });
  expect(graph.nodes.find((node) => node.kind === "video")?.label).toBe(
    "YouTube video · NnV_cWeoo5Q",
  );
  expect(graph.edges).toHaveLength(3);
  expect(
    youtubeReferenceGraph({ ...item, references: [], reference_count: 50 })
      .graph.edges,
  ).toEqual([]);
});

it("uses collision-safe source identity and deterministic graph structure independent of response order", () => {
  const other = { ...reference, domain: "a:b", post_id: "c" };
  const another = { ...reference, domain: "a", post_id: "b:c" };
  const first = youtubeReferenceGraph({
    ...item,
    references: [reference, other, another],
  }).graph;
  const reversed = youtubeReferenceGraph({
    ...item,
    references: [another, other, reference],
  }).graph;
  expect(first).toEqual(reversed);
  expect(first.nodes.filter((node) => node.kind === "post")).toHaveLength(3);
  expect(referenceGraphNodeId("post", "a:b", "c")).not.toBe(
    referenceGraphNodeId("post", "a", "b:c"),
  );
});

it("keeps layout topology stable when metadata supplies a new video title", () => {
  const first = youtubeReferenceGraph(item).graph;
  const enriched = youtubeReferenceGraph({
    ...item,
    title: "New authoritative title",
  }).graph;
  expect(graphTopology(first)).toBe(graphTopology(enriched));
  expect(relationshipPositions(first.nodes)).toEqual(
    relationshipPositions(enriched.nodes),
  );
  const positions = relationshipPositions(first.nodes);
  expect(positions[first.nodes[0].id].x).toBeLessThan(
    positions[first.nodes.at(-1)?.id ?? ""].x,
  );
  expect(
    relationshipPositions(first.nodes, true)[first.nodes.at(-1)?.id ?? ""].y,
  ).toBeGreaterThan(0);
  expect(compactGraphLabel(" A  long\nlabel ")).toBe("A long label");
  expect(compactGraphLabel("x".repeat(100))).toHaveLength(72);
});

it("keeps an aggregator's discovery provenance distinct from publication", () => {
  const aggregator = {
    ...reference,
    domain: "feeds.example",
    source: "rss",
    url: "https://publisher.example/article",
  };
  const { graph } = youtubeReferenceGraph({
    ...item,
    references: [aggregator],
  });
  const sourceId = referenceGraphNodeId("source", aggregator.domain);
  expect(graph.edges.find((edge) => edge.source === sourceId)).toMatchObject({
    kind: "collected",
    label: "collected",
  });
  expect(graph.edges.some((edge) => edge.label === "published")).toBe(false);
});
