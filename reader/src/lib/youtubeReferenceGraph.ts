import type {
  RelationshipEdge,
  RelationshipGraphData,
  RelationshipNode,
} from "./relationshipGraph";
import type { YouTubeItem, YouTubeReference } from "./youtubeCollection";

export function referenceGraphNodeId(kind: string, ...identity: string[]) {
  return JSON.stringify([kind, ...identity]);
}

function sourceLabel(reference: YouTubeReference) {
  switch (reference.source) {
    case "reddit":
      return "Reddit";
    case "hacker_news":
      return "Hacker News";
    case "x":
      return "X";
    default:
      return reference.domain;
  }
}

/** Only API-verified link evidence creates edges. No topic similarity is implied. */
export function youtubeReferenceGraph(item: YouTubeItem): {
  graph: RelationshipGraphData;
  referencesByNode: ReadonlyMap<string, readonly YouTubeReference[]>;
  videoNodeId: string;
} {
  const nodes = new Map<string, RelationshipNode>();
  const edges = new Map<string, RelationshipEdge>();
  const referencesByNode = new Map<string, YouTubeReference[]>();
  const videoNodeId = referenceGraphNodeId("video", item.id);
  const references = [...item.references].sort((a, b) =>
    [a.domain, a.post_id, a.kind, a.reference_id, a.observed_at]
      .join("\0")
      .localeCompare(
        [b.domain, b.post_id, b.kind, b.reference_id, b.observed_at].join("\0"),
      ),
  );
  const seen = new Set<string>();

  function connect(
    source: string,
    target: string,
    kind: string,
    label: string,
  ) {
    const id = JSON.stringify([kind, source, target]);
    edges.set(id, { id, source, target, kind, label });
  }

  function attach(nodeId: string, reference: YouTubeReference) {
    const entries = referencesByNode.get(nodeId) ?? [];
    entries.push(reference);
    referencesByNode.set(nodeId, entries);
  }

  nodes.set(videoNodeId, {
    id: videoNodeId,
    kind: "video",
    label: item.title || `YouTube video · ${item.video_id ?? item.id}`,
    kindLabel: "Video",
    tone: "accent",
    layer: 3,
  });

  for (const reference of references) {
    const referenceId = referenceGraphNodeId(
      reference.kind,
      reference.domain,
      reference.post_id,
      reference.reference_id,
    );

    if (seen.has(referenceId)) continue;
    seen.add(referenceId);
    const sourceId = referenceGraphNodeId("source", reference.domain);
    const postId = referenceGraphNodeId(
      "post",
      reference.domain,
      reference.post_id,
    );
    nodes.set(sourceId, {
      id: sourceId,
      kind: "source",
      label: sourceLabel(reference),
      kindLabel: "Source",
      tone: "neutral",
      layer: 0,
    });
    nodes.set(postId, {
      id: postId,
      kind: "post",
      label: reference.title || reference.url,
      kindLabel: "Post",
      tone: "blue",
      layer: 1,
    });
    connect(sourceId, postId, "published", "published");
    attach(sourceId, reference);
    attach(postId, reference);
    attach(videoNodeId, reference);

    if (reference.kind === "comment") {
      nodes.set(referenceId, {
        id: referenceId,
        kind: "comment",
        label: reference.author || "Saved comment",
        kindLabel: "Comment",
        tone: "amber",
        layer: 2,
      });
      attach(referenceId, reference);
      connect(postId, referenceId, "has_comment", "has comment");
      connect(referenceId, videoNodeId, "mentions", "mentions");
    } else {
      connect(postId, videoNodeId, "links", "links to");
    }
  }

  return {
    graph: {
      id: item.id,
      nodes: [...nodes.values()].sort(
        (a, b) => a.layer - b.layer || a.id.localeCompare(b.id),
      ),
      edges: [...edges.values()].sort((a, b) => a.id.localeCompare(b.id)),
    },
    referencesByNode,
    videoNodeId,
  };
}
