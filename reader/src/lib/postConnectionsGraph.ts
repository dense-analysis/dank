import {
  connectionTargetLabel,
  type PostConnection,
  type PostConnectionsData,
  type PostConnectionTarget,
} from "./postConnections";
import type {
  RelationshipEdge,
  RelationshipGraphData,
  RelationshipNode,
} from "./relationshipGraph";
import { dateLabel } from "./timestamps";
import type { CollectionInfo, PostReference } from "./types";

export function connectionNodeId(kind: string, ...identity: string[]) {
  return JSON.stringify([kind, ...identity]);
}

export function connectionSourceLabel(post: PostReference): string {
  switch (post.source) {
    case "hacker_news":
      return "Hacker News";
    case "reddit":
      return "Reddit";
    case "x":
      return "X";
    case "youtube":
      return "YouTube";
    default:
      return post.domain.replace(/^www\./, "");
  }
}

/** Map only verified links and their saved ownership; no inferred relationships. */
export type ConnectionDirection = "all" | "incoming" | "outgoing";

export function postConnectionsGraph(
  data: PostConnectionsData,
  direction: ConnectionDirection = "all",
  collectionInfo?: CollectionInfo | null,
): {
  graph: RelationshipGraphData;
  rootNodeId: string;
  discoveryNodeId: string | null;
  connectionsByNode: ReadonlyMap<string, readonly PostConnection[]>;
  targetsByNode: ReadonlyMap<string, PostConnectionTarget>;
} {
  const nodes = new Map<string, RelationshipNode>();
  const edges = new Map<string, RelationshipEdge>();
  const connectionsByNode = new Map<string, Map<string, PostConnection>>();
  const targetsByNode = new Map<string, PostConnectionTarget>();
  const rootNodeId = connectionNodeId("post", data.root.domain, data.root.id);
  const connections = (
    direction === "all"
      ? [...data.incoming, ...data.outgoing, ...(data.related ?? [])]
      : data[direction]
  )
    .slice()
    .sort((a, b) => a.id.localeCompare(b.id));

  const articleContext = ["reddit", "hacker_news", "x", "twitter"].includes(
    data.root.source,
  );
  const articleIds = new Set(
    connections.flatMap((connection) => {
      const post = connection.target.post;

      return articleContext &&
        connection.direction === "outgoing" &&
        connection.evidence.kind === "post" &&
        post &&
        ["rss", "linked_article"].includes(post.source)
        ? [connectionNodeId("post", post.domain, post.id)]
        : [];
    }),
  );

  function attach(id: string, connection: PostConnection) {
    const entries = connectionsByNode.get(id) ?? new Map();
    entries.set(connection.id, connection);
    connectionsByNode.set(id, entries);
  }

  function connect(
    source: string,
    target: string,
    kind: string,
    label: string,
    bend?: number,
  ) {
    const id = connectionNodeId("edge", kind, source, target);
    edges.set(id, {
      id,
      source,
      target,
      kind,
      label,
      ...(bend === undefined ? {} : { bend }),
    });
  }

  function postNode(post: PostReference, layer: number) {
    const id = connectionNodeId("post", post.domain, post.id);
    const root = id === rootNodeId;
    const identity = root ? data.root : post;
    const previous = nodes.get(id);
    nodes.set(id, {
      id,
      kind: root ? "root" : articleIds.has(id) ? "article" : "post",
      kindLabel: root
        ? "This post"
        : articleIds.has(id)
          ? "Saved article"
          : "Saved post",
      label: identity.title || identity.url,
      tone: root ? "accent" : "blue",
      layer: root
        ? 3
        : articleIds.has(id)
          ? 0
          : Math.min(layer, previous?.layer ?? layer),
    });
    targetsByNode.set(id, { url: post.url, post, youtube_item_id: null });

    return id;
  }

  function publisher(
    post: PostReference,
    postId: string,
    connection: PostConnection,
  ) {
    // An article is the context anchor, not a claim that its publisher was the
    // collection adapter or that it was discovered before these social posts.
    if (articleIds.has(postId)) return;
    const community =
      post.source === "reddit"
        ? /\/r\/([^/]+)\/comments\//i.exec(post.url)?.[1].toLowerCase()
        : undefined;
    const domain = post.domain.toLowerCase().replace(/^www\./, "");
    const sourceDomain = ["twitter.com", "x.com"].includes(domain)
      ? "x.com"
      : domain;
    const id = community
      ? connectionNodeId("source", sourceDomain, community)
      : connectionNodeId("source", sourceDomain);
    nodes.set(id, {
      id,
      kind: "source",
      kindLabel: "Source",
      label: community ? `r/${community}` : connectionSourceLabel(post),
      tone: "neutral",
      layer: articleIds.size ? 1 : 0,
    });
    connect(id, postId, "collected", "collected");
    attach(id, connection);
  }

  postNode(data.root, 3);

  for (const connection of connections) {
    const incoming = connection.direction === "incoming";
    const originId = postNode(
      connection.origin,
      articleIds.size ? 3 : incoming ? 1 : 3,
    );
    publisher(connection.origin, originId, connection);
    attach(originId, connection);
    let targetId: string;

    if (incoming) {
      targetId = rootNodeId;
      publisher(data.root, targetId, connection);
    } else if (connection.target.post) {
      targetId = postNode(connection.target.post, 5);
      publisher(connection.target.post, targetId, connection);
    } else {
      const target = connection.target;
      const video = Boolean(target.youtube_item_id);
      targetId = video
        ? connectionNodeId("video", target.youtube_item_id ?? "")
        : connectionNodeId("url", target.url);
      nodes.set(targetId, {
        id: targetId,
        kind: video ? "video" : "external",
        kindLabel: video ? "YouTube video" : "Not collected",
        label: connectionTargetLabel(target),
        tone: "neutral",
        layer: 5,
      });
      targetsByNode.set(targetId, target);
    }

    attach(targetId, connection);
    const evidence = connection.evidence;

    if (evidence.kind === "comment") {
      const commentId = connectionNodeId(
        "comment",
        evidence.domain,
        evidence.post_id,
        evidence.reference_id,
      );
      const previous = nodes.get(commentId);
      const layer = incoming ? 2 : 4;
      nodes.set(commentId, {
        id: commentId,
        kind: "comment",
        kindLabel: "Comment",
        label: evidence.author || "Saved comment",
        tone: "amber",
        layer: Math.min(layer, previous?.layer ?? layer),
      });
      attach(commentId, connection);
      connect(originId, commentId, "has_comment", "has comment");
      connect(
        commentId,
        targetId,
        "mentions",
        "links to",
        articleIds.has(targetId) ? 220 : undefined,
      );
    } else {
      // Curve returning publisher links around the middle source layer, so
      // their truthful arrow direction and label stay visible on phones too.
      connect(
        originId,
        targetId,
        "links",
        "links to",
        articleIds.has(targetId) ? 220 : undefined,
      );
    }
  }

  // History annotates only a destination already in view. It never expands
  // the first source's other posts or counts as hyperlink evidence.
  let discoveryNodeId: string | null = null;
  const discoveryTarget =
    collectionInfo &&
    [...targetsByNode].find(([, target]) =>
      target.post
        ? target.post.domain === collectionInfo.article?.domain &&
          target.post.id === collectionInfo.article.id
        : target.url === collectionInfo.article_url,
    );

  if (collectionInfo && discoveryTarget) {
    const [targetId] = discoveryTarget;
    discoveryNodeId = connectionNodeId("discovery", targetId);
    const labels = [
      ...new Set(collectionInfo.first_seen_from.map((origin) => origin.label)),
    ];
    nodes.set(discoveryNodeId, {
      id: discoveryNodeId,
      kind: "discovery",
      kindLabel: "First discovered",
      label: labels.join(" and ") || "Source not recorded",
      detail: collectionInfo.first_seen_at
        ? dateLabel(collectionInfo.first_seen_at)
        : "Time not recorded",
      tone: "neutral",
      layer: (nodes.get(targetId)?.layer ?? 0) - 1,
    });
    connect(discoveryNodeId, targetId, "discovery", "first observed");
  }

  // Filtered graphs omit whole stages; close those gaps so a small graph stays legible.
  const layers = [
    ...new Set([...nodes.values()].map((node) => node.layer)),
  ].sort((a, b) => a - b);

  return {
    graph: {
      id: connectionNodeId(
        "post-connections",
        data.root.domain,
        data.root.id,
        direction,
        data.context?.source ?? "",
        data.context?.community ?? "",
      ),
      nodes: [...nodes.values()]
        .map((node) => ({ ...node, layer: layers.indexOf(node.layer) }))
        .sort((a, b) => a.layer - b.layer || a.id.localeCompare(b.id)),
      edges: [...edges.values()].sort((a, b) => a.id.localeCompare(b.id)),
    },
    rootNodeId,
    discoveryNodeId,
    connectionsByNode: new Map(
      [...connectionsByNode].map(([id, entries]) => [
        id,
        [...entries.values()],
      ]),
    ),
    targetsByNode,
  };
}
