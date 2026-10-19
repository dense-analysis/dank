import { request } from "./api";
import type { Filters, PostReference } from "./types";
import { isRecord } from "./validation";

export interface PostConnectionEvidence {
  domain: string;
  post_id: string;
  kind: "post" | "comment";
  reference_id: string;
  reference_url: string;
  author: string;
  matched_url: string;
  excerpt: string;
  observed_at: string;
}

export interface PostConnectionTarget {
  url: string;
  title?: string;
  post: PostReference | null;
  youtube_item_id: string | null;
}

export interface PostConnection {
  id: string;
  direction: "incoming" | "outgoing" | "related";
  origin: PostReference;
  target: PostConnectionTarget;
  evidence: PostConnectionEvidence;
}

export interface ConnectionsContext {
  source?: "reddit" | "hacker_news" | "x" | "youtube" | "rss";
  community?: string;
}

export function connectionContext(
  filters: Pick<Filters, "kind" | "community">,
): ConnectionsContext {
  if (filters.kind === "all" || filters.kind === "stories") return {};
  if (filters.kind === "reddit" && filters.community)
    return { source: "reddit", community: filters.community.toLowerCase() };

  return { source: filters.kind };
}

export function connectionContextLabel(context: ConnectionsContext): string {
  if (context.community) return `r/${context.community}`;

  switch (context.source) {
    case "reddit":
      return "Reddit";
    case "hacker_news":
      return "Hacker News";
    case "x":
      return "X";
    case "youtube":
      return "YouTube";
    case "rss":
      return "Blogs";
    default:
      return "All sources";
  }
}

export interface PostConnectionsData {
  root: PostReference;
  context?: ConnectionsContext;
  related?: PostConnection[];
  related_count?: number;
  incoming: PostConnection[];
  outgoing: PostConnection[];
  incoming_count: number;
  outgoing_count: number;
  limit: number;
  coverage: "saved_evidence";
}

function isPostReference(value: unknown): value is PostReference {
  return (
    isRecord(value) &&
    ["domain", "id", "url", "title", "source"].every(
      (field) => typeof value[field] === "string",
    ) &&
    value.domain !== "" &&
    value.id !== ""
  );
}

function isEvidence(value: unknown): value is PostConnectionEvidence {
  return (
    isRecord(value) &&
    [
      "domain",
      "post_id",
      "reference_id",
      "reference_url",
      "author",
      "matched_url",
      "excerpt",
      "observed_at",
    ].every((field) => typeof value[field] === "string") &&
    (value.kind === "post" ||
      (value.kind === "comment" && value.reference_id !== "")) &&
    (value.observed_at === "" ||
      Number.isFinite(Date.parse(String(value.observed_at))))
  );
}

function isConnection(value: unknown): value is PostConnection {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    value.id !== "" &&
    (value.direction === "incoming" ||
      value.direction === "outgoing" ||
      value.direction === "related") &&
    isPostReference(value.origin) &&
    isRecord(value.target) &&
    typeof value.target.url === "string" &&
    (value.target.title === undefined ||
      typeof value.target.title === "string") &&
    (value.target.post === null || isPostReference(value.target.post)) &&
    (value.target.youtube_item_id === null ||
      (typeof value.target.youtube_item_id === "string" &&
        /^video:[A-Za-z0-9_-]{11}$/.test(value.target.youtube_item_id))) &&
    isEvidence(value.evidence) &&
    value.evidence.domain === value.origin.domain &&
    value.evidence.post_id === value.origin.id
  );
}

function isCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

export function isPostConnections(
  value: unknown,
): value is PostConnectionsData {
  if (
    !isRecord(value) ||
    !isPostReference(value.root) ||
    value.coverage !== "saved_evidence" ||
    !isCount(value.limit) ||
    value.limit === 0 ||
    !isCount(value.incoming_count) ||
    !isCount(value.outgoing_count)
  )
    return false;

  const root = value.root;

  for (const direction of ["incoming", "outgoing"] as const) {
    const connections = value[direction];
    const count =
      direction === "incoming" ? value.incoming_count : value.outgoing_count;

    if (
      !Array.isArray(connections) ||
      connections.length > value.limit ||
      connections.length > count ||
      !connections.every(
        (connection) =>
          isConnection(connection) &&
          connection.direction === direction &&
          (direction === "outgoing"
            ? connection.origin.domain === root.domain &&
              connection.origin.id === root.id
            : connection.target.post?.domain === root.domain &&
              connection.target.post?.id === root.id),
      ) ||
      new Set(connections.map((connection) => connection.id)).size !==
        connections.length
    )
      return false;
  }

  if (value.context !== undefined) {
    const context = value.context;

    if (
      !isRecord(context) ||
      Object.keys(context).some(
        (key) => key !== "source" && key !== "community",
      ) ||
      (context.source !== undefined &&
        !["reddit", "hacker_news", "x", "youtube", "rss"].includes(
          String(context.source),
        )) ||
      (context.community !== undefined &&
        (context.source !== "reddit" ||
          typeof context.community !== "string" ||
          !/^[a-z0-9_]{2,21}$/.test(context.community)))
    )
      return false;
  }

  if (value.related !== undefined || value.related_count !== undefined) {
    const related = value.related;
    const outgoing = value.outgoing as PostConnection[];
    const targets = new Set(
      outgoing.flatMap((connection) =>
        connection.evidence.kind === "post" && connection.target.post
          ? [
              JSON.stringify([
                connection.target.post.domain,
                connection.target.post.id,
              ]),
            ]
          : [],
      ),
    );

    if (
      !Array.isArray(related) ||
      !isCount(value.related_count) ||
      related.length > value.limit ||
      related.length > value.related_count ||
      new Set(related.map((connection) => connection?.id)).size !==
        related.length ||
      !related.every(
        (connection) =>
          isConnection(connection) &&
          connection.direction === "related" &&
          (connection.origin.domain !== root.domain ||
            connection.origin.id !== root.id) &&
          connection.target.post &&
          targets.has(
            JSON.stringify([
              connection.target.post.domain,
              connection.target.post.id,
            ]),
          ),
      )
    )
      return false;
  }

  return true;
}

export async function fetchPostConnections(
  domain: string,
  id: string,
  signal?: AbortSignal,
  context: ConnectionsContext = {},
): Promise<PostConnectionsData> {
  const params = new URLSearchParams({ domain, id });
  if (context.source) params.set("source", context.source);
  if (context.community) params.set("community", context.community);
  const body = await request(`/connections?${params}`, signal);

  if (
    !isPostConnections(body) ||
    body.root.domain !== domain ||
    body.root.id !== id ||
    (body.context !== undefined &&
      (body.context.source !== context.source ||
        body.context.community !== context.community))
  )
    throw new Error(
      "The reader received unexpected connections. Please try again.",
    );

  return body;
}

/** The neighboring destination to explore, separate from the link's origin. */
export function connectionNeighbor(
  connection: PostConnection,
): PostConnectionTarget {
  return connection.direction !== "outgoing"
    ? {
        url: connection.origin.url,
        post: connection.origin,
        youtube_item_id: null,
      }
    : connection.target;
}

export function connectionTargetLabel(target: PostConnectionTarget): string {
  if (target.post) return target.post.title || target.post.url;
  if (target.title) return target.title;
  if (target.youtube_item_id)
    return `YouTube video · ${target.youtube_item_id.slice(6)}`;

  return target.url;
}
