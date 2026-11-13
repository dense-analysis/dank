import { apiGet } from "./api";
import type { components } from "./api.gen";
import type { Filters } from "./types";

type Schemas = components["schemas"];

export type PostConnectionEvidence = Schemas["ConnectionEvidenceOut"];
export type PostConnectionTarget = Schemas["ConnectionTargetOut"];
export type PostConnection = Schemas["ConnectionOut"];
export type ConnectionsContext = Schemas["ConnectionContextOut"];
export type PostConnectionsData = Schemas["ConnectionsResponse"];

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

export async function fetchPostConnections(
  domain: string,
  id: string,
  signal?: AbortSignal,
  context: ConnectionsContext = {},
): Promise<PostConnectionsData> {
  const query = new URLSearchParams({ domain, id });
  if (context.source) query.set("source", context.source);
  if (context.community) query.set("community", context.community);
  const body = await apiGet("/api/reader/connections", { query, signal });

  // A late answer for another post or scope must not replace this one.
  if (
    body.root.domain !== domain ||
    body.root.id !== id ||
    (body.context.source ?? undefined) !== context.source ||
    (body.context.community ?? undefined) !== context.community
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
        title: connection.origin.title,
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
