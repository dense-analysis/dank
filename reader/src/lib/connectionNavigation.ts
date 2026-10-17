import {
  articleParams,
  clearConnectionParams,
  clearReferenceParams,
} from "./navigation";
import { referenceParams, type SourceReference } from "./referenceFocus";
import type { Post } from "./types";

type Destination = {
  post: Pick<Post, "domain" | "id"> | null;
  youtube_item_id: string | null;
};

// The browser keeps the full traversal history. A one-hop return URL also makes
// a copied link or a new tab useful, without recursively growing every URL.
export function connectionOrigin(params: URLSearchParams): string {
  const origin = new URLSearchParams(params);
  origin.delete("connections_from");
  return origin.toString();
}

export function connectionReturn(
  params: URLSearchParams,
): URLSearchParams | undefined {
  const value = params.get("connections_from");
  if (!value || value.length > 8192) return;
  const origin = new URLSearchParams(value);
  origin.delete("connections_from");
  const domain = origin.get("article_source") ?? "";
  const id = origin.get("article") ?? "";
  const article =
    /^[a-z0-9.-]{1,253}$/i.test(domain) && !!id && id.length <= 300;
  const video =
    origin.get("view") === "youtube" &&
    /^video:[A-Za-z0-9_-]{11}$/.test(origin.get("youtube_item") ?? "");
  return article || video ? origin : undefined;
}

function withReturn(
  next: URLSearchParams,
  from: URLSearchParams,
): URLSearchParams {
  const origin = connectionOrigin(from);
  if (origin.length <= 8192) next.set("connections_from", origin);
  return next;
}

export function connectionEvidenceParams(
  params: URLSearchParams,
  evidence: SourceReference,
): URLSearchParams {
  return withReturn(referenceParams(params, evidence), params);
}

export function connectionTargetParams(
  params: URLSearchParams,
  target: Destination,
): URLSearchParams | undefined {
  if (target.post) {
    const next = articleParams(params, target.post);
    next.set("connections", "graph");
    return withReturn(next, params);
  }
  if (
    target.youtube_item_id &&
    /^video:[A-Za-z0-9_-]{11}$/.test(target.youtube_item_id)
  ) {
    const next = new URLSearchParams({
      view: "youtube",
      feed: "linked",
      youtube_item: target.youtube_item_id,
      references: "graph",
    });
    return withReturn(next, params);
  }
}

export function connectionsClosedParams(
  params: URLSearchParams,
): URLSearchParams {
  const next = new URLSearchParams(params);
  clearConnectionParams(next);
  clearReferenceParams(next);
  next.delete("article");
  next.delete("article_source");
  return next;
}
