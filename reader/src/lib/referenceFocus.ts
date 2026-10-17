import { articleParams } from "./navigation";
import { youtubeEmbedUrl } from "./youtube";
import type { YouTubeReference } from "./youtubeCollection";

export interface ReferenceTarget {
  kind: "post" | "comment";
  id: string;
  videoId: string;
  saved: string;
}

const referenceKeys = [
  "reference_kind",
  "reference_id",
  "reference_video",
  "reference_saved",
];

export function clearReferenceParams(params: URLSearchParams) {
  for (const key of referenceKeys) params.delete(key);
}

export function videoReferenceId(value: string): string | undefined {
  const embed = youtubeEmbedUrl(value, window.location.origin);

  return embed ? new URL(embed).pathname.split("/").at(-1) : undefined;
}

export function referenceParams(
  params: URLSearchParams,
  reference: YouTubeReference,
): URLSearchParams {
  const next = articleParams(params, {
    domain: reference.domain,
    id: reference.post_id,
  });
  clearReferenceParams(next);
  const videoId = videoReferenceId(reference.matched_url);

  if (videoId) {
    next.set("reference_kind", reference.kind);
    next.set("reference_id", reference.reference_id);
    next.set("reference_video", videoId);
    if (reference.observed_at)
      next.set("reference_saved", reference.observed_at);
  }

  return next;
}

export function parseReferenceTarget(
  params: URLSearchParams,
): ReferenceTarget | undefined {
  const kind = params.get("reference_kind");
  const id = params.get("reference_id") ?? "";
  const videoId = params.get("reference_video") ?? "";
  const saved = params.get("reference_saved") ?? "";

  if (
    !params.get("article") ||
    !params.get("article_source") ||
    (kind !== "comment" && kind !== "post") ||
    (kind === "comment" && (!id || id.length > 300)) ||
    !/^[A-Za-z0-9_-]{11}$/.test(videoId)
  )
    return undefined;

  return {
    kind,
    id,
    videoId,
    saved: saved && Number.isFinite(Date.parse(saved)) ? saved : "",
  };
}

function visibleReference(node: Element, scope: HTMLElement): boolean {
  for (
    let current: Element | null = node;
    current && current !== scope.parentElement;
    current = current.parentElement
  ) {
    if (
      current.matches(
        "[hidden], nav, header, footer, template, noscript, [data-reader-comment-boundary], [data-reader-reference-excluded]",
      )
    )
      return false;
  }

  return true;
}

/** Mark only verified video destinations, never text that merely names an ID. */
export function highlightVideoReferences(scope: HTMLElement, videoId: string) {
  for (const anchor of scope.querySelectorAll<HTMLAnchorElement>("a[href]")) {
    if (
      !visibleReference(anchor, scope) ||
      videoReferenceId(anchor.href) !== videoId
    )
      continue;

    anchor.classList.add("reference-match");
    anchor.dataset.referenceMatch = "true";
  }

  const walker = document.createTreeWalker(scope, NodeFilter.SHOW_TEXT);
  const textNodes: Text[] = [];

  while (walker.nextNode()) {
    const node = walker.currentNode as Text;

    if (
      node.parentElement &&
      visibleReference(node.parentElement, scope) &&
      !node.parentElement.closest(
        "a, pre, code, .reference-match, [data-reader-comment-boundary]",
      )
    )
      textNodes.push(node);
  }

  for (const node of textNodes) {
    const fragment = document.createDocumentFragment();
    let offset = 0;

    for (const match of node.data.matchAll(/https?:\/\/[^\s<>"']+/gi)) {
      const url = match[0].replace(/[.,!?;:)\]]+$/, "");

      if (videoReferenceId(url) !== videoId) continue;

      fragment.append(node.data.slice(offset, match.index));
      const mark = document.createElement("mark");
      mark.className = "reference-match";
      mark.dataset.referenceMatch = "true";
      mark.tabIndex = -1;
      mark.textContent = url;
      fragment.append(mark);
      offset = match.index + url.length;
    }

    if (offset) {
      fragment.append(node.data.slice(offset));
      node.replaceWith(fragment);
    }
  }
}
