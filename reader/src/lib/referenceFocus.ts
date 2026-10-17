import { articleParams, clearReferenceParams } from "./navigation";

export { clearReferenceParams } from "./navigation";

import { matchesLinkUrl, normalizedLinkUrl } from "./linkIdentity";
import { youtubeEmbedUrl } from "./youtube";

export interface ReferenceTarget {
  kind: "post" | "comment";
  id: string;
  videoId: string;
  url: string;
  saved: string;
}

export function videoReferenceId(value: string): string | undefined {
  const embed = youtubeEmbedUrl(value, window.location.origin);

  return embed ? new URL(embed).pathname.split("/").at(-1) : undefined;
}

export interface SourceReference {
  domain: string;
  post_id: string;
  kind: "post" | "comment";
  reference_id: string;
  matched_url: string;
  observed_at: string;
}

export function referenceParams(
  params: URLSearchParams,
  reference: SourceReference,
): URLSearchParams {
  const next = articleParams(params, {
    domain: reference.domain,
    id: reference.post_id,
  });
  clearReferenceParams(next);
  const url = normalizedLinkUrl(reference.matched_url);
  const videoId = url?.match(
    /^https:\/\/www\.youtube\.com\/watch\?v=([A-Za-z0-9_-]{11})$/,
  )?.[1];

  if (url) {
    next.set("reference_kind", reference.kind);
    next.set("reference_id", reference.reference_id);
    next.set("reference_url", reference.matched_url);
    if (videoId) next.set("reference_video", videoId);
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
  const url =
    params.get("reference_url") ??
    (videoId ? `https://www.youtube.com/watch?v=${videoId}` : "");
  const identity = url.length <= 8192 ? normalizedLinkUrl(url) : undefined;

  if (
    !params.get("article") ||
    !params.get("article_source") ||
    (kind !== "comment" && kind !== "post") ||
    (kind === "comment" && (!id || id.length > 300)) ||
    !identity ||
    (videoId &&
      (!/^[A-Za-z0-9_-]{11}$/.test(videoId) ||
        identity !== `https://www.youtube.com/watch?v=${videoId}`))
  )
    return undefined;

  return {
    kind,
    id,
    videoId,
    url,
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

/** Mark actual destinations within the selected source body, never matching labels alone. */
export function highlightLinkReferences(
  scope: HTMLElement,
  url: string,
  base?: string,
) {
  for (const anchor of scope.querySelectorAll<HTMLAnchorElement>("a[href]")) {
    if (
      !visibleReference(anchor, scope) ||
      !matchesLinkUrl(anchor.getAttribute("href") ?? "", url, base)
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
      const candidate = matchesLinkUrl(match[0], url)
        ? match[0]
        : match[0].replace(/[.,!?;:)\]]+$/, "");

      if (!matchesLinkUrl(candidate, url)) continue;

      fragment.append(node.data.slice(offset, match.index));
      const mark = document.createElement("mark");
      mark.className = "reference-match";
      mark.dataset.referenceMatch = "true";
      mark.tabIndex = -1;
      mark.textContent = candidate;
      fragment.append(mark);
      offset = match.index + candidate.length;
    }

    if (offset) {
      fragment.append(node.data.slice(offset));
      node.replaceWith(fragment);
    }
  }
}

/** Compatibility for existing video deep links and consumers. */
export function highlightVideoReferences(scope: HTMLElement, videoId: string) {
  highlightLinkReferences(scope, `https://www.youtube.com/watch?v=${videoId}`);
}
