import { sanitizeArticle } from "./article";

export interface XTextSegment {
  text: string;
  start: number;
  href?: string;
}

// X processing emits paragraphs, line breaks and links; React owns their controls.
export function xTextParagraphs(html: string): XTextSegment[][] {
  const paragraphs: XTextSegment[][] = [];
  let segments: XTextSegment[] = [];
  let offset = 0;

  function finish() {
    if (segments.some((segment) => segment.text.trim()))
      paragraphs.push(segments);
    segments = [];
  }

  function append(text: string, href?: string) {
    if (!text) return;

    const previous = segments.at(-1);

    if (previous && previous.href === href) previous.text += text;
    else segments.push({ text, href, start: offset });
    offset += text.length;
  }

  function visit(node: Node) {
    if (node.nodeType === Node.TEXT_NODE) {
      append(node.textContent ?? "");
      return;
    }
    if (!(node instanceof Element)) return;

    if (node.tagName === "A") {
      const href = node.getAttribute("href");
      let safe: string | undefined;

      try {
        const url = href ? new URL(href, window.location.origin) : null;
        if (url && ["http:", "https:"].includes(url.protocol)) safe = url.href;
      } catch {
        // Invalid links remain readable text.
      }

      append(node.textContent ?? "", safe);
      return;
    }
    if (node.tagName === "BR") {
      append("\n");
      return;
    }

    const block = ["P", "DIV", "LI", "BLOCKQUOTE", "PRE"].includes(
      node.tagName,
    );
    if (block) finish();
    for (const child of node.childNodes) visit(child);
    if (block) finish();
  }

  for (const node of sanitizeArticle(html).childNodes) visit(node);
  finish();

  return paragraphs;
}

export function videoReferenceKey(value: string): string | undefined {
  return mediaReferenceKey(value, "video");
}

export function imageReferenceKey(value: string): string | undefined {
  return mediaReferenceKey(value, "photo");
}

// Match native X media references or the exact attachment address.
function mediaReferenceKey(
  value: string,
  kind: "photo" | "video",
): string | undefined {
  const mediaHost = kind === "photo" ? "pbs.twimg.com" : "video.twimg.com";

  try {
    const label =
      /^(?:(?:www\.)?(?:x|twitter)\.com|mobile\.twitter\.com|(?:video|pbs)\.twimg\.com)\//i.test(
        value,
      )
        ? `https://${value}`
        : value;
    const url = new URL(label, window.location.origin);
    if (
      !["http:", "https:"].includes(url.protocol) ||
      url.username ||
      url.password
    )
      return undefined;
    const path = /^\/[^/]+\/status\/(\d+)\/(photo|video)\/(\d+)\/?$/i.exec(
      url.pathname,
    );

    if (
      /^(?:(?:www\.)?(?:x|twitter)\.com|mobile\.twitter\.com)$/i.test(
        url.hostname,
      ) &&
      path &&
      path[2].toLowerCase() === kind
    )
      return `x-${kind}:${path[1]}:${path[3]}`;
    if (
      (url.origin === window.location.origin &&
        url.pathname.startsWith("/assets/")) ||
      (url.protocol === "https:" && url.hostname === mediaHost)
    )
      return url.href;
  } catch {
    return undefined;
  }

  return undefined;
}

export function previewReferenceKey(value: string): string | undefined {
  if (!value) return undefined;

  try {
    const url = new URL(value, window.location.origin);
    if (
      ["http:", "https:"].includes(url.protocol) &&
      !url.username &&
      !url.password
    )
      return url.href;
  } catch {
    return undefined;
  }

  return undefined;
}

export function withoutDisplayedLinks(
  html: string,
  displayedMedia: Set<string>,
  displayedPreviews: Set<string> = new Set(),
): string {
  if (!displayedMedia.size && !displayedPreviews.size) return html;

  const content = sanitizeArticle(html);

  for (const anchor of content.querySelectorAll("a[href]")) {
    const href = anchor.getAttribute("href") ?? "";
    const label = anchor.textContent?.trim() ?? "";
    const mediaKey = videoReferenceKey(href) ?? imageReferenceKey(href);
    const previewKey = previewReferenceKey(href);
    const representedMedia =
      mediaKey &&
      displayedMedia.has(mediaKey) &&
      (videoReferenceKey(label) ?? imageReferenceKey(label)) === mediaKey;
    const representedPreview =
      previewKey &&
      displayedPreviews.has(previewKey) &&
      [
        href,
        previewKey,
        href.replace(/^https?:\/\//i, ""),
        previewKey.replace(/^https?:\/\//i, ""),
      ].includes(label);

    if (!representedMedia && !representedPreview) continue;

    const parent = anchor.parentElement;
    anchor.remove();

    if (parent?.matches("p, div")) {
      while (
        parent.lastChild &&
        ((parent.lastChild instanceof Element &&
          parent.lastChild.tagName === "BR") ||
          (parent.lastChild.nodeType === Node.TEXT_NODE &&
            !parent.lastChild.textContent?.trim()))
      )
        parent.lastChild.remove();
      if (
        !parent.textContent?.trim() &&
        !parent.querySelector("img, video, audio")
      )
        parent.remove();
    }
  }

  const container = document.createElement("div");
  container.append(content);

  return container.innerHTML;
}
