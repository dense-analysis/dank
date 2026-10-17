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

// Match only native X video references or the exact playable media address.
export function videoReferenceKey(value: string): string | undefined {
  try {
    const label =
      /^(?:(?:www\.)?(?:x|twitter)\.com|mobile\.twitter\.com|video\.twimg\.com)\//i.test(
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
    const path = /^\/[^/]+\/status\/(\d+)\/video\/(\d+)\/?$/i.exec(
      url.pathname,
    );

    if (
      /^(?:(?:www\.)?(?:x|twitter)\.com|mobile\.twitter\.com)$/i.test(
        url.hostname,
      ) &&
      path
    )
      return `x-video:${path[1]}:${path[2]}`;
    if (
      (url.origin === window.location.origin &&
        url.pathname.startsWith("/assets/")) ||
      (url.protocol === "https:" && url.hostname === "video.twimg.com")
    )
      return url.href;
  } catch {
    return undefined;
  }

  return undefined;
}

export function withoutDisplayedVideoLinks(
  html: string,
  displayed: Set<string>,
): string {
  if (!displayed.size) return html;

  const content = sanitizeArticle(html);

  for (const anchor of content.querySelectorAll("a[href]")) {
    const key = videoReferenceKey(anchor.getAttribute("href") ?? "");
    if (
      !key ||
      !displayed.has(key) ||
      videoReferenceKey(anchor.textContent?.trim() ?? "") !== key
    )
      continue;

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
