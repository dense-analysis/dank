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
