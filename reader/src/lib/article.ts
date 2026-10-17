import DOMPurify from "dompurify";
import { highlightMatches } from "./highlightMatches";

export function sanitizeArticle(html: string) {
  return DOMPurify.sanitize(html, {
    USE_PROFILES: { html: true },
    FORBID_TAGS: ["form", "input", "button", "iframe", "style"],
    FORBID_ATTR: ["style", "id", "name"],
    RETURN_DOM_FRAGMENT: true,
  });
}

export function hasArticleContent(html: string): boolean {
  const content = sanitizeArticle(html);

  return Boolean(
    content.textContent?.trim() ||
      content.querySelector("img[src], video[src], audio[src], source[src]"),
  );
}

export function articleText(html: string): string {
  const content = sanitizeArticle(html);

  for (const block of content.querySelectorAll("p, div, li, br")) {
    block.append(document.createTextNode("\n"));
  }

  return content.textContent?.trim() ?? "";
}

export interface ArticleCodeBlock {
  code: HTMLElement;
  text: string;
  language: string | null;
  label: HTMLElement;
  controls: HTMLElement;
}

export interface ArticleImage {
  image: HTMLImageElement;
  frame: HTMLElement;
}

export interface ArticleContent {
  codeBlocks: ArticleCodeBlock[];
  images: ArticleImage[];
}

function codeLanguage(code: Element, pre: Element): string | null {
  const elements = code === pre ? [pre] : [code, pre];

  if (
    elements.some((element) => element.matches(".nohighlight, .no-highlight"))
  ) {
    return "plaintext";
  }

  for (const element of elements) {
    const candidate =
      [...element.classList]
        .find((value) => /^(language|lang)-/i.test(value))
        ?.replace(/^(language|lang)-/i, "") ??
      element.getAttribute("data-language") ??
      element.getAttribute("data-lang");

    if (!candidate) continue;

    const language = candidate.trim().toLowerCase();

    if (/^[a-z0-9][a-z0-9_+#.-]{0,39}$/.test(language)) {
      return ["text", "txt", "plain", "none"].includes(language)
        ? "plaintext"
        : language;
    }
  }

  return null;
}

export function renderArticle(
  element: HTMLElement,
  html: string,
  query = "",
): ArticleContent {
  // Source markup is sanitized before adding any reader-owned elements.
  element.replaceChildren(sanitizeArticle(html));

  if (query.trim()) {
    const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
    const nodes: Text[] = [];

    while (walker.nextNode()) nodes.push(walker.currentNode as Text);

    for (const node of nodes) {
      const parts = highlightMatches(node.data, query);
      if (!parts.some((part) => part.highlighted)) continue;

      node.replaceWith(
        ...parts.map((part) => {
          if (!part.highlighted) return document.createTextNode(part.text);

          const mark = document.createElement("mark");
          mark.textContent = part.text;

          return mark;
        }),
      );
    }
  }

  for (const link of element.querySelectorAll("a")) {
    link.target = "_blank";
    link.rel = "noopener noreferrer";
  }

  for (const [index, table] of element.querySelectorAll("table").entries()) {
    const container = document.createElement("div");
    container.className = "table-scroll";
    container.tabIndex = 0;
    container.setAttribute("role", "region");
    container.setAttribute(
      "aria-label",
      table.caption?.textContent?.trim() || `Article table ${index + 1}`,
    );
    table.replaceWith(container);
    container.append(table);
  }

  for (const image of element.querySelectorAll("img")) {
    image.loading = "lazy";
    image.referrerPolicy = "no-referrer";
    const existingHref = image.closest("a")?.getAttribute("href")?.trim();

    if (existingHref && !existingHref.startsWith("#")) continue;

    try {
      const source = image.getAttribute("src");

      if (!source) continue;

      const url = new URL(source, window.location.origin);

      if (!["http:", "https:"].includes(url.protocol)) continue;

      const link = document.createElement("a");
      link.href = url.href;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.className = "article-image-link";
      link.title = "Open full-size image in a new tab";
      link.setAttribute(
        "aria-label",
        image.alt
          ? `Open full-size image: ${image.alt}`
          : "Open full-size image",
      );

      // Remove an empty source anchor rather than create nested links.
      const emptyLink = image.closest("a");

      if (emptyLink) emptyLink.replaceWith(...emptyLink.childNodes);

      image.replaceWith(link);
      link.append(image);
    } catch {
      // An invalid source remains an ordinary image, never a navigation target.
    }
  }

  const codeBlocks = [...element.querySelectorAll("pre")].map((pre) => {
    const originalCode = pre.querySelector("code") ?? pre;
    const language = codeLanguage(originalCode, pre);
    const text = pre.textContent ?? "";
    const code = document.createElement("code");
    code.textContent = text;

    // Rebuild from text to discard source-site highlighting and preserve whitespace.
    pre.replaceChildren(code);
    pre.className = "code-block-content";
    pre.tabIndex = 0;
    pre.setAttribute("role", "region");
    pre.setAttribute(
      "aria-label",
      language && language !== "plaintext"
        ? `${language.toUpperCase()} code`
        : "Code",
    );

    const block = document.createElement("div");
    block.className = "code-block";
    const header = document.createElement("div");
    header.className = "code-block-header";
    const label = document.createElement("span");
    label.className = "code-language";
    label.textContent =
      language === "plaintext"
        ? "Plain text"
        : (language?.toUpperCase() ?? "Code");
    header.append(label);
    pre.replaceWith(block);
    block.append(header, pre);

    return { code, text, language, label, controls: header };
  });

  const images: ArticleImage[] = [];

  for (const image of element.querySelectorAll("img")) {
    const target = image.closest("a") ?? image.closest("picture") ?? image;

    // Keep controls outside links; leave links containing several images intact.
    if (target.querySelectorAll("img").length > 1) continue;

    const frame = document.createElement("span");
    frame.className = "article-image-host";
    target.replaceWith(frame);
    frame.append(target);
    images.push({ image, frame });
  }

  return { codeBlocks, images };
}
