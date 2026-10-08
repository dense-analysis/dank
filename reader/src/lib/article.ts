import DOMPurify from "dompurify";

export interface ArticleCodeBlock {
  code: HTMLElement;
  text: string;
  language: string | null;
  label: HTMLElement;
  copy: HTMLButtonElement;
  status: HTMLElement;
}

function codeLanguage(code: Element, pre: Element): string | null {
  const elements = code === pre ? [pre] : [code, pre];
  if (
    elements.some((element) => element.matches(".nohighlight, .no-highlight"))
  )
    return "plaintext";
  for (const element of elements) {
    const candidate =
      [...element.classList]
        .find((value) => /^(language|lang)-/i.test(value))
        ?.replace(/^(language|lang)-/i, "") ??
      element.getAttribute("data-language") ??
      element.getAttribute("data-lang");
    if (!candidate) continue;
    const language = candidate.trim().toLowerCase();
    if (/^[a-z0-9][a-z0-9_+#.-]{0,39}$/.test(language))
      return ["text", "txt", "plain", "none"].includes(language)
        ? "plaintext"
        : language;
  }
  return null;
}

export function renderArticle(
  element: HTMLElement,
  html: string,
): ArticleCodeBlock[] {
  // Source markup is sanitized before adding any reader-owned elements.
  element.replaceChildren(
    DOMPurify.sanitize(html, {
      USE_PROFILES: { html: true },
      FORBID_TAGS: ["form", "input", "button", "iframe", "style"],
      FORBID_ATTR: ["style", "id", "name"],
      RETURN_DOM_FRAGMENT: true,
    }),
  );
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

  return [...element.querySelectorAll("pre")].map((pre) => {
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
    const status = document.createElement("span");
    status.className = "code-copy-status";
    status.setAttribute("role", "status");
    const copy = document.createElement("button");
    copy.type = "button";
    copy.className = "code-copy";
    copy.setAttribute("aria-label", "Copy code");
    copy.textContent = "Copy";
    header.append(label, status, copy);
    pre.replaceWith(block);
    block.append(header, pre);
    return { code, text, language, label, copy, status };
  });
}
