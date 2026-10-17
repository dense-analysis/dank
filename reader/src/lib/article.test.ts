import { describe, expect, it } from "vitest";
import { articleText, hasArticleContent, renderArticle } from "./article";
import { highlightCode } from "./highlightCode";

describe("collected article content", () => {
  it("keeps paragraph boundaries in compact X previews without including unsafe markup", () => {
    expect(
      articleText(
        "<p>First &amp; second</p><p>Next line<br>Last line</p><script>bad()</script>",
      ),
    ).toBe("First & second\nNext line\nLast line");
  });

  it("highlights literal search text inside sanitized markup without changing link destinations", () => {
    const body = document.createElement("div");
    renderArticle(
      body,
      '<p>Read <a href="https://example.com/research">C++ research</a>.</p><script>bad()</script>',
      "C++",
    );
    expect(body.querySelector("mark")?.textContent).toBe("C++");
    expect(body.querySelector("a")?.href).toBe("https://example.com/research");
    expect(body.querySelector("script")).toBeNull();
    expect(body.textContent).toBe("Read C++ research.");
  });

  it.each([
    "",
    "\n\n  ",
    "<div><p>&nbsp;<br></p></div>",
    '<script>articleData()</script><style>.loading {}</style><iframe src="https://example.com"></iframe>',
    '<img src="javascript:attack()">',
  ])("recognizes an empty or unreadable body: %s", (html) => {
    expect(hasArticleContent(html)).toBe(false);
  });

  it.each([
    "<p>A short article.</p>",
    '<figure><img src="/assets/photo.png"></figure>',
    '<video controls><source src="/assets/clip.mp4"></video>',
    '<audio controls src="/assets/interview.mp3"></audio>',
    "<pre>&lt;script&gt;Example code&lt;/script&gt;</pre>",
  ])("recognizes readable text and media: %s", (html) => {
    expect(hasArticleContent(html)).toBe(true);
  });
});

describe("article code rendering", () => {
  it("retains figure captions and opens the local image at full size", () => {
    const body = document.createElement("div");
    renderArticle(
      body,
      '<p>Before.</p><figure onclick="attack()"><img src="/assets/diagram.png" alt="Diagram" onerror="attack()"><figcaption style="position:fixed">Credit: Lab</figcaption></figure><p>After.</p>',
    );
    const figure = body.querySelector("figure");
    expect(figure?.previousElementSibling?.textContent).toBe("Before.");
    expect(figure?.nextElementSibling?.textContent).toBe("After.");
    expect(figure?.querySelector("figcaption")?.textContent).toBe(
      "Credit: Lab",
    );
    expect(figure?.querySelector("a")?.getAttribute("href")).toBe(
      new URL("/assets/diagram.png", window.location.origin).href,
    );
    expect(body.querySelector("[onclick], [onerror], [style]")).toBeNull();
  });
  it("preserves table structure while isolating scrolling from the table grid", () => {
    const body = document.createElement("div");
    renderArticle(
      body,
      '<table style="position:fixed" onclick="attack()"><caption>Results</caption><colgroup span="2"></colgroup><thead><tr><th colspan="2" scope="colgroup">Models</th></tr></thead><tbody><tr><th rowspan="2" scope="rowgroup">Task</th><td>11</td></tr><tr><td>22</td></tr></tbody><tfoot><tr><td colspan="2">Total</td></tr></tfoot></table>',
    );
    const table = body.querySelector("table");
    expect(table?.querySelector("thead th")?.getAttribute("colspan")).toBe("2");
    expect(table?.querySelector("tbody th")?.getAttribute("rowspan")).toBe("2");
    expect(table?.querySelector("tbody th")?.getAttribute("scope")).toBe(
      "rowgroup",
    );
    expect(table?.querySelector("colgroup")?.getAttribute("span")).toBe("2");
    expect(table?.querySelector("tfoot td")?.textContent).toBe("Total");
    expect(table?.parentElement?.className).toBe("table-scroll");
    expect(table?.parentElement?.getAttribute("aria-label")).toBe("Results");
    expect(table?.parentElement?.getAttribute("tabindex")).toBe("0");
    expect(body.querySelector("[style], [onclick]")).toBeNull();
  });
  it("replaces source lightbox placeholders with direct image links", () => {
    const body = document.createElement("div");
    renderArticle(
      body,
      '<p><a href="#lightbox"><img src="/diagram.png" alt="Diagram"></a></p>',
    );
    const link = body.querySelector<HTMLAnchorElement>(".article-image-link");
    expect(link?.href).toBe(
      new URL("/diagram.png", window.location.origin).href,
    );
    expect(body.querySelector("a a")).toBeNull();
  });
  it("opens unlinked images at full size while preserving authored image links", () => {
    const body = document.createElement("div");
    renderArticle(
      body,
      '<img src="/diagram.png" alt="System diagram"><a href="https://example.com/full-size.png"><img src="/thumbnail.png" alt="Preview"></a><img src="javascript:attack()">',
    );
    const link = body.querySelector<HTMLAnchorElement>(".article-image-link");
    expect(link?.href).toBe(
      new URL("/diagram.png", window.location.origin).href,
    );
    expect(link?.target).toBe("_blank");
    expect(link?.rel).toBe("noopener noreferrer");
    expect(link?.getAttribute("aria-label")).toBe(
      "Open full-size image: System diagram",
    );
    expect(
      body.querySelector('a[href="https://example.com/full-size.png"] img'),
    ).not.toBeNull();
    expect(body.querySelector("a a, [href^='javascript:']")).toBeNull();
  });
  it("sanitizes source markup before exposing hosts for React controls", () => {
    const body = document.createElement("div");
    const { codeBlocks: blocks, images } = renderArticle(
      body,
      `<pre onclick="attack()" style="position:fixed"><code class="language-sql">SELECT '&lt;script&gt;attack()&lt;/script&gt;' AS example;</code><img src="/inline.svg" onerror="attack()"></pre><script>attack()</script><button onclick="attack()">Untrusted control</button>`,
    );
    highlightCode(blocks);
    expect(body.querySelector("script, [onclick], [style]")).toBeNull();
    expect(body.querySelector("button")).toBeNull();
    expect(body.contains(blocks[0].controls)).toBe(true);
    expect(images).toHaveLength(0);
    expect(blocks[0].code.textContent).toBe(
      "SELECT '<script>attack()</script>' AS example;",
    );
    expect(blocks[0].code.querySelector(".hljs-keyword")?.textContent).toBe(
      "SELECT",
    );
  });

  it("treats language metadata as data and ignores malformed labels", () => {
    const body = document.createElement("div");
    const { codeBlocks: blocks } = renderArticle(
      body,
      `<pre data-lang="&lt;img src=x onerror=attack()&gt;">SELECT 1;</pre>`,
    );
    highlightCode(blocks);
    expect(blocks[0].label.textContent).toBe("Code");
    expect(body.querySelector("img")).toBeNull();
    expect(blocks[0].code.innerHTML).toBe("SELECT 1;");
  });

  it("retains oversized code unchanged as selectable plain text", () => {
    const source = "SELECT 1;\n".repeat(6000);
    const body = document.createElement("div");
    const { codeBlocks: blocks } = renderArticle(
      body,
      `<pre><code class="language-sql">${source}</code></pre>`,
    );
    highlightCode(blocks);
    expect(blocks[0].code.textContent).toBe(source);
    expect(blocks[0].code.children).toHaveLength(0);
  });

  it("keeps whitespace and language aliases when source spans are removed", () => {
    const body = document.createElement("div");
    const { codeBlocks: blocks } = renderArticle(
      body,
      '<pre class="language-js"><code><span>const</span> example = "a &amp; b";\n\tconsole.log(example);\n</code></pre>',
    );
    highlightCode(blocks);
    expect(blocks[0].code.textContent).toBe(
      'const example = "a & b";\n\tconsole.log(example);\n',
    );
    expect(blocks[0].label.textContent).toBe("JavaScript");
    expect(blocks[0].code.querySelector(".hljs-keyword")?.textContent).toBe(
      "const",
    );
  });
});
