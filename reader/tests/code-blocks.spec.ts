import { expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const sql = [
  "-- on the shop server",
  "CREATE TABLE shop.customers (",
  "\tid integer PRIMARY KEY,",
  "\tname text NOT NULL,",
  "\tmarkup text DEFAULT '<img src=x onerror=\"window.readerCodeAttack=true\"> & <script>window.readerCodeAttack=true</script>'",
  ");",
  "SELECT id, name FROM shop.customers WHERE id > 42;",
  "",
].join("\n");

function escapeHtml(text: string) {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

const sqlHtml = `<pre class="chroma"><code class="language-sql" data-lang="sql"><span class="line"><span class="cl">${escapeHtml(sql).replace("CREATE", '<span class="k">CREATE</span>')}</span></span></code></pre>`;

async function openArticle(page: Page, html = sqlHtml) {
  const post: Post = {
    id: "code-example",
    domain: "example.com",
    title: "Reading code examples",
    url: "https://example.com/code-example",
    author: "Ada Writer",
    excerpt: "Code should be as readable as the article around it.",
    html,
    created_at: "2026-10-08T10:00:00Z",
    source: "rss",
    thumbnail: null,
    media: [],
  };
  await page.route("**/api/reader/**", (route) => {
    const path = new URL(route.request().url()).pathname;
    return route.fulfill({
      json: path.endsWith("/sources")
        ? {
            sources: [
              { domain: post.domain, name: "Example News", tags: [], count: 1 },
            ],
            total_posts: 1,
          }
        : path.endsWith("/post")
          ? post
          : { posts: [post], next_cursor: null, limited: false },
    });
  });
  await page.goto("/reader/?article=code-example&article_source=example.com");
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await expect(page.locator(".code-block").first()).toBeVisible();
}

test("highlights declared SQL while preserving source text and escaped HTML", async ({
  page,
}) => {
  await openArticle(page);
  const block = page.locator(".code-block");
  const code = block.locator("pre code");
  await expect(block.locator(".code-block-header")).toContainText("SQL");
  await expect(code.locator(".hljs-keyword").first()).toBeVisible();
  await expect(code.locator(".hljs-string").first()).toBeVisible();
  await expect(code.locator(".hljs-comment").first()).toBeVisible();
  expect(await code.textContent()).toBe(sql);
  await expect(code.locator(".line, .cl, .k")).toHaveCount(0);
  await expect(code.locator("img, script, [onerror]")).toHaveCount(0);
  expect(
    await page.evaluate(
      () =>
        (window as Window & { readerCodeAttack?: boolean }).readerCodeAttack,
    ),
  ).toBeUndefined();
  const colors = await Promise.all(
    [
      code,
      ...[".hljs-keyword", ".hljs-string", ".hljs-comment"].map((selector) =>
        code.locator(selector).first(),
      ),
    ].map((token) =>
      token.evaluate((element) => getComputedStyle(element).color),
    ),
  );
  expect(new Set(colors).size).toBe(4);
});

test("supports language aliases and metadata without guessing plaintext", async ({
  page,
}) => {
  const javascript = "const answer = 42;\n";
  const plain = "SELECT id FROM shop.customers;\n";
  await openArticle(
    page,
    [
      `<pre><code class="lang-js">${javascript}</code></pre>`,
      `<pre class="language-sql"><code>${plain}</code></pre>`,
      `<pre><code data-lang="sql">${plain}</code></pre>`,
      `<pre data-language="sql">${plain}</pre>`,
      `<pre><code class="language-unknown-example">${plain}</code></pre>`,
      `<pre><code>${plain}</code></pre>`,
      `<pre class="nohighlight"><code class="language-sql">${plain}</code></pre>`,
    ].join(""),
  );
  const blocks = page.locator(".code-block");
  await expect(blocks).toHaveCount(7);
  for (let index = 0; index < 4; index += 1) {
    const code = blocks.nth(index).locator("pre code");
    await expect(code.locator(".hljs-keyword").first()).toBeVisible();
    expect(await code.textContent()).toBe(index === 0 ? javascript : plain);
  }
  for (let index = 4; index < 7; index += 1) {
    const code = blocks.nth(index).locator("pre code");
    await expect(code.locator('[class^="hljs-"]')).toHaveCount(0);
    expect(await code.textContent()).toBe(plain);
  }
});

test("copies the exact code without labels or upstream markup", async ({
  page,
}) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: async (text: string) => {
          (window as Window & { copiedCode?: string }).copiedCode = text;
        },
      },
    });
  });
  await openArticle(page);
  const block = page.locator(".code-block");
  await block.getByRole("button", { name: "Copy code", exact: true }).click();
  await expect(block).toContainText("Copied");
  expect(
    await page.evaluate(
      () => (window as Window & { copiedCode?: string }).copiedCode,
    ),
  ).toBe(sql);
});

test("reports clipboard failure without claiming the code was copied", async ({
  page,
}) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: async () => {
          throw new DOMException("Clipboard denied", "NotAllowedError");
        },
      },
    });
  });
  await openArticle(page);
  const block = page.locator(".code-block");
  await block.getByRole("button", { name: "Copy code", exact: true }).click();
  await expect(block).toContainText(/couldn’t|could not|unable|failed/i);
  await expect(block).not.toContainText("Copied");
  expect(await block.locator("pre code").textContent()).toBe(sql);
});

test("long code scrolls with the keyboard on mobile without widening the page", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openArticle(page);
  const pre = page.locator(".code-block pre");
  await expect(pre.locator(".hljs-keyword").first()).toBeVisible();
  await expect(pre).toHaveAttribute("tabindex", "0");
  await expect(pre).toHaveCSS("white-space", "pre");
  expect(
    await pre.evaluate((element) => element.scrollWidth > element.clientWidth),
  ).toBe(true);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await pre.focus();
  await expect(pre).toBeFocused();
  await pre.press("ArrowRight");
  await expect
    .poll(() => pre.evaluate((element) => element.scrollLeft))
    .toBeGreaterThan(0);
  expect(await pre.locator("code").textContent()).toBe(sql);
});

test("resizing article text preserves highlights, code nodes and horizontal position", async ({
  page,
}) => {
  await page.setViewportSize({ width: 983, height: 898 });
  await openArticle(page);
  const pre = page.locator(".code-block pre");
  await expect(pre.locator(".hljs-keyword").first()).toBeVisible();
  const original = await pre.elementHandle();
  const markup = await pre.locator("code").innerHTML();
  await pre.evaluate((element) => {
    element.scrollLeft = 120;
  });
  const position = await pre.evaluate((element) => element.scrollLeft);
  expect(position).toBeGreaterThan(0);
  const size = await page
    .locator(".article-body")
    .evaluate((element) =>
      Number.parseFloat(getComputedStyle(element).fontSize),
    );
  await page.getByRole("button", { name: "Larger article text" }).click();
  await expect(page.locator(".article-body")).toHaveCSS(
    "font-size",
    `${size + 1}px`,
  );
  expect(await original?.evaluate((element) => element.isConnected)).toBe(true);
  expect(await pre.locator("code").innerHTML()).toBe(markup);
  expect(await pre.locator("code").textContent()).toBe(sql);
  expect(await pre.evaluate((element) => element.scrollLeft)).toBe(position);
  await expect(pre.locator(".hljs-keyword").first()).toBeVisible();
});
