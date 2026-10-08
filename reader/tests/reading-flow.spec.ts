import { type BrowserContext, expect, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const article: Post = {
  id: "shared",
  domain: "example.com",
  url: "https://example.com/story",
  author: "Ada Writer",
  title: "Reading C++ examples",
  excerpt: "A technical introduction.",
  html:
    '<p><img src="/test-diagram.svg" alt="Architecture diagram"></p>' +
    Array.from(
      { length: 45 },
      (_, index) =>
        `<p>Section ${index}. A useful article explains the design with concrete examples. Keep the current paragraph in view when returning to this story. This paragraph provides enough text to read across multiple lines.</p>`,
    ).join(""),
  created_at: "2026-10-08T10:00:00Z",
  source: "rss",
  thumbnail: null,
  media: [],
};
const other: Post = {
  ...article,
  domain: "other.example",
  title: "Another source's story",
};

async function mockReader(context: BrowserContext) {
  await context.route("**/test-diagram.svg", (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="600"><rect width="1200" height="600" fill="#315b48"/><text x="80" y="100" fill="white" font-size="40">Architecture diagram</text></svg>',
    }),
  );
  await context.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());
    const posts = [article, other];
    if (url.pathname.endsWith("/sources"))
      return route.fulfill({
        json: {
          sources: posts.map((post) => ({
            domain: post.domain,
            name: post.domain,
            tags: [],
            count: 1,
          })),
          total_posts: 2,
        },
      });
    if (url.pathname.endsWith("/post"))
      return route.fulfill({
        json: posts.find(
          (post) => post.domain === url.searchParams.get("domain"),
        ),
      });
    const domains = url.searchParams.getAll("domain");
    return route.fulfill({
      json: {
        posts: posts
          .filter((post) => !domains.length || domains.includes(post.domain))
          .map((post) => ({
            ...post,
            excerpt: url.searchParams.has("q")
              ? '… A needle in C++ examples. <img src=x onerror="window.previewAttack=true">'
              : post.excerpt,
          })),
        next_cursor: null,
        limited: false,
      },
    });
  });
}

test("article links support middle-click and keep search context in a new tab", async ({
  page,
  context,
}) => {
  await mockReader(context);
  await page.goto("/reader/?q=examples&sort=oldest");
  const link = page.getByRole("link", {
    name: `Read ${article.title}`,
    exact: true,
  });
  await expect(link).toHaveAttribute("href", /article=shared/);
  const originalUrl = page.url();
  const [tab] = await Promise.all([
    context.waitForEvent("page"),
    link.click({ button: "middle" }),
  ]);
  await expect(
    tab.getByRole("heading", { name: article.title, exact: true }),
  ).toBeVisible();
  expect(new URL(tab.url()).searchParams.get("q")).toBe("examples");
  expect(new URL(tab.url()).searchParams.get("sort")).toBe("oldest");
  expect(new URL(tab.url()).searchParams.get("article_source")).toBe(
    article.domain,
  );
  expect(page.url()).toBe(originalUrl);
  await expect(
    page
      .locator(".post-card")
      .filter({ has: link })
      .getByText("Read", { exact: true }),
  ).toBeVisible();
  await tab.close();
});

test("source links filter stories from both the list and article header", async ({
  page,
  context,
}) => {
  await mockReader(context);
  await page.goto("/reader/");
  await page
    .getByRole("link", { name: "Stories from example", exact: true })
    .click();
  expect(new URL(page.url()).searchParams.getAll("domain")).toEqual([
    article.domain,
  ]);
  await expect(
    page.getByRole("link", { name: `Read ${other.title}`, exact: true }),
  ).toHaveCount(0);
  await page
    .getByRole("link", { name: `Read ${article.title}`, exact: true })
    .click();
  const reader = page.getByRole("main", { name: "Article reader" });
  await reader
    .getByRole("link", { name: "Stories from example", exact: true })
    .click();
  await expect(reader).toHaveCount(0);
  expect(new URL(page.url()).searchParams.getAll("domain")).toEqual([
    article.domain,
  ]);
  await expect(
    page.getByRole("link", { name: `Read ${article.title}`, exact: true }),
  ).toBeVisible();
});

test("article diagrams open their image URL in a new tab without a modal", async ({
  page,
  context,
}) => {
  await mockReader(context);
  await page.goto("/reader/?article=shared&article_source=example.com");
  const link = page.getByRole("link", {
    name: "Open full-size image: Architecture diagram",
    exact: true,
  });
  await expect(link).toHaveAttribute("target", "_blank");
  await expect(link).toHaveAttribute("rel", "noopener noreferrer");
  const [tab] = await Promise.all([context.waitForEvent("page"), link.click()]);
  await tab.waitForURL("**/test-diagram.svg");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  expect(await tab.evaluate(() => window.opener === null)).toBe(true);
  await tab.close();
});

test("search highlights literal terms as text without executing excerpt markup", async ({
  page,
  context,
}) => {
  await mockReader(context);
  await page.goto("/reader/?q=needle+C%2B%2B");
  const card = page.locator(".post-card").first();
  await expect(card.locator("h2 mark")).toHaveText("C++");
  await expect(card.locator("p mark")).toHaveText(["needle", "C++"]);
  await expect(card.locator("p")).toContainText("<img src=x");
  await expect(card.locator("p img")).toHaveCount(0);
  expect(
    await page.evaluate(() => Reflect.get(window, "previewAttack")),
  ).toBeUndefined();
});

test("reopening and reloading long articles resumes the same paragraph by source", async ({
  page,
  context,
}) => {
  await mockReader(context);
  await page.goto("/reader/");
  const story = page.getByRole("link", {
    name: `Read ${article.title}`,
    exact: true,
  });
  await story.click();
  const paragraph = page.locator(".article-body > p").nth(22);
  await page.evaluate(() => document.fonts.ready);
  await page
    .locator(".article-body img")
    .evaluate((image) => (image as HTMLImageElement).decode());
  await paragraph.evaluate((element) =>
    window.scrollTo(
      0,
      window.scrollY + element.getBoundingClientRect().top - 90,
    ),
  );
  const before = await paragraph.evaluate(
    (element) => element.getBoundingClientRect().top,
  );
  await expect
    .poll(() =>
      page.evaluate(() => localStorage.getItem("dank-reader:positions:v1")),
    )
    .not.toBeNull();
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await story.click();
  await expect
    .poll(() =>
      paragraph.evaluate((element) => element.getBoundingClientRect().top),
    )
    .toBeCloseTo(before, 0);
  await page.reload();
  await expect
    .poll(() =>
      paragraph.evaluate((element) => element.getBoundingClientRect().top),
    )
    .toBeCloseTo(before, 0);
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await page
    .getByRole("link", { name: `Read ${other.title}`, exact: true })
    .click();
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(0);
});

test("article links and highlighted previews fit a narrow mobile viewport", async ({
  page,
  context,
}) => {
  await mockReader(context);
  await page.setViewportSize({ width: 320, height: 760 });
  await page.goto("/reader/?q=needle+C%2B%2B");
  await expect(
    page.getByRole("link", { name: `Read ${article.title}`, exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page
    .getByRole("link", { name: `Read ${article.title}`, exact: true })
    .click();
  await expect(
    page.getByRole("link", {
      name: "Open full-size image: Architecture diagram",
      exact: true,
    }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});
