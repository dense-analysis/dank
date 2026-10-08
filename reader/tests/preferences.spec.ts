import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";

const post = {
  id: "reading-font",
  domain: "example.com",
  title: "A story worth your time",
  url: "https://example.com/story",
  author: "Ada Writer",
  excerpt: "A little room to breathe.",
  html: "<p>Good stories deserve a little room to breathe.</p>",
  created_at: "2026-10-08T10:00:00Z",
  source: "rss",
  thumbnail: null,
  media: [],
};

async function mockReader(page: Page) {
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
}

test("reader navigation works without secure-context-only browser APIs", async ({
  page,
}) => {
  await mockReader(page);
  await page.addInitScript(() => {
    Object.defineProperty(crypto, "randomUUID", { value: undefined });
  });
  await page.goto("/reader/");
  await page
    .getByRole("link", { name: `Read ${post.title}`, exact: true })
    .click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await page.getByRole("button", { name: "Settings", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Settings", exact: true }),
  ).toBeVisible();
});

test("persists reading preferences across settings, feed, article and reload", async ({
  page,
}) => {
  await mockReader(page);
  await page.goto("/reader/");
  await page.getByRole("button", { name: "Settings", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Settings", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page.getByRole("radio", { name: /Newsreader/ }).check();
  const size = page.getByRole("slider", { name: "Article text size" });
  await size.fill("23");
  const preview = page.locator(".preview-body");
  await expect(preview).toHaveCSS("font-family", /Newsreader/);
  await expect(preview).toHaveCSS("font-size", "23px");
  await page.reload();
  await expect(page.getByRole("radio", { name: /Newsreader/ })).toBeChecked();
  await expect(size).toHaveValue("23");
  await page.getByRole("button", { name: "All stories", exact: true }).click();
  await expect(page.locator(".post-open h2")).toHaveCSS(
    "font-family",
    /Newsreader/,
  );
  await page
    .getByRole("link", { name: `Read ${post.title}`, exact: true })
    .click();
  await expect(page.locator(".article-body")).toHaveCSS(
    "font-family",
    /Newsreader/,
  );
  await expect(page.locator(".article-body")).toHaveCSS("font-size", "23px");
  await page.getByRole("button", { name: "Larger article text" }).click();
  await expect(page.locator(".article-body")).toHaveCSS("font-size", "24px");
  await page.reload();
  await expect(page.locator(".article-body")).toHaveCSS("font-size", "24px");
  await page.getByRole("button", { name: "Settings", exact: true }).click();
  await expect(size).toHaveValue("24");
  await page.getByRole("radio", { name: /Georgia/ }).check();
  await expect(preview).toHaveCSS("font-family", /Georgia/);
  await page.getByRole("button", { name: "Reset to defaults" }).click();
  await expect(page.getByRole("radio", { name: /DM Sans/ })).toBeChecked();
  await expect(size).toHaveValue("19");
});

test("settings work on mobile and have no serious accessibility violations", async ({
  page,
}) => {
  await mockReader(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/reader/");
  await page.getByRole("button", { name: "Open navigation" }).click();
  await page.getByRole("button", { name: "Settings", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Settings", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page.getByRole("radio", { name: /Newsreader/ }).check();
  await page.getByRole("slider", { name: "Article text size" }).fill("26");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  const results = await new AxeBuilder({ page }).analyze();
  expect(
    results.violations.filter((item) =>
      ["serious", "critical"].includes(item.impact ?? ""),
    ),
  ).toEqual([]);
});

test("unavailable preference storage keeps settings usable and explains persistence failure", async ({
  page,
}) => {
  await mockReader(page);
  await page.addInitScript(() => {
    Storage.prototype.setItem = () => {
      throw new Error("Storage is full");
    };
  });
  await page.goto("/reader/?view=settings");
  await page.getByRole("radio", { name: /Newsreader/ }).check();
  await expect(page.locator(".preview-body")).toHaveCSS(
    "font-family",
    /Newsreader/,
  );
  await expect(page.getByRole("alert")).toContainText(
    "Browser storage is unavailable or full",
  );
});
