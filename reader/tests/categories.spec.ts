import { expect, test } from "@playwright/test";
import { feedSource, post } from "../src/lib/api.fixture";
import { apiPath, emptyResponses, fulfillEmpty } from "./apiMock";

for (const width of [320, 390, 1280]) {
  test(`category selection and labels at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    const story = post({
      id: "1",
      domain: "example.com",
      url: "https://example.com/1",
      author: "Writer",
      title: "Software story",
      excerpt: "A saved story.",
      html: "<p>A saved story.</p>",
      created_at: "2026-11-04T10:00:00Z",
      source: "rss",
      thumbnail: null,
      media: [],
      categories: [{ name: "Software", confidence: 0.9, similarity: 0.95 }],
    });
    await page.route("**/api/reader/**", async (route) => {
      const url = new URL(route.request().url());
      switch (apiPath(route)) {
        case "/api/reader/categories":
          return route.fulfill({
            json: { categories: ["Software", "Science"] },
          });
        case "/api/reader/sources":
          return route.fulfill({
            json: {
              ...emptyResponses["/api/reader/sources"],
              sources: [
                feedSource({
                  domain: "example.com",
                  name: "Example",
                  count: 1,
                }),
              ],
              total_posts: 1,
            },
          });
        case "/api/reader/post":
          return route.fulfill({ json: story });
        case "/api/reader/posts": {
          const categories = url.searchParams.getAll("category");
          return route.fulfill({
            json: {
              posts:
                categories.includes("Science") &&
                !categories.includes("Software")
                  ? []
                  : [story],
              next_cursor: null,
              limited: false,
            },
          });
        }
        default:
          return fulfillEmpty(route);
      }
    });
    await page.goto("/");
    const filters = page.getByRole("group", { name: "Filter by category" });
    await expect(filters).toBeVisible();
    await expect(
      page.getByRole("list", { name: "Post categories" }),
    ).toContainText("Software");
    await filters.getByRole("button", { name: "Science", exact: true }).click();
    await expect(page).toHaveURL(/category=Science/);
    await expect(
      page.getByRole("heading", { name: "Software story" }),
    ).toHaveCount(0);
    await filters
      .getByRole("button", { name: "Software", exact: true })
      .click();
    await expect(page).toHaveURL(/category=Software/);
    expect(new URL(page.url()).searchParams.getAll("category")).toEqual([
      "Software",
    ]);
    await expect(
      page.getByRole("heading", { name: "Software story", exact: true }),
    ).toBeVisible();
    await page.locator(".post-card [data-card-link]").first().click();
    await expect(
      page.getByRole("main", { name: "Article reader" }),
    ).toBeVisible();
    await expect(
      page.getByRole("list", { name: "Post categories" }),
    ).toContainText("Software");
    await page
      .getByRole("button", { name: "Back to stories", exact: true })
      .click();
    await expect(page).toHaveURL(/category=Software/);
    await page.reload();
    await expect(
      filters.getByRole("button", { name: "Software", exact: true }),
    ).toHaveAttribute("aria-pressed", "true");
    await expect(page.locator("body")).toHaveJSProperty("scrollWidth", width);
    await filters
      .getByRole("button", { name: "Software", exact: true })
      .click();
    expect(new URL(page.url()).searchParams.getAll("category")).toEqual([
      "Software",
    ]);
    await page.goto("/?category=Science&category=Software");
    await expect(
      filters.getByRole("button", { name: "Science", exact: true }),
    ).toHaveAttribute("aria-pressed", "true");
    await expect(
      filters.getByRole("button", { name: "Software", exact: true }),
    ).toHaveAttribute("aria-pressed", "true");
    await expect(
      page.getByRole("heading", { name: "Software story", exact: true }),
    ).toBeVisible();
    await page.reload();
    expect(new URL(page.url()).searchParams.getAll("category")).toEqual([
      "Science",
      "Software",
    ]);
    await filters.getByRole("button", { name: "Science", exact: true }).click();
    expect(new URL(page.url()).searchParams.getAll("category")).toEqual([
      "Science",
    ]);
    await expect(
      filters.getByRole("button", { name: "Software", exact: true }),
    ).toHaveAttribute("aria-pressed", "false");

    await filters
      .getByRole("button", { name: "All categories", exact: true })
      .click();
    await expect(page).not.toHaveURL(/category=/);
  });
}
