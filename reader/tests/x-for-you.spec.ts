import { expect, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const post: Post = {
  id: "123",
  domain: "x.com",
  url: "https://x.com/reporter/status/123",
  author: "Reporter",
  title: "A recommended post",
  excerpt: "From your homepage",
  html: "<p>A recommended post</p>",
  created_at: "2026-10-25T12:00:00Z",
  source: "x",
  thumbnail: null,
  media: [],
  x: { handle: "reporter", name: "Reporter", icon_url: null },
};

for (const width of [390, 1280]) {
  test(`For You navigation, saved post and refresh scope at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 850 });
    const scopes: unknown[] = [];
    const queries: URLSearchParams[] = [];
    await page.route("**/api/reader/**", async (route) => {
      const url = new URL(route.request().url());
      if (url.pathname.endsWith("/youtube/sources")) {
        await route.fulfill({
          json: { feeds: [], channels: [], total: 0, linked_count: 0 },
        });
      } else if (url.pathname.endsWith("/sources")) {
        await route.fulfill({
          json: {
            sources: [
              {
                domain: "x.com",
                name: "X",
                kind: "x",
                count: 2,
                tags: [],
                for_you_count: 1,
                accounts: [
                  {
                    handle: "for_you",
                    name: "An actual account",
                    count: 1,
                    icon_url: null,
                  },
                ],
              },
            ],
            total_posts: 2,
          },
        });
      } else if (url.pathname.endsWith("/refresh")) {
        if (route.request().method() === "POST") {
          const scope = route.request().postDataJSON().scope;
          scopes.push(scope);
          await route.fulfill({
            status: 202,
            json: {
              job: {
                id: "capture",
                scope,
                label: "X For You",
                status: "running",
                message: "Collecting",
                created_at: "2026-10-25T12:00:00Z",
                finished_at: null,
                posts_saved: 0,
              },
            },
          });
        } else await route.fulfill({ json: { jobs: [] } });
      } else if (url.pathname.endsWith("/post")) {
        await route.fulfill({ json: post });
      } else if (url.pathname.endsWith("/posts")) {
        queries.push(url.searchParams);
        await route.fulfill({
          json: { posts: [post], next_cursor: null, limited: false },
        });
      } else await route.fulfill({ json: {} });
    });
    await page.goto("/reader/?view=x&x_feed=for_you");
    await expect(
      page.getByRole("heading", { name: "For You", exact: true }),
    ).toBeVisible();
    await expect(page.getByPlaceholder("Search For You")).toBeVisible();
    await expect(
      page.getByRole("group", { name: "X post selection" }),
    ).toHaveCount(0);
    await expect(page.locator(".x-post-card")).toHaveCount(1);
    expect(queries.some((query) => query.get("x_feed") === "for_you")).toBe(
      true,
    );
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
    ).toBe(false);
    await page
      .getByRole("button", {
        name: "Refresh X For You recommendations",
        exact: true,
      })
      .click();
    await expect
      .poll(() => scopes)
      .toEqual([{ kind: "x", feed_id: "for_you" }]);
    await page.locator(".x-post-card a[data-card-link]").first().click();
    await expect(page).toHaveURL(/x_feed=for_you.*article=123/);
    await page.goBack();
    await expect(
      page.getByRole("heading", { name: "For You", exact: true }),
    ).toBeVisible();
    if (width < 761)
      await page.getByRole("button", { name: "Open navigation" }).click();
    await expect(
      page.getByRole("button", {
        name: "For You, 1 collected posts",
        exact: true,
      }),
    ).toHaveAttribute("aria-current", "page");
    await page
      .getByRole("button", {
        name: "An actual account (@for_you), 1 collected posts",
        exact: true,
      })
      .click();
    await expect(page).toHaveURL(/view=x&account=for_you/);
  });
}
