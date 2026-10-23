import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";

async function fixture(page: Page) {
  await page.route("**/api/reader/**", async (route) => {
    const path = new URL(route.request().url()).pathname;

    if (path.endsWith("/source-icon")) {
      await route.fulfill({ status: 404, body: "" });
      return;
    }

    await route.fulfill({
      json: path.endsWith("/refresh")
        ? { jobs: [] }
        : path.endsWith("/youtube/sources")
          ? { feeds: [], channels: [], total: 0, linked_count: 0 }
          : path.endsWith("/youtube")
            ? { items: [], total: 0, next_cursor: null }
            : path.endsWith("/sources")
              ? {
                  sources: [
                    {
                      domain: "example.com",
                      name: "example.com",
                      count: 1,
                      tags: ["Tech"],
                    },
                    {
                      domain: "reddit.com",
                      name: "Reddit",
                      kind: "reddit",
                      count: 2,
                      tags: [],
                    },
                    {
                      domain: "x.com",
                      name: "X",
                      kind: "x",
                      count: 0,
                      tags: [],
                    },
                  ],
                  total_posts: 3,
                }
              : { posts: [], next_cursor: null, limited: false },
    });
  });
}

for (const width of [320, 1280]) {
  for (const view of ["all", "youtube", "sources"]) {
    test(`${view} search clears and restores focus consistently at ${width}px`, async ({
      page,
    }) => {
      await page.setViewportSize({ width, height: 800 });
      await fixture(page);
      const scoped =
        view === "youtube"
          ? "&feed=linked&format=video"
          : view === "all"
            ? "&domain=example.com"
            : "";
      await page.goto(`/reader/?view=${view}${scoped}`);
      const search = page.getByRole("searchbox");
      await expect(search).toBeVisible();
      await page.keyboard.press("/");
      await expect(search).toBeFocused();
      await search.fill("unmatched phrase");

      if (view === "sources") {
        await expect(
          page.getByText("No sources match this search."),
        ).toBeVisible();
      } else {
        await search.press("Enter");
        await expect(page).toHaveURL(/q=unmatched\+phrase/);
      }

      await page
        .getByRole("button", { name: "Clear search", exact: true })
        .click();
      await expect(search).toHaveValue("");
      await expect(search).toBeFocused();
      await expect(page).not.toHaveURL(/[?&]q=/);

      if (view === "sources") {
        await expect(page.locator(".source-row")).toHaveCount(3);
      } else if (view === "youtube") {
        await expect(page).toHaveURL(/feed=linked/);
        await expect(page).toHaveURL(/format=video/);
        await search.fill("unsubmitted draft");
        await page
          .getByRole("button", { name: "Clear search", exact: true })
          .click();
        await expect(search).toHaveValue("");
      } else {
        await expect(page).toHaveURL(/domain=example.com/);
      }

      expect(
        await page.evaluate(() => document.documentElement.scrollWidth),
      ).toBe(width);
      const searchBox = await page.locator(".search-bar").boundingBox();
      expect(searchBox?.height).toBe(50);
      expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    });
  }
}

test("source rows share platform identity and use accurate count labels", async ({
  page,
}) => {
  await fixture(page);
  await page.goto("/reader/?view=sources");
  const blog = page.locator(".source-row").filter({ hasText: "example.com" });
  const reddit = page.locator(".source-row").filter({ hasText: "Reddit" });
  await expect(blog).toContainText("1 post");
  await expect(blog.locator("small")).toHaveCount(0);
  await expect(reddit).toContainText("2 posts");
  await expect(reddit.locator(".platform-icon")).toBeVisible();
  await expect(reddit.locator(".source-mark")).toHaveCSS("width", "22px");
  await page.getByRole("searchbox").fill(" TECH ");
  await expect(page.locator(".source-row")).toHaveCount(1);
  await blog.click();
  await expect(page).toHaveURL(/domain=example.com/);
});

test("X post selection keeps touch targets consistent with other mobile filters", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await fixture(page);
  await page.goto("/reader/?view=x");

  for (const name of ["Posts", "Posts & replies"]) {
    const button = page.getByRole("button", { name, exact: true });
    expect((await button.boundingBox())?.height).toBeGreaterThanOrEqual(44);
  }
});
