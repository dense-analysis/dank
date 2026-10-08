import { readFileSync } from "node:fs";
import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const html = readFileSync(
  new URL("../../tests/fixtures/reader-table.html", import.meta.url),
  "utf8",
);

async function openArticle(page: Page) {
  const post: Post = {
    id: "table-example",
    domain: "example.com",
    title: "Comparing benchmarks",
    url: "https://example.com/table-example",
    author: "Example author",
    excerpt: "A comparison of models.",
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
              { domain: post.domain, name: "Example", tags: [], count: 1 },
            ],
            total_posts: 1,
          }
        : path.endsWith("/post")
          ? post
          : { posts: [post], next_cursor: null, limited: false },
    });
  });
  await page.goto("/reader/?article=table-example&article_source=example.com");
  await expect(
    page.getByRole("table", { name: "Benchmark comparison" }),
  ).toBeVisible();
  await page.evaluate(() => document.fonts.ready);
}

test("merged cells keep values under the correct model headers", async ({
  page,
}) => {
  await openArticle(page);
  const table = page.getByRole("table", { name: "Benchmark comparison" });
  await expect(table).toHaveCSS("display", "table");
  await expect(table).toHaveCSS("border-collapse", "collapse");
  await expect(table.locator('th[rowspan="2"]')).toHaveCount(1);
  await expect(table.locator("tfoot td")).toHaveAttribute("colspan", "6");
  await expect(table.locator("colgroup").first()).toHaveAttribute("span", "2");
  for (const [header, values] of [
    ["Model Alpha", ["101", "11", "55"]],
    ["Model Delta", ["404", "44", "88"]],
  ] as const) {
    const headerX = await table
      .getByRole("columnheader", { name: header, exact: true })
      .evaluate((element) => element.getBoundingClientRect().left);
    for (const value of values) {
      const cellX = await table
        .getByRole("cell", { name: value, exact: true })
        .evaluate((element) => element.getBoundingClientRect().left);
      expect(Math.abs(cellX - headerX)).toBeLessThan(1);
    }
  }
  const noToolsX = await table
    .getByRole("rowheader", { name: "No tools", exact: true })
    .evaluate((element) => element.getBoundingClientRect().left);
  const withToolsX = await table
    .getByRole("rowheader", { name: "With tools", exact: true })
    .evaluate((element) => element.getBoundingClientRect().left);
  expect(Math.abs(noToolsX - withToolsX)).toBeLessThan(1);
  await expect(table.locator("[style], [onclick]")).toHaveCount(0);
  expect(
    await page.evaluate(() => Reflect.get(window, "tableAttack")),
  ).toBeUndefined();
  expect(
    await page.evaluate(() =>
      [...document.fonts].some(
        (font) =>
          font.family.includes("DM Sans") &&
          font.style === "italic" &&
          font.status === "loaded",
      ),
    ),
  ).toBe(true);
  const accessibility = await new AxeBuilder({ page }).analyze();
  expect(
    accessibility.violations
      .filter((item) => ["serious", "critical"].includes(item.impact ?? ""))
      .map((item) => ({
        id: item.id,
        targets: item.nodes.map((node) => node.target),
      })),
  ).toEqual([]);
});

test("wide tables scroll independently on mobile using the keyboard", async ({
  page,
}) => {
  await page.setViewportSize({ width: 320, height: 760 });
  await openArticle(page);
  const region = page.getByRole("region", { name: "Benchmark comparison" });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  expect(
    await region.evaluate(
      (element) => element.scrollWidth > element.clientWidth,
    ),
  ).toBe(true);
  await region.focus();
  await region.press("ArrowRight");
  await expect
    .poll(() => region.evaluate((element) => element.scrollLeft))
    .toBeGreaterThan(0);
  await expect(page.getByRole("table").locator('th[rowspan="2"]')).toHaveCount(
    1,
  );
});
