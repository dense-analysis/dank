import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const base: Post = {
  id: "one",
  domain: "example.com",
  author: "Ada Writer",
  title: "Architecture overview",
  url: "https://example.com/one",
  html: "<p>An architecture article.</p>",
  excerpt: "An architecture article.",
  created_at: "2026-10-08T10:00:00Z",
  source: "rss",
  thumbnail: null,
  media: [],
};
const posts: Post[] = [
  base,
  {
    ...base,
    id: "two",
    title: "Architecture yesterday",
    created_at: "2026-10-07T10:00:00Z",
  },
  {
    ...base,
    id: "junior",
    author: "Ada Writer Jr",
    title: "Architecture by another author",
  },
  { ...base, id: "unknown", author: "", title: "Architecture notes" },
];

async function mockReader(page: Page) {
  const requests: URL[] = [];
  await page.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());
    requests.push(url);
    if (url.pathname.endsWith("/sources"))
      return route.fulfill({
        json: {
          sources: [
            {
              domain: base.domain,
              name: "Example",
              tags: ["technology"],
              count: posts.length,
            },
          ],
          total_posts: posts.length,
        },
      });
    if (url.pathname.endsWith("/post"))
      return route.fulfill({
        json: posts.find((post) => post.id === url.searchParams.get("id")),
      });
    const author = url.searchParams.get("author")?.toLowerCase();
    const after = url.searchParams.get("after");
    const before = url.searchParams.get("before");
    const selected = posts.filter(
      (post) =>
        (!author ||
          (url.searchParams.get("author_match") === "exact"
            ? post.author.toLowerCase() === author
            : post.author.toLowerCase().includes(author))) &&
        (!after || post.created_at.slice(0, 10) >= after) &&
        (!before || post.created_at.slice(0, 10) <= before),
    );
    return route.fulfill({
      json: { posts: selected, next_cursor: null, limited: false },
    });
  });
  return requests;
}

const initial =
  "q=architecture&sort=oldest&domain=example.com&tag=technology&after=2026-10-01&before=2026-10-08";

test("clicking an author preserves the view and excludes similar names", async ({
  page,
}) => {
  const requests = await mockReader(page);
  await page.goto(`/reader/?${initial}`);
  const card = page.locator(".post-card").filter({
    has: page.getByRole("link", {
      name: "Read Architecture overview",
      exact: true,
    }),
  });
  const author = card.getByRole("link", {
    name: "Stories by Ada Writer",
    exact: true,
  });
  await expect(author).toHaveAttribute("href", /author_match=exact/);
  await author.click();
  const params = new URL(page.url()).searchParams;
  for (const [key, value] of new URLSearchParams(initial))
    expect(params.get(key)).toBe(value);
  expect(params.get("author")).toBe("Ada Writer");
  expect(params.get("author_match")).toBe("exact");
  await expect(
    page.getByRole("link", {
      name: "Read Architecture by another author",
      exact: true,
    }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Remove author filter" }),
  ).toContainText("Author: Ada Writer");
  await expect
    .poll(() =>
      requests.some((url) => url.searchParams.get("author_match") === "exact"),
    )
    .toBe(true);
  await page.reload();
  await expect(
    page.getByRole("button", { name: "Remove author filter" }),
  ).toContainText("Ada Writer");
  await page.getByRole("button", { name: "Remove author filter" }).click();
  expect(new URL(page.url()).searchParams.has("author_match")).toBe(false);
  expect(new URL(page.url()).searchParams.toString()).toBe(initial);
  await expect(
    page.getByRole("link", {
      name: "Read Architecture by another author",
      exact: true,
    }),
  ).toBeVisible();
});

test("author filtering works from the article and missing authors stay plain", async ({
  page,
}) => {
  await mockReader(page);
  await page.goto(`/reader/?${initial}`);
  const unknown = page.locator(".post-card").filter({
    has: page.getByRole("link", {
      name: "Read Architecture notes",
      exact: true,
    }),
  });
  await expect(unknown.locator(".author")).toHaveText(base.domain);
  await expect(unknown.locator(".author-link")).toHaveCount(0);
  await page
    .getByRole("link", { name: "Read Architecture overview", exact: true })
    .click();
  await page
    .getByRole("main", { name: "Article reader" })
    .getByRole("link", { name: "Stories by Ada Writer", exact: true })
    .click();
  await expect(page.getByRole("main", { name: "Article reader" })).toHaveCount(
    0,
  );
  const params = new URL(page.url()).searchParams;
  expect(params.get("q")).toBe("architecture");
  expect(params.get("author_match")).toBe("exact");
  await expect(
    page.getByRole("link", {
      name: "Read Architecture by another author",
      exact: true,
    }),
  ).toHaveCount(0);
  await page.getByRole("button", { name: /^Filters/ }).click();
  const authorInput = page.getByRole("textbox", {
    name: "Author",
    exact: true,
  });
  await authorInput.fill("");
  await authorInput.pressSequentially("Ada W");
  await expect(authorInput).toHaveValue("Ada W");
  expect(new URL(page.url()).searchParams.get("author")).toBe("Ada W");
  expect(new URL(page.url()).searchParams.has("author_match")).toBe(false);
  await expect(
    page.getByRole("link", {
      name: "Read Architecture by another author",
      exact: true,
    }),
  ).toBeVisible();
});

test("quick date ranges preserve other filters and can be removed individually", async ({
  page,
}) => {
  await mockReader(page);
  await page.clock.setFixedTime(new Date("2026-10-08T12:00:00Z"));
  await page.goto(`/reader/?${initial}&author=Ada+Writer&author_match=exact`);
  await page.getByRole("button", { name: /^Filters/ }).click();
  const ranges = page.getByRole("group", { name: "Date range UTC" });
  await ranges.getByRole("button", { name: "Today", exact: true }).click();
  expect(new URL(page.url()).searchParams.get("after")).toBe("2026-10-08");
  expect(new URL(page.url()).searchParams.get("before")).toBe("2026-10-08");
  await expect(
    page.getByRole("link", {
      name: "Read Architecture yesterday",
      exact: true,
    }),
  ).toHaveCount(0);
  await ranges
    .getByRole("button", { name: "Past 7 days", exact: true })
    .click();
  await expect(
    page.getByRole("link", {
      name: "Read Architecture yesterday",
      exact: true,
    }),
  ).toBeVisible();
  expect(new URL(page.url()).searchParams.get("after")).toBe("2026-10-02");
  await ranges
    .getByRole("button", { name: "Past 30 days", exact: true })
    .click();
  expect(new URL(page.url()).searchParams.get("after")).toBe("2026-09-09");
  await page.getByRole("button", { name: /^Filters/ }).click();
  await expect(
    page.getByRole("button", { name: "Remove start date filter" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Remove end date filter" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Remove start date filter" }).click();
  expect(new URL(page.url()).searchParams.has("after")).toBe(false);
  expect(new URL(page.url()).searchParams.get("before")).toBe("2026-10-08");
  await page.getByRole("button", { name: "Remove end date filter" }).click();
  const params = new URL(page.url()).searchParams;
  expect(params.has("before")).toBe(false);
  expect(params.get("author")).toBe("Ada Writer");
  expect(params.get("author_match")).toBe("exact");
  expect(params.get("q")).toBe("architecture");
  expect(params.get("sort")).toBe("oldest");
  expect(params.getAll("domain")).toEqual([base.domain]);
  expect(params.getAll("tag")).toEqual(["technology"]);
});

test("filter chips and shortcuts remain accessible on mobile", async ({
  page,
}) => {
  await mockReader(page);
  await page.setViewportSize({ width: 320, height: 760 });
  await page.goto(`/reader/?${initial}&author=Ada+Writer&author_match=exact`);
  await page.getByRole("button", { name: /^Filters/ }).click();
  await expect(
    page.getByRole("button", { name: "Past 7 days", exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  const results = await new AxeBuilder({ page }).analyze();
  expect(
    results.violations
      .filter((item) => ["serious", "critical"].includes(item.impact ?? ""))
      .map((item) => ({
        id: item.id,
        nodes: item.nodes.map((node) => node.target),
      })),
  ).toEqual([]);
});
