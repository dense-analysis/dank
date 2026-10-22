import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post, Source } from "../src/lib/types";

const github: Post = {
  id: "github-release",
  domain: "github.blog",
  url: "https://github.blog/release",
  title: "GitHub release notes",
  author: "GitHub Engineering",
  excerpt: "A collected release from a configured blog feed.",
  html: "<p>A collected release from a configured blog feed.</p>",
  created_at: "2026-10-22T10:00:00Z",
  source: "rss",
  thumbnail: null,
  media: [],
};
const lobster: Post = {
  ...github,
  id: "lobster-release",
  domain: "lobste.rs",
  url: "https://lobste.rs/s/release",
  title: "Lobsters release discussion",
};
const external: Post = {
  ...github,
  id: "external-release",
  domain: "publisher.example",
  url: "https://publisher.example/release",
  title: "Publisher release linked from Reddit",
};
const hackerNews: Post = {
  ...github,
  id: "hn-release",
  domain: "news.ycombinator.com",
  url: "https://news.ycombinator.com/item?id=hn-release",
  title: "Hacker News release discussion",
  source: "hacker_news",
  hacker_news: {
    submitted_at: github.created_at,
    day: "2026-10-22",
    rank: 1,
    points: 42,
    comments: 7,
    external_url: null,
    discussion_url: "https://news.ycombinator.com/item?id=hn-release",
  },
};
const posts = [github, lobster, external, hackerNews];
const sources: (Source & { icon_url?: string | null })[] = [
  {
    domain: github.domain,
    name: "github.blog",
    kind: "rss",
    count: 1,
    tags: [],
    configured: true,
    icon_url: "/api/reader/source-icon?domain=github.blog",
  },
  {
    domain: lobster.domain,
    name: "lobste.rs",
    kind: "rss",
    count: 1,
    tags: [],
    configured: true,
    icon_url: "/assets/broken-blog.svg",
  },
  {
    domain: "empty.example",
    name: "Empty Blog",
    kind: "rss",
    count: 0,
    tags: [],
    configured: true,
    icon_url: null,
  },
  {
    domain: external.domain,
    name: "Discovered Publisher",
    kind: "rss",
    count: 1,
    tags: [],
    configured: false,
  },
  {
    domain: hackerNews.domain,
    name: "Hacker News",
    kind: "hacker_news",
    count: 1,
    tags: [],
    configured: true,
  },
];

async function fixture(page: Page) {
  const requests: URL[] = [];
  await page.route("**/assets/broken-blog.svg", (route) =>
    route.fulfill({ status: 404 }),
  );
  await page.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    requests.push(url);

    if (url.pathname.endsWith("/source-icon")) {
      return route.fulfill({
        contentType: "image/svg+xml",
        body: '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"><rect width="32" height="32" fill="#537d70"/></svg>',
      });
    }

    if (url.pathname.endsWith("/youtube/sources")) {
      return route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 0 },
      });
    }

    if (url.pathname.endsWith("/sources")) {
      return route.fulfill({
        json: { sources, total_posts: posts.length, blog_external_count: 1 },
      });
    }

    if (url.pathname.endsWith("/post")) {
      return route.fulfill({
        json: posts.find(
          (post) =>
            post.id === params.get("id") &&
            post.domain === params.get("domain"),
        ),
      });
    }

    const selected = posts.filter((post) => {
      const domains = params.getAll("domain");
      const feed = params.get("blog_feed");
      const source = sources.find((item) => item.domain === post.domain);

      return (
        (params.get("kind") !== "hacker_news" ||
          post.source === "hacker_news") &&
        (feed !== "external" || post.id === external.id) &&
        (feed !== "sources" || source?.configured) &&
        (!domains.length || domains.includes(post.domain)) &&
        (!params.get("q") ||
          post.title
            .toLowerCase()
            .includes(params.get("q")?.toLowerCase() ?? ""))
      );
    });

    return route.fulfill({
      json: { posts: selected, next_cursor: null, limited: false },
    });
  });

  return requests;
}

function navigation(page: Page) {
  return page.getByRole("navigation", { name: "Main navigation" });
}

function sourceEntry(page: Page, name: string) {
  return navigation(page).getByRole("button", {
    name: new RegExp(
      `^${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}, \\d+ collected posts$`,
    ),
  });
}

function story(page: Page, post: Post) {
  return page.getByRole("link", { name: `Read ${post.title}`, exact: true });
}

for (const width of [320, 390, 1280]) {
  test(`Blogs and Source Feeds keep independent expansion choices at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 800 });
    await fixture(page);
    await page.goto("/reader/");

    async function openNavigation() {
      if (width <= 760) {
        await page.getByRole("button", { name: "Open navigation" }).click();
      }
    }

    await openNavigation();
    const nav = navigation(page);

    if (width <= 760) {
      await expect(page.locator("#blog-feeds")).toBeHidden();
      await nav.getByRole("button", { name: "Expand Blog feeds" }).click();
      await expect(page.locator("#blog-sources")).toBeHidden();
      await nav
        .getByRole("button", { name: "Expand Blog Source Feeds" })
        .click();
    }

    await expect(sourceEntry(page, "github.blog")).toBeVisible();
    await nav
      .getByRole("button", { name: "Collapse Blog Source Feeds" })
      .click();
    await nav.getByRole("button", { name: "Collapse Blog feeds" }).click();
    await page.reload();
    await openNavigation();
    await expect(page.locator("#blog-feeds")).toBeHidden();
    await nav.getByRole("button", { name: "Expand Blog feeds" }).click();
    await expect(page.locator("#blog-sources")).toBeHidden();
    await nav.getByRole("button", { name: "Expand Blog Source Feeds" }).click();
    await sourceEntry(page, "github.blog").click();
    await expect(page).toHaveURL(/view=stories.*domain=github.blog/);
    await expect(story(page, github)).toBeVisible();

    if (width <= 760) {
      await expect(
        page.getByRole("dialog", { name: "Navigation" }),
      ).toHaveCount(0);
      await expect(page.locator("body")).not.toHaveCSS("overflow", "hidden");
    }

    await page.reload();
    await openNavigation();
    await expect(page.locator("#blog-feeds")).toBeVisible();
    await expect(page.locator("#blog-sources")).toBeVisible();
    await expect(sourceEntry(page, "github.blog")).toHaveAttribute(
      "aria-current",
      "page",
    );
    await expect(sourceEntry(page, "github.blog")).toBeInViewport();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  });
}

test("Blogs separates configured source feeds from externally discovered articles", async ({
  page,
}) => {
  const requests = await fixture(page);
  await page.goto("/reader/?view=stories");
  const nav = navigation(page);
  await expect(story(page, github)).toBeVisible();
  await expect(story(page, external)).toBeVisible();
  await expect(story(page, hackerNews)).toBeVisible();
  await expect(sourceEntry(page, "Empty Blog")).toBeVisible();
  await expect(sourceEntry(page, "Discovered Publisher")).toHaveCount(0);
  const linked = nav.getByRole("button", { name: /^From external/ });
  await expect(linked).toContainText("1");
  await linked.click();
  await expect(page).toHaveURL(/view=stories.*blog_feed=external/);
  await expect(linked).toHaveAttribute("aria-current", "page");
  await expect(story(page, external)).toBeVisible();
  await expect(story(page, github)).toHaveCount(0);
  await expect(story(page, hackerNews)).toHaveCount(0);
  await nav.getByRole("button", { name: "Source Feeds", exact: true }).click();
  await expect(page).toHaveURL(/view=stories.*blog_feed=sources/);
  await expect(story(page, external)).toHaveCount(0);
  await expect(story(page, github)).toBeVisible();
  await expect(story(page, lobster)).toBeVisible();
  await expect(story(page, hackerNews)).toBeVisible();
  await sourceEntry(page, "github.blog").click();
  await expect(story(page, github)).toBeVisible();
  await expect(story(page, lobster)).toHaveCount(0);
  await expect(story(page, hackerNews)).toHaveCount(0);
  await expect
    .poll(() =>
      requests.some(
        (url) =>
          url.pathname.endsWith("/posts") &&
          url.searchParams.get("domain") === "github.blog",
      ),
    )
    .toBe(true);
});

for (const selection of ["domain=github.blog", "blog_feed=external"]) {
  test(`Blog selection ${selection} survives search, article return and reload`, async ({
    page,
  }) => {
    await fixture(page);
    await page.goto(`/reader/?view=stories&${selection}&after=2026-10-01`);
    const post = selection.startsWith("domain") ? github : external;
    await page.getByRole("textbox", { name: /^Search / }).fill("release");
    await page.getByRole("button", { name: "Search", exact: true }).click();
    await page
      .getByRole("combobox", { name: "Order stories" })
      .selectOption("oldest");
    const before = page.url();
    const link = story(page, post);
    await link.click();
    await expect(
      page.getByRole("main", { name: "Article reader" }),
    ).toBeVisible();
    const [key, value] = selection.split("=");
    expect(new URL(page.url()).searchParams.get(key)).toBe(value);
    expect(new URL(page.url()).searchParams.get("q")).toBe("release");
    expect(new URL(page.url()).searchParams.get("after")).toBe("2026-10-01");
    await page
      .getByRole("button", { name: "Back to stories", exact: true })
      .click();
    await expect(page).toHaveURL(before);
    await expect(link).toBeFocused();
    await page.reload();
    await expect(link).toBeVisible();
    await expect(page.getByRole("textbox", { name: /^Search / })).toHaveValue(
      "release",
    );
    expect(new URL(page.url()).searchParams.get(key)).toBe(value);
  });
}

test("Hacker News appears under Blog Source Feeds and keeps its daily ranked view", async ({
  page,
}) => {
  await fixture(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(
    "/reader/?view=hacker_news&domain=news.ycombinator.com&hn_filter=top20",
  );
  await expect(
    page
      .getByRole("group", { name: "Hacker News daily selection" })
      .getByRole("button", { name: "Top 20", exact: true }),
  ).toHaveAttribute("aria-pressed", "true");
  await page.getByRole("button", { name: "Open navigation" }).click();
  await expect(page.locator("#blog-feeds")).toBeVisible();
  await expect(page.locator("#blog-sources")).toBeVisible();
  const entry = sourceEntry(page, "Hacker News");
  await expect(entry).toHaveAttribute("aria-current", "page");
  await expect(
    page
      .locator("#blog-sources")
      .getByRole("button", { name: /^Hacker News,/ }),
  ).toHaveCount(1);
  await entry.click();
  await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(0);
  expect(new URL(page.url()).searchParams.get("view")).toBe("hacker_news");
  expect(new URL(page.url()).searchParams.get("domain")).toBe(
    "news.ycombinator.com",
  );
  await expect(story(page, hackerNews)).toBeVisible();
  await expect(
    page.getByRole("group", { name: "Hacker News daily selection" }),
  ).toBeVisible();
});

test("Blog source icons use captured assets and readable fallbacks", async ({
  page,
}) => {
  await fixture(page);
  await page.goto("/reader/?view=stories");
  const icon = sourceEntry(page, "github.blog").locator("img");
  await expect(icon).toHaveJSProperty("naturalWidth", 32);
  await expect(icon).toHaveAttribute("alt", "");

  for (const [name, initial] of [
    ["lobste.rs", "L"],
    ["Empty Blog", "E"],
  ]) {
    const entry = sourceEntry(page, name);
    await expect(entry.locator("img")).toHaveCount(0);
    await expect(entry.locator(".identity-avatar")).toHaveText(initial);
  }

  expect(
    (await new AxeBuilder({ page }).analyze()).violations.filter((item) =>
      ["serious", "critical"].includes(item.impact ?? ""),
    ),
  ).toEqual([]);
});
