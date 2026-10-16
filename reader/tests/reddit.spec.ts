import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post, Source } from "../src/lib/types";

const redditPost: Post = {
  id: "reddit-one",
  domain: "reddit.com",
  url: "https://www.reddit.com/r/LocalLLaMA/comments/reddit-one/local_models/",
  author: "model_builder",
  title: "Running local models on a small computer",
  excerpt: "A practical comparison of local inference setups.",
  html: "<p>A practical comparison of local inference setups.</p>",
  created_at: "2026-10-15T10:00:00Z",
  source: "reddit",
  thumbnail: null,
  media: [],
  reddit: {
    community: "LocalLLaMA",
    score: 128,
    comments: 42,
    flair: "Discussion",
  },
};
const blogPost: Post = {
  ...redditPost,
  id: "blog-one",
  domain: "example.com",
  title: "A collected blog article",
  source: "rss",
  reddit: undefined,
};
const otherPost: Post = {
  ...redditPost,
  id: "reddit-two",
  title: "A community research discussion",
  reddit: { community: "accelerate", score: null, comments: null, flair: null },
};

async function mockReddit(page: Page, configured = true) {
  const requests: URL[] = [];
  const posts = [blogPost, redditPost, otherPost];
  const sources: Source[] = [
    { domain: "example.com", name: "Example", count: 1, tags: [] },
    ...(configured
      ? [
          {
            domain: "reddit.com",
            name: "Reddit",
            kind: "reddit" as const,
            count: 2,
            tags: ["community"],
            communities: [
              { name: "LocalLLaMA", count: 1 },
              { name: "accelerate", count: 1 },
              { name: "EmptyCommunity", count: 0 },
            ],
          },
        ]
      : []),
  ];
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    requests.push(url);
    if (url.pathname.endsWith("/sources")) {
      await route.fulfill({ json: { sources, total_posts: posts.length } });
      return;
    }
    if (url.pathname.endsWith("/post")) {
      await route.fulfill({
        json: posts.find((post) => post.id === url.searchParams.get("id")),
      });
      return;
    }
    const params = url.searchParams;
    const selected = posts.filter(
      (post) =>
        (params.get("kind") === "reddit"
          ? post.source === "reddit"
          : post.source !== "reddit") &&
        (!params.get("community") ||
          post.reddit?.community === params.get("community")) &&
        (!params.get("author") || post.author === params.get("author")) &&
        (!params.get("q") ||
          post.title
            .toLowerCase()
            .includes((params.get("q") ?? "").toLowerCase())),
    );
    await route.fulfill({
      json: { posts: selected, next_cursor: null, limited: false },
    });
  });
  return requests;
}

async function noOverflow(page: Page) {
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
}

test("keeps Reddit separate and exposes configured communities with accessible disclosure", async ({
  page,
}, testInfo) => {
  const requests = await mockReddit(page);
  await page.goto("/reader/");
  await expect(
    page.getByRole("heading", { name: blogPost.title }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: redditPost.title }),
  ).toHaveCount(0);
  expect(
    requests
      .find((url) => url.pathname.endsWith("/posts"))
      ?.searchParams.get("kind"),
  ).toBe("stories");
  const nav = page.getByRole("navigation", { name: "Main navigation" });
  await expect(
    nav.getByRole("button", { name: "Reddit", exact: true }),
  ).toBeVisible();
  await nav
    .getByRole("button", { name: "Collapse Reddit communities" })
    .click();
  await expect(nav.getByRole("button", { name: /r\/LocalLLaMA/ })).toBeHidden();
  await nav.getByRole("button", { name: "Expand Reddit communities" }).click();
  await nav.getByRole("button", { name: "Reddit", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Reddit", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: redditPost.title }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: otherPost.title }),
  ).toBeVisible();
  await expect(page.getByRole("heading", { name: blogPost.title })).toHaveCount(
    0,
  );
  await expect(
    page.locator(".reddit-score").filter({ hasText: "128" }),
  ).toBeVisible();
  await expect(page.getByText("42 comments", { exact: true })).toBeVisible();
  await expect(page.getByText("Discussion", { exact: true })).toBeVisible();
  await noOverflow(page);
  await page.screenshot({
    path: testInfo.outputPath("reddit-desktop.png"),
    fullPage: true,
  });
  const results = await new AxeBuilder({ page }).analyze();
  expect(
    results.violations.filter((item) =>
      ["serious", "critical"].includes(item.impact ?? ""),
    ),
  ).toEqual([]);
  await nav.getByRole("button", { name: /r\/EmptyCommunity/ }).click();
  await expect(
    page.getByRole("heading", { name: "No collected posts found" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Show all Reddit posts" }).click();
  await expect(
    page.getByRole("heading", { name: redditPost.title }),
  ).toBeVisible();
});

test("hides Reddit navigation when it is not configured", async ({ page }) => {
  await mockReddit(page, false);
  await page.goto("/reader/");
  await expect(
    page.getByRole("heading", { name: blogPost.title }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Reddit", exact: true }),
  ).toHaveCount(0);
});

test("retains community, search, author and article navigation in Reddit", async ({
  page,
}) => {
  const requests = await mockReddit(page);
  await page.goto("/reader/?view=reddit&community=LocalLLaMA");
  await expect(
    page.getByRole("heading", { name: "r/LocalLLaMA", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: otherPost.title }),
  ).toHaveCount(0);
  await page
    .getByRole("textbox", { name: "Search r/LocalLLaMA" })
    .fill("local models");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await page
    .getByRole("link", { name: "Stories by model_builder", exact: true })
    .click();
  const params = new URL(page.url()).searchParams;
  expect(params.get("view")).toBe("reddit");
  expect(params.get("community")).toBe("LocalLLaMA");
  expect(params.get("q")).toBe("local models");
  expect(params.get("author_match")).toBe("exact");
  await expect
    .poll(() =>
      requests.some(
        (url) =>
          url.searchParams.get("kind") === "reddit" &&
          url.searchParams.get("community") === "LocalLLaMA" &&
          url.searchParams.get("author") === "model_builder",
      ),
    )
    .toBe(true);
  const story = page.getByRole("link", {
    name: `Read ${redditPost.title}`,
    exact: true,
  });
  await expect(story).toHaveAttribute(
    "href",
    /view=reddit.*community=LocalLLaMA/,
  );
  const timelineUrl = page.url();
  await story.click();
  const article = page.getByRole("main", { name: "Article reader" });
  await expect(
    article.getByRole("heading", { name: redditPost.title }),
  ).toBeVisible();
  await expect(article.getByText("42 comments", { exact: true })).toBeVisible();
  await page.reload();
  await article
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page).toHaveURL(timelineUrl);
  await expect(story).toBeVisible();
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: /r\/LocalLLaMA/ })
    .click();
  expect(new URL(page.url()).searchParams.get("view")).toBe("reddit");
  expect(new URL(page.url()).searchParams.get("community")).toBe("LocalLLaMA");
});

test("supports Reddit communities on mobile without leaving the navigation overlay open", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockReddit(page);
  await page.goto("/reader/");
  await page
    .getByRole("button", { name: "Open navigation", exact: true })
    .click();
  const dialog = page.getByRole("dialog", { name: "Navigation", exact: true });
  await page.screenshot({
    path: testInfo.outputPath("reddit-mobile-navigation.png"),
  });
  await dialog.getByRole("button", { name: /r\/LocalLLaMA/ }).click();
  await expect(dialog).toHaveCount(0);
  await expect(
    page.getByRole("heading", { name: redditPost.title }),
  ).toBeVisible();
  await noOverflow(page);
  await page.screenshot({
    path: testInfo.outputPath("reddit-mobile.png"),
    fullPage: true,
  });
  await page
    .getByRole("link", { name: `Read ${redditPost.title}`, exact: true })
    .click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await noOverflow(page);
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "r/LocalLLaMA", exact: true }),
  ).toBeVisible();
  await noOverflow(page);
});
