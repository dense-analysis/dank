import { expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const url = "https://publisher.test/research";
const publisherText =
  "The saved publisher explains the research and its results.";
const reddit: Post = {
  id: "t3_link",
  domain: "reddit.com",
  source: "reddit",
  url: "https://www.reddit.com/r/science/comments/link/article/",
  title: "A community link to new research",
  author: "submitter",
  excerpt: url,
  html: `<p><img src="/preview.svg" alt=""></p><p><a href="${url}">${url}</a></p>`,
  thumbnail: "/preview.svg",
  media: [],
  created_at: "2026-10-19T09:00:00Z",
  reddit: { community: "science", score: 42, comments: 7, flair: null },
  linked_post: {
    domain: "publisher.test",
    id: "research",
    source: "rss",
    url,
    title: "Publisher title",
    excerpt: publisherText,
  },
};

async function fixtures(page: Page, post = reddit) {
  await page.route("**/preview.svg", (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="120" height="80"><rect width="120" height="80" fill="#57865b"/></svg>',
    }),
  );
  await page.route("**/api/reader/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.includes("/youtube/")) {
      await route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 0 },
      });
    } else if (path.endsWith("/sources")) {
      await route.fulfill({
        json: {
          sources: [
            {
              domain: "reddit.com",
              name: "Reddit",
              kind: "reddit",
              count: 1,
              tags: [],
              communities: [{ name: "science", count: 1 }],
            },
          ],
          total_posts: 1,
        },
      });
    } else if (path.endsWith("/post")) {
      await route.fulfill({ json: post });
    } else {
      await route.fulfill({
        json: { posts: [post], next_cursor: null, limited: false },
      });
    }
  });
}

for (const width of [390, 1280]) {
  for (const view of ["", "view=reddit", "view=reddit&community=science"]) {
    test(`publisher excerpt and Reddit discussion navigation work in ${view || "All"} at ${width}px`, async ({
      page,
    }, testInfo) => {
      await page.setViewportSize({ width, height: 900 });
      await fixtures(page);
      const requests: string[] = [];
      page.on("request", (request) => {
        if (new URL(request.url()).pathname === "/api/reader/post")
          requests.push(request.url());
      });
      await page.goto(`/reader/?${view}`);
      const card = page.locator(".reddit-card");
      await expect(card.locator(".post-open p")).toHaveText(publisherText);
      await expect(card.locator(".thumbnail img")).toBeVisible();
      await expect(
        card.getByRole("link", { name: `Read ${reddit.title}`, exact: true }),
      ).toHaveAttribute("href", /article_source=reddit.com/);
      expect(requests).toEqual([]);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
      await page.screenshot({
        path: testInfo.outputPath(`reddit-excerpt-${width}.png`),
        fullPage: true,
      });
      await card
        .getByRole("link", { name: `Read ${reddit.title}`, exact: true })
        .click();
      await expect(page).toHaveURL(/article_source=reddit.com/);
      await expect(
        page.getByRole("main", { name: "Article reader" }),
      ).toBeVisible();
    });
  }
}

test("Reddit own text takes priority and an unavailable publisher keeps the original link", async ({
  page,
}) => {
  await fixtures(page, {
    ...reddit,
    html: "<p>My own explanation.</p>",
    excerpt: "My own explanation.",
  });
  await page.goto("/reader/?view=reddit");
  await expect(page.locator(".reddit-card .post-open p")).toHaveText(
    "My own explanation.",
  );
  await page.unroute("**/api/reader/**");
  await fixtures(page, { ...reddit, linked_post: undefined });
  await page.reload();
  await expect(page.locator(".reddit-card .post-open p")).toHaveText(url);
});

test("publisher excerpts keep shared search highlighting", async ({ page }) => {
  await fixtures(page);
  await page.goto("/reader/?view=reddit&q=research");
  await expect(page.locator(".reddit-card .post-open p mark")).toHaveText(
    "research",
  );
});

test("empty RSS aggregators show the saved excerpt in Blogs", async ({
  page,
}) => {
  await fixtures(page, {
    ...reddit,
    source: "rss",
    domain: "lobste.rs",
    url,
    html: "",
    excerpt: "",
    reddit: undefined,
    thumbnail: null,
  });
  await page.goto("/reader/?view=stories");
  await expect(page.locator(".post-card .post-open p")).toHaveText(
    publisherText,
  );
});

test("link-only X previews show saved publisher text without a duplicate raw URL", async ({
  page,
}) => {
  await fixtures(page, {
    ...reddit,
    id: "123456789",
    source: "x",
    domain: "x.com",
    url: "https://x.com/writer/status/123456789",
    reddit: undefined,
    thumbnail: null,
    html: `<p><a href="${url}">${url}</a></p>`,
    x: {
      handle: "writer",
      name: "Writer",
      icon_url: null,
      links: [
        {
          url,
          title: "Publisher title",
          description: publisherText,
          image_url: null,
        },
      ],
    },
  });
  await page.goto("/reader/?view=x");
  await expect(page.locator(".x-link-description")).toHaveText(publisherText);
  await expect(
    page.locator(".x-post-preview").getByText(url, { exact: true }),
  ).toHaveCount(0);
});
