import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { CollectionInfo, Post } from "../src/lib/types";

const publisher = {
  id: "article",
  domain: "publisher.test",
  source: "linked_article",
  url: "https://publisher.test/article",
  title: "Original research",
  excerpt: "A saved explanation from the publisher.",
};
const firstOrigin = {
  domain: "news.ycombinator.com",
  id: "hn-first",
  source: "hacker_news",
  url: "https://news.ycombinator.com/item?id=1",
  label: "Hacker News",
  observed_at: "2026-10-19T08:00:00Z",
  kind: "reference" as const,
};
const info: CollectionInfo = {
  article_url: publisher.url,
  article: publisher,
  article_status: "collected",
  post_first_seen_at: "2026-10-19T09:00:00Z",
  link_first_seen_at: "2026-10-19T09:00:00Z",
  first_seen_at: firstOrigin.observed_at,
  first_seen_from: [firstOrigin],
  article_collected_at: "2026-10-19T08:01:00Z",
  collected_via: firstOrigin,
  seen_before_post: true,
  coverage: "saved_history",
};
const reddit: Post = {
  id: "t3_later",
  domain: "reddit.com",
  source: "reddit",
  url: "https://reddit.com/r/science/comments/later/",
  title: "A community link to research",
  author: "submitter",
  excerpt: publisher.url,
  html: `<p>${publisher.url}</p>`,
  created_at: "2026-10-19T07:00:00Z",
  thumbnail: null,
  media: [],
  reddit: { community: "science", score: 42, comments: 7, flair: null },
  linked_post: publisher,
  collection_info: info,
};
const article: Post = {
  ...publisher,
  author: "Researcher",
  created_at: "2026-10-18T06:00:00Z",
  html: `<p>${publisher.excerpt}</p>`,
  thumbnail: null,
  media: [],
  collection_info: info,
};

async function fixtures(page: Page, post = reddit) {
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.includes("/youtube/")) {
      await route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 0 },
      });
    } else if (url.pathname.endsWith("/sources")) {
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
            {
              domain: "publisher.test",
              name: "Publisher",
              kind: "rss",
              count: 1,
              tags: [],
            },
          ],
          total_posts: 2,
        },
      });
    } else if (url.pathname.endsWith("/post")) {
      await route.fulfill({
        json:
          url.searchParams.get("domain") === publisher.domain ? article : post,
      });
    } else {
      await route.fulfill({
        json: { posts: [post], next_cursor: null, limited: false },
      });
    }
  });
}

for (const width of [390, 1280]) {
  for (const view of ["view=reddit&community=science", "view=all"]) {
    test(`collection info stays inside the feed and preserves context at ${width}px in ${view}`, async ({
      page,
    }, testInfo) => {
      await page.setViewportSize({ width, height: 900 });
      await fixtures(page);
      await page.goto(`/reader/?${view}`);
      const card = page.locator(".reddit-card");
      const toggle = card.getByRole("button", {
        name: "Collection info",
        exact: true,
      });
      const original = card.getByRole("link", {
        name: `Open original: ${reddit.title}`,
      });
      await expect(toggle).toHaveAttribute(
        "title",
        /^First seen from Hacker News at /,
      );
      await expect(toggle).toHaveClass(/has-earlier-discovery/);
      const toggleBox = await toggle.boundingBox();
      const originalBox = await original.boundingBox();
      expect(
        toggleBox &&
          originalBox &&
          toggleBox.x + toggleBox.width <= originalBox.x,
      ).toBeTruthy();
      await toggle.focus();
      await page.keyboard.press("Enter");
      await expect(toggle).toHaveAttribute("aria-expanded", "true");
      const panel = card.getByRole("region", { name: "Collection info" });
      await expect(panel).toContainText(
        "already known to DANK before this link was observed",
      );
      await expect(panel).toContainText(
        "The feed preview uses text from the saved publisher article",
      );
      await expect(panel).toContainText("not publication times");
      await panel.getByText("First observed by DANK", { exact: true }).click();
      await expect(page).not.toHaveURL(/article=/);
      await expect(
        panel.locator('time[datetime="2026-10-19T09:00:00Z"]').first(),
      ).toBeVisible();
      await expect(
        panel.getByRole("link", { name: "Read saved article" }),
      ).toHaveAttribute(
        "href",
        new RegExp(`${view.replace("&", ".*")}.*article=article`),
      );
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
      await page.screenshot({
        path: testInfo.outputPath(`collection-info-${width}.png`),
        fullPage: true,
      });
      const scan = await new AxeBuilder({ page })
        .include(".reddit-card")
        .analyze();
      expect(scan.violations).toEqual([]);
      await toggle.click();
      await expect(panel).toHaveCount(0);
      await card
        .getByRole("link", { name: `Read ${reddit.title}`, exact: true })
        .click();
      const details = page.locator(".collection-info-details");
      await details.locator("summary").click();
      await expect(details).toContainText("Hacker News");
      await expect(details).toContainText("This post first collected");
      await details.getByRole("link", { name: "Read saved article" }).click();
      await expect(page).toHaveURL(
        /article=article.*article_source=publisher.test/,
      );
      if (view.includes("community="))
        await expect(page).toHaveURL(/community=science/);
    });
  }
}

test("unknown, incomplete, tied and disabled history stays explicit", async ({
  page,
}) => {
  await fixtures(page, {
    ...reddit,
    collection_info: {
      ...info,
      article_status: "incomplete",
      article_collected_at: null,
      collected_via: null,
      seen_before_post: null,
      first_seen_from: [
        firstOrigin,
        { ...firstOrigin, id: "another", label: "r/science" },
      ],
    },
  });
  await page.goto("/reader/?view=reddit");
  const toggle = page.getByRole("button", {
    name: "Collection info",
    exact: true,
  });
  await expect(toggle).toHaveAttribute("title", /Hacker News and r\/science/);
  await expect(toggle).not.toHaveClass(/has-earlier-discovery/);
  await toggle.click();
  const panel = page.getByRole("region", { name: "Collection info" });
  await expect(panel).toContainText(
    "Incomplete — some article content may be missing",
  );
  await expect(panel).toContainText("Not recorded for this saved history");
  await expect(panel).toContainText("isn’t enough saved history");
  await page.unroute("**/api/reader/**");
  await fixtures(page, {
    ...reddit,
    linked_post: null,
    collection_info: {
      ...info,
      article: null,
      article_status: "unavailable",
      article_collected_at: null,
      collected_via: null,
      seen_before_post: false,
    },
  });
  await page.reload();
  await toggle.click();
  await expect(panel).toContainText("Not collected");
  await expect(panel).toContainText("No earlier discovery appears");
  await expect(
    panel.getByRole("link", { name: "Read saved article" }),
  ).toHaveCount(0);
  await expect(
    panel.getByRole("link", { name: "Open publisher" }),
  ).toHaveAttribute("href", publisher.url);
  await page.unroute("**/api/reader/**");
  await fixtures(page, { ...reddit, collection_info: undefined });
  await page.reload();
  await expect(toggle).toHaveCount(0);
});

test("saved publisher cards show their actual Reddit collection route", async ({
  page,
}) => {
  await fixtures(page, {
    ...article,
    collection_info: {
      ...info,
      collected_via: {
        ...firstOrigin,
        domain: "reddit.com",
        source: "reddit",
        label: "r/science",
        id: reddit.id,
        url: reddit.url,
      },
    },
  });
  await page.goto("/reader/?view=stories");
  await page
    .getByRole("button", { name: "Collection info", exact: true })
    .click();
  const panel = page.getByRole("region", { name: "Collection info" });
  await expect(
    panel
      .locator("dl > div")
      .filter({ hasText: "Publisher page collected through" }),
  ).toContainText("r/science");
  await expect(
    panel.getByText("This post first collected", { exact: true }),
  ).toHaveCount(0);
  await expect(
    panel.getByRole("link", { name: "Read saved article" }),
  ).toHaveCount(0);
});

test("own post text is not attributed to the saved publisher", async ({
  page,
}) => {
  await fixtures(page, {
    ...reddit,
    html: "<p>My own explanation of the research.</p>",
    excerpt: "My own explanation of the research.",
  });
  await page.goto("/reader/?view=reddit");
  await page
    .getByRole("button", { name: "Collection info", exact: true })
    .click();
  const panel = page.getByRole("region", { name: "Collection info" });
  await expect(panel).toContainText("already known to DANK");
  await expect(panel).not.toContainText(
    "The feed preview uses text from the saved publisher article",
  );
  await page.unroute("**/api/reader/**");
  await fixtures(page, {
    ...reddit,
    source: "hacker_news",
    domain: "news.ycombinator.com",
    reddit: undefined,
    html: "",
    excerpt: "An HN submission with its own text.",
    hacker_news: {
      points: 20,
      comments: 2,
      rank: 1,
      day: "2026-10-19",
      discussion_url: "https://news.ycombinator.com/item?id=2",
      external_url: publisher.url,
    },
  });
  await page.goto("/reader/?view=all");
  await page
    .locator(".hn-card")
    .getByRole("button", { name: "Collection info", exact: true })
    .click();
  await expect(panel).not.toContainText(
    "The feed preview uses text from the saved publisher article",
  );
});

test("edited links keep post collection and link discovery times separate", async ({
  page,
}) => {
  await fixtures(page, {
    ...reddit,
    collection_info: {
      ...info,
      post_first_seen_at: "2026-10-19T07:00:00Z",
      first_seen_at: "2026-10-19T08:00:00Z",
      link_first_seen_at: "2026-10-19T09:00:00Z",
      seen_before_post: true,
    },
  });
  await page.goto("/reader/?view=reddit");
  const toggle = page.getByRole("button", {
    name: "Collection info",
    exact: true,
  });
  await expect(toggle).toHaveClass(/has-earlier-discovery/);
  await toggle.click();
  const panel = page.getByRole("region", { name: "Collection info" });
  await expect(
    panel
      .locator("dl > div")
      .filter({ hasText: "This post first collected" })
      .locator("time"),
  ).toHaveAttribute("datetime", "2026-10-19T07:00:00Z");
  await expect(
    panel
      .locator("dl > div")
      .filter({ hasText: "This link first observed" })
      .locator("time"),
  ).toHaveAttribute("datetime", "2026-10-19T09:00:00Z");
  await expect(panel).toContainText(
    "already known to DANK before this link was observed",
  );
});
