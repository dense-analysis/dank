import { expect, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

test.use({ timezoneId: "Europe/London", locale: "en-GB" });

test("All and source readers share recent dates and switch yesterday to an exact date", async ({
  page,
}) => {
  await page.clock.setFixedTime(new Date("2026-10-18T00:10:00+01:00"));
  const recent = "2026-10-18T00:05:00+01:00";
  const yesterday = "2026-10-17T23:50:00+01:00";
  const posts: Post[] = [
    "rss",
    "reddit",
    "x",
    "hacker_news",
    "youtube",
  ].flatMap((source) =>
    [recent, yesterday].map((date, index) => ({
      id: `${source}-${index}`,
      domain:
        source === "rss"
          ? "example.com"
          : source === "hacker_news"
            ? "news.ycombinator.com"
            : `${source}.com`,
      source,
      url: "https://example.com/story",
      author: "Writer",
      title: `${source} story ${index}`,
      html: `<p>${source} story ${index}</p>`,
      excerpt: `${source} story ${index}`,
      created_at: date,
      thumbnail: null,
      media: [],
      ...(source === "reddit"
        ? {
            reddit: {
              community: "example",
              score: 1,
              comments: 0,
              flair: null,
            },
          }
        : {}),
      ...(source === "x"
        ? { x: { handle: "writer", name: "Writer", media: [] } }
        : {}),
      ...(source === "hacker_news"
        ? {
            hacker_news: {
              submitted_at: date,
              points: 1,
              comments: 0,
              day: "2026-10-18",
              rank: 1,
              discussion_url: "https://news.ycombinator.com/item?id=1",
              external_url: null,
            },
          }
        : {}),
    })),
  );
  await page.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());
    const kind = url.searchParams.get("kind");
    return route.fulfill({
      json: url.pathname.endsWith("/sources")
        ? { sources: [], total_posts: posts.length }
        : url.pathname.endsWith("/post")
          ? posts.find((post) => post.id === url.searchParams.get("id"))
          : {
              posts: posts.filter(
                (post) =>
                  kind === "all" ||
                  post.source === (kind === "stories" ? "rss" : kind),
              ),
              next_cursor: null,
              limited: false,
            },
    });
  });

  for (const view of ["all", "stories", "reddit", "x", "hacker_news"]) {
    await page.goto(`/reader/?view=${view}`);
    const times = page.locator(".post-card time");
    await expect(times).toHaveCount(view === "all" ? 10 : 2);
    for (let index = 0; index < (await times.count()); index += 2) {
      await expect(times.nth(index)).toHaveText("5 minutes ago");
      await expect(times.nth(index)).toHaveAttribute(
        "title",
        /18 Oct 2026, 00:05/,
      );
      await expect(times.nth(index + 1)).toHaveText("17 Oct 2026, 23:50");
    }
  }

  for (const post of posts) {
    await page.goto(
      `/reader/?article=${post.id}&article_source=${post.domain}`,
    );
    await expect(page.locator(".article-page time").first()).toHaveText(
      post.created_at === recent ? "5 minutes ago" : "17 Oct 2026, 23:50",
    );
  }
});
