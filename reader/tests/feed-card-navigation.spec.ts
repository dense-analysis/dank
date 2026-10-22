import { type BrowserContext, expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";
import type { YouTubeItem } from "../src/lib/youtubeCollection";

const blog: Post = {
  id: "blog",
  domain: "example.com",
  source: "rss",
  url: "https://example.com/article",
  title: "A collected blog article",
  author: "Writer",
  excerpt: "Collected text stays selectable while its card opens the article.",
  html: "<p>The complete collected article.</p>",
  created_at: "2026-10-17T12:00:00Z",
  media: [],
  thumbnail: null,
};
const reddit: Post = {
  ...blog,
  id: "reddit",
  domain: "reddit.com",
  source: "reddit",
  url: "https://reddit.com/r/research/comments/reddit/example",
  title: "A collected Reddit post",
  reddit: { community: "research", score: 12, comments: 3, flair: "Research" },
};
const x: Post = {
  ...blog,
  id: "x",
  domain: "x.com",
  source: "x",
  url: "https://x.com/writer/status/x",
  title: "A collected X post",
  x: { handle: "writer", name: "Writer", icon_url: null, reply_count: 3 },
};
const hn: Post = {
  ...blog,
  id: "hn",
  domain: "news.ycombinator.com",
  source: "hacker_news",
  url: "https://news.ycombinator.com/item?id=hn",
  title: "An HN story linking to the blog",
  linked_post: blog,
  hacker_news: {
    submitted_at: blog.created_at,
    day: "2026-10-17",
    rank: 1,
    points: 42,
    comments: 2,
    external_url: blog.url,
    discussion_url: "https://news.ycombinator.com/item?id=hn",
  },
};
const posts = [blog, reddit, x, hn];
const video: YouTubeItem = {
  id: "video:dQw4w9WgXcQ",
  video_id: "dQw4w9WgXcQ",
  kind: "video",
  url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  title: "A collected video",
  text: "",
  channel: {
    id: "channel",
    name: "A channel",
    url: "https://www.youtube.com/@channel",
  },
  thumbnail_url: null,
  duration_text: "5:12",
  views_text: "10K views",
  published_text: "1 day ago",
  observed_at: blog.created_at,
  detail_capture: { status: "disabled" },
  discoveries: [],
  references: [],
  reference_count: 0,
};
const communityPost: YouTubeItem = {
  ...video,
  id: "post:community",
  video_id: null,
  kind: "post",
  title: "A community update",
  text: "A community update with selectable text.",
  url: "https://www.youtube.com/post/community",
};

async function fixtures(context: BrowserContext) {
  await context.route("https://i.ytimg.com/**", (route) =>
    route.fulfill({ status: 404 }),
  );
  await context.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/youtube/sources"))
      return route.fulfill({
        json: { feeds: [], channels: [], total: 2, linked_count: 0 },
      });
    if (url.pathname.endsWith("/youtube"))
      return route.fulfill({
        json: {
          items: [video, communityPost].filter(
            (item) =>
              !url.searchParams.has("id") ||
              item.id === url.searchParams.get("id"),
          ),
          total: 2,
          next_cursor: null,
        },
      });
    if (url.pathname.endsWith("/sources"))
      return route.fulfill({
        json: {
          sources: posts.map((post) => ({
            domain: post.domain,
            name: post.domain,
            kind: post.source,
            count: 1,
            tags: [],
          })),
          total_posts: 4,
        },
      });
    if (url.pathname.endsWith("/post"))
      return route.fulfill({
        json: posts.find(
          (post) =>
            post.id === url.searchParams.get("id") &&
            post.domain === url.searchParams.get("domain"),
        ),
      });
    const kind = url.searchParams.get("kind");

    return route.fulfill({
      json: {
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
}

async function expectBackgroundTab(
  page: Page,
  click: () => Promise<void>,
  href: string,
) {
  const originalUrl = page.url();
  const context = page.context();
  const [tab] = await Promise.all([context.waitForEvent("page"), click()]);
  await tab.waitForURL(new URL(href, originalUrl).href);
  await tab.locator(".reader-toolbar, .yt-card-detailed").first().waitFor();
  // Playwright normally makes every tab report focus, hiding foreground-tab bugs.
  const session = await context.newCDPSession(tab);
  await session.send("Emulation.setFocusEmulationEnabled", { enabled: false });
  await expect.poll(() => tab.evaluate(() => document.hasFocus())).toBe(false);
  await expect(page).toHaveURL(originalUrl);
  expect(await tab.evaluate(() => window.opener === null)).toBe(true);
  await tab.close();
  expect(context.pages()).toHaveLength(1);
}

for (const [post, dedicated] of [
  [blog, "stories"],
  [reddit, "reddit"],
  [x, "x"],
  [hn, "hacker_news"],
] as const) {
  for (const view of [dedicated, "all"]) {
    test(`${post.source} cards keep Cmd-click and middle-click in the background in ${view}`, async ({
      page,
      context,
    }) => {
      await fixtures(context);
      await page.goto(
        `/reader/?view=${view}&sort=oldest&q=collected&after=2026-10-01`,
      );
      const primary = page.getByRole("link", {
        name: `Read ${post.title}`,
        exact: true,
      });
      const card = page.locator(".post-card").filter({ has: primary });
      await expect(primary).toBeVisible();
      const href = await primary.getAttribute("href");
      if (!href) throw new Error("Missing primary post destination");
      await expect(card).toHaveCSS("cursor", "pointer");
      const originalUrl = page.url();
      await expectBackgroundTab(
        page,
        () =>
          card.click({
            position: { x: 8, y: 8 },
            modifiers: ["ControlOrMeta"],
          }),
        href,
      );
      await expectBackgroundTab(
        page,
        () => card.click({ position: { x: 8, y: 8 }, button: "middle" }),
        href,
      );
      await expectBackgroundTab(
        page,
        () => primary.click({ modifiers: ["ControlOrMeta"] }),
        href,
      );
      await card.click({ position: { x: 8, y: 8 } });
      await expect(page).toHaveURL(new URL(href, originalUrl).href);
      await page.goBack();
      await expect(page).toHaveURL(originalUrl);
      await expect(primary).toBeFocused();
    });
  }
}

test("Hacker News discussion links retain their own destination", async ({
  page,
  context,
}) => {
  await fixtures(context);
  await page.goto("/reader/?view=hacker_news&hn_filter=top20");
  const discussion = page.getByRole("link", {
    name: `Discussion on Hacker News: ${hn.title}`,
  });
  const href = await discussion.getAttribute("href");
  if (!href) throw new Error("Missing discussion destination");
  expect(new URL(href, page.url()).searchParams.get("article")).toBe(hn.id);
  await expectBackgroundTab(
    page,
    () => discussion.click({ modifiers: ["ControlOrMeta"] }),
    href,
  );
});

for (const kind of ["video", "post"] as const) {
  test(`YouTube ${kind} cards and information links open background details`, async ({
    page,
    context,
  }) => {
    await fixtures(context);
    await page.goto("/reader/?view=youtube");
    const card = page.locator(`.yt-card.yt-${kind}`);
    const details = card.getByRole("link", { name: /Discovery details for/ });
    await expect(details).toBeVisible();
    const href = await details.getAttribute("href");
    if (!href) throw new Error("Missing discovery destination");
    const metadata = card.locator(
      kind === "video" ? ".yt-channel" : ".yt-post-identity strong",
    );
    await expectBackgroundTab(
      page,
      () => metadata.click({ modifiers: ["ControlOrMeta"] }),
      href,
    );
    await expectBackgroundTab(
      page,
      () => details.click({ modifiers: ["ControlOrMeta"] }),
      href,
    );
    await metadata.click();
    await expect(page).toHaveURL(new URL(href, page.url()).href);
    await expect(page.locator(".yt-card-detailed")).not.toHaveClass(
      /navigable-card/,
    );
  });
}

for (const width of [1280, 390]) {
  test(`source links copy the original URL without opening cards at ${width}px`, async ({
    page,
    context,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await fixtures(context);
    await page.addInitScript(() => {
      Object.defineProperty(navigator, "clipboard", {
        configurable: true,
        value: {
          writeText: async (text: string) => {
            document.documentElement.dataset.copiedSource = text;
          },
        },
      });
    });

    for (const [post, view] of [
      [blog, "stories"],
      [reddit, "reddit"],
      [x, "x"],
      [hn, "hacker_news"],
    ] as const) {
      for (const feed of [view, "all"]) {
        await page.goto(`/reader/?view=${feed}`);
        const primary = page.getByRole("link", {
          name: `Read ${post.title}`,
          exact: true,
        });
        const card = page.locator(".post-card").filter({ has: primary });
        const copy = card.getByRole("button", { name: "Copy source link" });
        const destination = post === hn ? blog.url : post.url;
        const before = page.url();
        await copy.click();
        await expect(card.getByRole("status")).toHaveText("Link copied");
        await expect(page.locator("html")).toHaveAttribute(
          "data-copied-source",
          destination,
        );
        await expect(page).toHaveURL(before);
        expect(context.pages()).toHaveLength(1);
        const bounds = await copy.boundingBox();
        expect(bounds?.x).toBeGreaterThanOrEqual(0);
        expect((bounds?.x ?? 0) + (bounds?.width ?? 0)).toBeLessThanOrEqual(
          width,
        );
      }
    }

    await page.goto("/reader/?view=youtube");

    for (const item of [video, communityPost]) {
      const card = page.locator(`.yt-card.yt-${item.kind}`);
      const before = page.url();
      await card.getByRole("button", { name: "Copy source link" }).click();
      await expect(page.locator("html")).toHaveAttribute(
        "data-copied-source",
        item.url,
      );
      await expect(page).toHaveURL(before);
      expect(context.pages()).toHaveLength(1);
    }

    await page.goto(
      "/reader/?view=stories&article=blog&article_source=example.com",
    );
    const original = page.getByRole("link", {
      name: "Read original",
      exact: true,
    });
    const actions = original.locator("..");
    await actions.getByRole("button", { name: "Copy source link" }).focus();
    await page.keyboard.press("Enter");
    await expect(actions.getByRole("status")).toHaveText("Link copied");
    await expect(page.locator("html")).toHaveAttribute(
      "data-copied-source",
      blog.url,
    );

    await page.evaluate(() => {
      Object.defineProperty(navigator, "clipboard", {
        configurable: true,
        value: {
          writeText: async () => {
            throw new Error("Permission denied");
          },
        },
      });
    });
    await actions.getByRole("button", { name: "Copy source link" }).click();
    await expect(actions.getByRole("status")).toContainText("Copy failed");
    await expect(original).toHaveAttribute("href", blog.url);
    await expect(
      actions.getByRole("button", { name: "Copy source link" }),
    ).toBeEnabled();
  });
}
