import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post, Source } from "../src/lib/types";
import type { YouTubeItem } from "../src/lib/youtubeCollection";

type MixedPost = Post & {
  youtube?: {
    kind: "video" | "short" | "post";
    video_id: string | null;
    channel: { id: string; name: string; url: string };
    duration_text: string;
    views_text: string;
    thumbnail_url: string | null;
    video_url: string | null;
  };
};

const blog: MixedPost = {
  id: "followed",
  domain: "followed.example",
  source: "rss",
  url: "https://followed.example/analysis",
  title: "Analysis from a followed blog",
  author: "Ada Writer",
  excerpt: "Collected analysis selected by following this publisher.",
  html: "<p>Collected analysis selected by following this publisher.</p>",
  created_at: "2026-10-21T12:00:00Z",
  thumbnail: null,
  media: [],
};
const discovered: MixedPost = {
  ...blog,
  id: "discovered",
  domain: "discovered.example",
  url: "https://discovered.example/analysis",
  title: "Analysis discovered through Hacker News",
};
const reddit: MixedPost = {
  ...blog,
  id: "reddit",
  domain: "reddit.com",
  source: "reddit",
  url: "https://www.reddit.com/r/research/comments/reddit/analysis/",
  title: "Analysis discussed on Reddit",
  reddit: { community: "research", score: 42, comments: 5, flair: "Research" },
};
const x: MixedPost = {
  ...blog,
  id: "x",
  domain: "x.com",
  source: "x",
  url: "https://x.com/ada/status/x",
  title: "Analysis shared on X",
  html: "<p>Analysis shared on X.</p>",
  excerpt: "Analysis shared on X.",
  x: { handle: "ada", name: "Ada Writer", icon_url: null, reply_count: 2 },
};
const hackerNews: MixedPost = {
  ...blog,
  id: "hacker-news",
  domain: "news.ycombinator.com",
  source: "hacker_news",
  url: "https://news.ycombinator.com/item?id=hacker-news",
  title: "Analysis discussed on Hacker News",
  hacker_news: {
    submitted_at: blog.created_at,
    day: "2026-10-21",
    rank: 1,
    points: 25,
    comments: 3,
    external_url: null,
    discussion_url: "https://news.ycombinator.com/item?id=hacker-news",
  },
};
const video = {
  ...blog,
  id: "video:dQw4w9WgXcQ",
  domain: "youtube.com",
  source: "youtube",
  url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  title: "Analysis explained on video",
  author: "Research Channel",
  excerpt: "",
  html: "",
  thumbnail: "/assets/universal-video.svg",
  youtube: {
    kind: "video",
    video_id: "dQw4w9WgXcQ",
    channel: {
      id: "research",
      name: "Research Channel",
      url: "https://www.youtube.com/@research",
    },
    duration_text: "12:34",
    views_text: "25K views",
    thumbnail_url: "/assets/universal-video.svg",
    video_url: null,
  },
} satisfies MixedPost;
const short: MixedPost = {
  ...video,
  id: "video:abcdefghijk",
  url: "https://www.youtube.com/shorts/abcdefghijk",
  title: "Analysis in a short",
  youtube: {
    ...video.youtube,
    kind: "short",
    video_id: "abcdefghijk",
    duration_text: "0:42",
  },
};
const community: MixedPost = {
  ...video,
  id: "post:community",
  url: "https://www.youtube.com/post/community",
  title: "Analysis community update",
  excerpt: "A community update with a useful research link.",
  html: "<p>A community update with a useful research link.</p>",
  youtube: {
    ...video.youtube,
    kind: "post",
    video_id: null,
    duration_text: "",
    views_text: "",
  },
};
const posts = [
  blog,
  discovered,
  reddit,
  x,
  hackerNews,
  video,
  short,
  community,
];
const sources: (Source & { configured: boolean })[] = [
  {
    domain: blog.domain,
    name: "Followed Blog",
    kind: "rss",
    tags: ["research"],
    count: 1,
    configured: true,
  },
  {
    domain: discovered.domain,
    name: "Discovered Publisher",
    kind: "rss",
    tags: [],
    count: 1,
    configured: false,
  },
  {
    domain: reddit.domain,
    name: "Reddit",
    kind: "reddit",
    tags: [],
    count: 1,
    configured: true,
    communities: [{ name: "research", count: 1 }],
  },
  {
    domain: x.domain,
    name: "X",
    kind: "x",
    tags: [],
    count: 1,
    configured: true,
    accounts: [{ handle: "ada", name: "Ada Writer", count: 1, icon_url: null }],
  },
  {
    domain: hackerNews.domain,
    name: "Hacker News",
    kind: "hacker_news",
    tags: [],
    count: 1,
    configured: true,
  },
  {
    domain: video.domain,
    name: "YouTube",
    kind: "youtube",
    tags: [],
    count: 3,
    configured: true,
  },
];

function platform(post: MixedPost) {
  return post.source === "rss" || post.source === "hacker_news"
    ? "stories"
    : post.source;
}

async function fixture(page: Page) {
  const requests: URL[] = [];
  await page.context().route(/universal-video|i\.ytimg\.com/, (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360"><rect width="640" height="360" fill="#266d69"/><text x="32" y="180" font-size="38" fill="white">Research video</text></svg>',
    }),
  );
  await page.context().route("https://www.youtube-nocookie.com/**", (route) =>
    route.fulfill({
      contentType: "text/html",
      body: "<p>Video player fixture</p>",
    }),
  );
  await page.context().route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    requests.push(url);

    if (url.pathname.endsWith("/youtube/sources")) {
      return route.fulfill({
        json: { feeds: [], channels: [], total: 3, linked_count: 0 },
      });
    }

    if (url.pathname.endsWith("/youtube")) {
      const items: YouTubeItem[] = posts
        .filter(
          (
            post,
          ): post is MixedPost & {
            youtube: NonNullable<MixedPost["youtube"]>;
          } => !!post.youtube,
        )
        .map((post) => ({
          ...post.youtube,
          id: post.id,
          title: post.title,
          url: post.url,
          text: post.excerpt,
          published_text: "1 day ago",
          observed_at: post.created_at,
          detail_capture: { status: "disabled" },
          discoveries: [],
          references: [],
          reference_count: 0,
        }));

      return route.fulfill({
        json: {
          items: items.filter(
            (item) => !params.has("id") || item.id === params.get("id"),
          ),
          total: 3,
          next_cursor: null,
        },
      });
    }

    if (url.pathname.endsWith("/sources")) {
      return route.fulfill({ json: { sources, total_posts: posts.length } });
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

    if (url.pathname.endsWith("/transcript")) {
      return route.fulfill({
        json: { status: "unavailable", segments: [], language: "", text: "" },
      });
    }

    const selectedPlatforms = params.getAll("platform");
    const kind = params.get("kind") ?? "all";
    const selected = posts.filter((post) => {
      if (kind === "stories" && platform(post) !== "stories") return false;
      if (kind !== "all" && kind !== "stories" && post.source !== kind)
        return false;
      if (
        selectedPlatforms.length &&
        !selectedPlatforms.includes(platform(post))
      )
        return false;
      if (
        params.has("domain") &&
        !params.getAll("domain").includes(post.domain)
      )
        return false;

      return true;
    });

    return route.fulfill({
      json: { posts: selected, next_cursor: null, limited: false },
    });
  });

  return requests;
}

function chips(page: Page) {
  return page.getByRole("group", { name: "Filter by platform", exact: true });
}

function story(page: Page, post: Post) {
  return page.getByRole("link", { name: `Read ${post.title}`, exact: true });
}

async function expectPlatforms(page: Page, selected: string[]) {
  await expect
    .poll(() => new URL(page.url()).searchParams.getAll("platform").sort())
    .toEqual([...selected].sort());
}

test("quick platform filters combine with OR, preserve other filters and reset independently", async ({
  page,
}) => {
  const requests = await fixture(page);
  const initial =
    "q=analysis&sort=oldest&after=2026-10-01&before=2026-10-22&author=Ada+Writer&author_match=exact";
  await page.goto(`/reader/?${initial}`);
  const filters = chips(page);
  await expect(
    filters.getByRole("button", { name: "All sources", exact: true }),
  ).toHaveAttribute("aria-pressed", "true");
  await filters.getByRole("button", { name: "Reddit", exact: true }).click();
  await filters.getByRole("button", { name: "Blogs", exact: true }).click();
  await expectPlatforms(page, ["reddit", "stories"]);
  await expect(story(page, reddit)).toBeVisible();
  await expect(story(page, hackerNews)).toBeVisible();
  await expect(story(page, discovered)).toBeVisible();
  await expect(story(page, blog)).toBeVisible();
  await expect(
    page.getByRole("button", { name: `Play ${video.title}`, exact: true }),
  ).toHaveCount(0);

  for (const [key, value] of new URLSearchParams(initial)) {
    expect(new URL(page.url()).searchParams.get(key)).toBe(value);
  }

  await expect
    .poll(() =>
      requests.some(
        (url) =>
          url.pathname.endsWith("/posts") &&
          ["reddit", "stories"].every((value) =>
            url.searchParams.getAll("platform").includes(value),
          ),
      ),
    )
    .toBe(true);
  await filters.getByRole("button", { name: "Reddit", exact: true }).click();
  await expectPlatforms(page, ["stories"]);
  await expect(story(page, reddit)).toHaveCount(0);
  await filters
    .getByRole("button", { name: "All sources", exact: true })
    .click();
  await expectPlatforms(page, []);
  await expect(story(page, blog)).toBeVisible();
  await expect(story(page, reddit)).toBeVisible();
  expect(new URL(page.url()).searchParams.get("q")).toBe("analysis");
  expect(new URL(page.url()).searchParams.get("after")).toBe("2026-10-01");
});

test("platform choices survive search, article return and reload, then clear on section navigation", async ({
  page,
}) => {
  await fixture(page);
  await page.goto("/reader/?platform=reddit&platform=stories&after=2026-10-01");
  await page
    .getByRole("searchbox", { name: "Search everything", exact: true })
    .fill("analysis");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await page
    .getByRole("combobox", { name: "Order stories" })
    .selectOption("oldest");
  await expectPlatforms(page, ["reddit", "stories"]);
  const before = page.url();
  const primary = story(page, discovered);
  await expect(primary).toHaveAttribute("href", /platform=/);
  await primary.click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await expectPlatforms(page, ["reddit", "stories"]);
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page).toHaveURL(before);
  await expect(primary).toBeFocused();
  await page.reload();
  await expect(
    chips(page).getByRole("button", { name: "Reddit", exact: true }),
  ).toHaveAttribute("aria-pressed", "true");
  await expect(
    chips(page).getByRole("button", { name: "Blogs", exact: true }),
  ).toHaveAttribute("aria-pressed", "true");
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Blogs", exact: true })
    .click();
  await expectPlatforms(page, []);
  await expect(chips(page)).toHaveCount(0);
  await expect(story(page, blog)).toBeVisible();
  await expect(story(page, discovered)).toBeVisible();
  await expect(story(page, hackerNews)).toBeVisible();
  await page.getByRole("button", { name: /^Filters/ }).click();
  const source = page.getByRole("combobox", { name: "Source", exact: true });
  await expect(source.locator("option")).toHaveText([
    "All sources",
    "Followed Blog",
    "Discovered Publisher",
    "Hacker News",
  ]);
});

test("legacy Hacker News platform links expose their selection as Blogs", async ({
  page,
}) => {
  const requests = await fixture(page);
  await page.goto("/reader/?platform=hacker_news");
  const filters = chips(page);
  await expect(
    filters.getByRole("button", { name: "Blogs", exact: true }),
  ).toHaveAttribute("aria-pressed", "true");
  await expect(story(page, hackerNews)).toBeVisible();
  await expect(story(page, blog)).toBeVisible();
  await filters.getByRole("button", { name: "X", exact: true }).click();
  await expectPlatforms(page, ["stories", "x"]);
  await expect(story(page, x)).toBeVisible();
  await expect(story(page, hackerNews)).toBeVisible();
  await expect
    .poll(() =>
      requests.some(
        (url) =>
          url.pathname.endsWith("/posts") &&
          url.searchParams.getAll("platform").length === 2 &&
          ["stories", "x"].every((value) =>
            url.searchParams.getAll("platform").includes(value),
          ),
      ),
    )
    .toBe(true);
  await filters.getByRole("button", { name: "Blogs", exact: true }).click();
  await expectPlatforms(page, ["x"]);
  await expect(story(page, hackerNews)).toHaveCount(0);
  await expect(story(page, blog)).toHaveCount(0);
});

test("Blogs quick filter includes configured blogs, discovered articles and Hacker News", async ({
  page,
}) => {
  await fixture(page);
  await page.goto("/reader/");
  await expect(story(page, discovered)).toBeVisible();
  await expect(
    chips(page).getByRole("button", { name: "Hacker News", exact: true }),
  ).toHaveCount(0);
  const blogs = chips(page).getByRole("button", { name: "Blogs", exact: true });
  await blogs.focus();
  await page.keyboard.press("Space");
  await expectPlatforms(page, ["stories"]);
  await expect(blogs).toHaveAttribute("aria-pressed", "true");
  await expect(story(page, blog)).toBeVisible();
  await expect(story(page, discovered)).toBeVisible();
  await expect(story(page, hackerNews)).toBeVisible();
  await expect(story(page, reddit)).toHaveCount(0);
  await page.keyboard.press("Space");
  await expectPlatforms(page, []);
  await expect(story(page, discovered)).toBeVisible();
});

test("discovered publisher source links open a filtered Blogs view in normal and new-tab navigation", async ({
  page,
  context,
}) => {
  await fixture(page);
  await page.goto("/reader/?platform=stories");
  const card = page
    .locator(".post-card")
    .filter({ has: story(page, discovered) });
  const source = card.locator(".source-link");
  const href = await source.getAttribute("href");
  if (!href) throw new Error("Missing source destination");
  expect(new URL(href, page.url()).searchParams.get("view")).toBe("stories");
  const initial = page.url();
  const [tab] = await Promise.all([
    context.waitForEvent("page"),
    source.click({ modifiers: ["ControlOrMeta"] }),
  ]);
  await expect(story(tab, discovered)).toBeVisible();
  await expect(story(tab, blog)).toHaveCount(0);
  await expect(tab).toHaveURL(new URL(href, initial).href);
  await expect(page).toHaveURL(initial);
  await tab.close();
  await source.click();
  await expect(page).toHaveURL(new URL(href, initial).href);
  await expect(story(page, discovered)).toBeVisible();
  await expect(story(page, blog)).toHaveCount(0);
});

test("video card background navigation retains its mixed-feed filters", async ({
  page,
  context,
}) => {
  await fixture(page);
  await page.goto("/reader/?platform=youtube&platform=reddit&after=2026-10-01");
  const card = page.locator(".youtube-feed-video");
  await expect(card).toBeVisible();
  const initial = page.url();
  const [tab] = await Promise.all([
    context.waitForEvent("page"),
    card.click({ position: { x: 8, y: 8 }, modifiers: ["ControlOrMeta"] }),
  ]);
  await expect(tab.locator(".yt-card-detailed")).toBeVisible();
  await expectPlatforms(tab, ["youtube", "reddit"]);
  expect(new URL(tab.url()).searchParams.get("youtube_item")).toBe(video.id);
  expect(new URL(tab.url()).searchParams.get("after")).toBe("2026-10-01");
  await expect(page).toHaveURL(initial);
  await expect(page.locator("iframe, video")).toHaveCount(0);
  await tab.close();
});

test("YouTube videos, Shorts and community posts fit the mixed feed and play only on demand", async ({
  page,
}) => {
  await fixture(page);
  await page.goto("/reader/?platform=youtube&after=2026-10-01");
  await expect(page.locator(".youtube-post-card")).toHaveCount(3);
  await expect(page.locator(".youtube-feed-video")).toContainText(
    "Research Channel",
  );
  await expect(page.locator(".youtube-feed-video")).toContainText("12:34");
  await expect(page.locator(".youtube-feed-short")).toContainText("0:42");
  await expect(page.locator(".youtube-feed-post")).toContainText(
    community.excerpt,
  );
  await expect(
    page.locator(".youtube-feed-post").getByRole("button", { name: /^Play / }),
  ).toHaveCount(0);
  await expect(page.locator("iframe, video")).toHaveCount(0);
  const initial = page.url();
  await page
    .getByRole("button", { name: `Play ${video.title}`, exact: true })
    .click();
  await expect(page.locator("iframe")).toHaveCount(1);
  await expect(page).toHaveURL(initial);
  await page.getByRole("button", { name: "Close video", exact: true }).click();
  await expect(page.locator("iframe, video")).toHaveCount(0);
  await expect(page).toHaveURL(initial);
  await page.locator(".youtube-feed-video [data-card-link]").click();
  await expect(page.locator(".yt-card-detailed")).toBeVisible();
  await expectPlatforms(page, ["youtube"]);
  expect(new URL(page.url()).searchParams.get("youtube_item")).toBe(video.id);
  expect(new URL(page.url()).searchParams.get("after")).toBe("2026-10-01");
  await page.getByRole("button", { name: "Back to all", exact: true }).click();
  await expect(page).toHaveURL(initial);
});

for (const width of [320, 390, 1440]) {
  test(`mixed feed and quick filters are accessible without page overflow at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    await fixture(page);
    await page.goto("/reader/");
    const filters = chips(page);

    for (const name of ["All sources", "X", "Reddit", "YouTube", "Blogs"]) {
      const button = filters.getByRole("button", { name, exact: true });
      await button.scrollIntoViewIfNeeded();
      await expect(button).toBeInViewport();
    }

    await page.locator(".youtube-feed-post").scrollIntoViewIfNeeded();
    await expect(filters).toBeInViewport();
    await filters.getByRole("button", { name: "Reddit", exact: true }).click();
    await expectPlatforms(page, ["reddit"]);
    await expect(story(page, reddit)).toBeVisible();
    await filters
      .getByRole("button", { name: "All sources", exact: true })
      .click();
    await expect(page.locator(".youtube-post-card")).toHaveCount(3);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    const result = await new AxeBuilder({ page }).analyze();
    expect(
      result.violations
        .filter((item) => ["serious", "critical"].includes(item.impact ?? ""))
        .map((item) => ({
          id: item.id,
          nodes: item.nodes.map((node) => node.target),
        })),
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath(`universal-feed-${width}.png`),
      fullPage: true,
    });
  });
}
