import { readFileSync } from "node:fs";
import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { YouTubeItem, YouTubeSources } from "../src/lib/youtubeCollection";

const news = "https://www.youtube.com/feed/news_destination";
const hype = "https://www.youtube.com/feed/hype";
const capture = "2026-10-17T16:00:00Z";
function item(
  id: string,
  title: string,
  kind: YouTubeItem["kind"] = "video",
): YouTubeItem {
  return {
    id: `${kind === "post" ? "post" : "video"}:${id}`,
    video_id: kind === "post" ? null : id,
    kind,
    title,
    text:
      kind === "post"
        ? "Our correspondents report from the field. Follow the stories behind the headlines, with context from today's newsroom."
        : "",
    url:
      kind === "post"
        ? `https://www.youtube.com/post/${id}`
        : `https://www.youtube.com/${kind === "short" ? "shorts/" : "watch?v="}${id}`,
    channel: {
      id: "channel4",
      name: "Channel 4 News",
      url: "https://www.youtube.com/@Channel4News",
    },
    thumbnail_url: `https://i.ytimg.com/vi/${id}/hqdefault.jpg`,
    duration_text: "12:45",
    views_text: "24K views",
    published_text: "2 hours ago",
    observed_at: capture,
    detail_capture: { status: "disabled" },
    discoveries: [
      {
        feed_url: news,
        feed_name: "News · Top stories",
        feed_kind: "news",
        tab: "top_stories",
        shelf: "The world in focus",
        rank: null,
        region: "United Kingdom",
        playlist_id: "",
        observed_at: capture,
        run_id: "capture-one",
      },
    ],
    references: [],
    reference_count: 0,
  };
}
const first = {
  ...item("dQw4w9WgXcQ", "Inside the cities shaping a greener future"),
  discoveries: [
    {
      ...item("dQw4w9WgXcQ", "").discoveries[0],
      feed_url: hype,
      feed_name: "Hype · Top 100",
      feed_kind: "hype",
      shelf: "Top 100",
      tab: "",
      rank: 1,
      playlist_id: "OLJbGgOZgwZ9k_XXE2tcr-JsD4tag1iKjng",
    },
  ],
  reference_count: 2,
  references: [
    {
      domain: "x.com",
      post_id: "source-post",
      url: "https://x.com/reporter/status/123",
      title: "A different view of the future of our cities",
      source: "x",
      reader_url: "/reader/?article=source-post&article_source=x.com",
    },
    {
      domain: "lobste.rs",
      post_id: "blog-link",
      url: "https://lobste.rs/s/video",
      title: "Urban design worth watching",
      source: "rss",
      reader_url: "/reader/?article=blog-link&article_source=lobste.rs",
    },
  ],
};
const videos = [
  first,
  item("aaaaaaaaaaa", "A new chapter for clean energy"),
  item("bbbbbbbbbbb", "What the next generation of transport looks like"),
  item("ccccccccccc", "On the ground: a changing world"),
  item("ddddddddddd", "The science behind a quieter city", "short"),
  item("eeeeeeeeeee", "One minute from the newsroom", "short"),
  item("fffffffffff", "A fresh perspective on tomorrow", "short"),
  item("UgkxPostOne", "From the newsroom", "post"),
];
const sources: YouTubeSources = {
  feeds: [
    {
      id: hype,
      url: hype,
      name: "Hype · Top 100",
      kind: "hype",
      count: 1,
      region: "United Kingdom",
      tab: "",
    },
    {
      id: news,
      url: news,
      name: "News · Top stories",
      kind: "news",
      count: 7,
      region: "United Kingdom",
      tab: "top_stories",
    },
    {
      id: `${news}/business`,
      url: `${news}/business`,
      name: "News · Business",
      kind: "news",
      count: 0,
      region: "United Kingdom",
      tab: "business",
    },
    {
      id: `${news}/world`,
      url: `${news}/world`,
      name: "News · World",
      kind: "news",
      count: 0,
      region: "United Kingdom",
      tab: "world",
    },
  ],
  channels: [
    {
      id: "channel4",
      name: "Channel 4 News",
      url: "https://www.youtube.com/@Channel4News/videos",
      count: 8,
    },
  ],
  total: videos.length,
  linked_count: 1,
};

async function fixture(page: Page) {
  const state = {
    data: videos,
    sources,
    requests: [] as URL[],
    fail: false,
    failMore: false,
    pageSize: 30,
  };
  await page.route("https://i.ytimg.com/**", (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: `<svg xmlns="http://www.w3.org/2000/svg" width="800" height="450"><defs><linearGradient id="a" x2="1" y2="1"><stop stop-color="#517c78"/><stop offset="1" stop-color="#172f40"/></linearGradient></defs><rect width="800" height="450" fill="url(#a)"/><circle cx="600" cy="120" r="74" fill="#f2d5a6" opacity=".8"/><path d="m0 450 0-120 80 0 0-90 60 0 0 150 60 0 0-200 90 0 0 260 80 0 0-140 60 0 0 100 80 0 0-200 60 0 0 290z" fill="#142b32"/><path d="m0 450 800-170v170z" fill="#537d59"/></svg>`,
    }),
  );
  await page.route("https://www.youtube-nocookie.com/**", (route) =>
    route.fulfill({
      contentType: "text/html",
      body: "<!doctype html><html><body style='background:#111;color:white'>YouTube fixture player</body></html>",
    }),
  );
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    state.requests.push(url);
    const params = url.searchParams;
    if (url.pathname.endsWith("/youtube/sources"))
      return route.fulfill({ json: state.sources });
    if (url.pathname.endsWith("/youtube")) {
      if (state.fail || (state.failMore && params.has("cursor")))
        return route.fulfill({
          status: 503,
          json: { detail: "Collection temporarily unavailable" },
        });
      const selected = state.data.filter(
        (entry) =>
          (!params.get("id") || entry.id === params.get("id")) &&
          (!params.get("feed") ||
            (params.get("feed") === "linked"
              ? entry.references.length > 0
              : entry.discoveries.some(
                  (discovery) => discovery.feed_url === params.get("feed"),
                ))) &&
          (!params.get("channel") ||
            entry.channel.id === params.get("channel")) &&
          (!params.get("kind") || entry.kind === params.get("kind")) &&
          (!params.get("q") ||
            entry.title
              .toLowerCase()
              .includes(params.get("q")?.toLowerCase() ?? "")),
      );
      const offset = Number(params.get("cursor") ?? 0);
      return route.fulfill({
        json: {
          items: selected.slice(offset, offset + state.pageSize),
          total: selected.length,
          next_cursor:
            offset + state.pageSize < selected.length
              ? String(offset + state.pageSize)
              : null,
        },
      });
    }
    if (url.pathname.endsWith("/sources"))
      return route.fulfill({ json: { sources: [], total_posts: 0 } });
    if (url.pathname.endsWith("/post"))
      return route.fulfill({
        json: {
          id: params.get("id"),
          domain: params.get("domain"),
          url: "https://example.com/story",
          title: "Urban design worth watching",
          author: "Reporter",
          source: "rss",
          created_at: capture,
          excerpt: "A collected source article",
          html: "<p>A collected source article</p>",
          media: [],
          thumbnail: null,
        },
      });
    return route.fulfill({
      json: { posts: [], next_cursor: null, limited: false },
    });
  });

  return state;
}
async function noOverflow(page: Page) {
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
}

test("YouTube offers independent shelves, portrait Shorts, posts and safe on-demand playback", async ({
  page,
}) => {
  const state = await fixture(page);
  await page.goto("/reader/?view=youtube");
  await expect(
    page.getByRole("heading", { name: "Collected recommendations" }),
  ).toBeVisible();
  await expect(page.locator(".yt-card")).toHaveCount(8);
  await expect(
    page.getByRole("region", { name: "Top 100", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Shorts", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Community posts", exact: true }),
  ).toBeVisible();
  await expect(page.locator("iframe")).toHaveCount(0);
  expect(state.requests.some((url) => url.pathname.endsWith("/posts"))).toBe(
    false,
  );
  await page
    .getByRole("button", { name: `Play ${first.title}`, exact: true })
    .click();
  await expect(
    page.getByTitle(`YouTube video: ${first.title}`),
  ).toHaveAttribute("src", /youtube-nocookie.com\/embed\/dQw4w9WgXcQ/);
  await page.getByRole("button", { name: "Close video", exact: true }).click();
  await expect(page.locator("iframe")).toHaveCount(0);
  const shorts = page.locator(".yt-short .yt-thumbnail").first();
  const box = await shorts.boundingBox();
  expect((box?.height ?? 0) / (box?.width ?? 1)).toBeCloseTo(16 / 9, 1);
  expect(
    (await new AxeBuilder({ page }).analyze()).violations.filter((entry) =>
      ["critical", "serious"].includes(entry.impact ?? ""),
    ),
  ).toEqual([]);
});

test("feed and format search preserve state through discovery details, source articles and history", async ({
  page,
}) => {
  const state = await fixture(page);
  await page.goto("/reader/?view=youtube&feed=linked");
  await expect(page.locator(".yt-card")).toHaveCount(1);
  await page
    .getByRole("searchbox", { name: "Search From your feeds", exact: true })
    .fill("cities");
  await page.getByRole("button", { name: "Search YouTube collection" }).click();
  const timeline = page.url();
  await page.getByRole("link", { name: first.title, exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "How it found you" }),
  ).toBeVisible();
  await expect(page.getByText("#1", { exact: true }).last()).toBeVisible();
  await expect(
    page.getByRole("link", { name: "View playlist" }),
  ).toHaveAttribute("href", /list=OLJb/);
  await expect(page.locator(".yt-discovery time")).toHaveText(/2026.*UTC/);
  await page.getByRole("link", { name: /lobste.rs Urban design/ }).click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await page.goBack();
  await expect(
    page.getByRole("heading", { name: "How it found you" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Back to from your feeds" }).click();
  await expect(page).toHaveURL(timeline);
  await expect(
    page.getByRole("searchbox", {
      name: "Search From your feeds",
      exact: true,
    }),
  ).toHaveValue("cities");
  await page.goForward();
  await expect(
    page.getByRole("heading", { name: "How it found you" }),
  ).toBeVisible();
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "How it found you" }),
  ).toBeVisible();
  expect(
    state.requests.some((url) => url.searchParams.get("id") === first.id),
  ).toBe(true);
});

test("configured feeds and channels are selectable and their disclosure is keyboard accessible", async ({
  page,
}) => {
  await fixture(page);
  await page.goto("/reader/?view=youtube");
  const nav = page.getByRole("navigation", { name: "Main navigation" });
  await nav
    .getByRole("button", { name: "Collapse YouTube feeds" })
    .press("Enter");
  await expect(page.locator("#youtube-feeds")).toBeHidden();
  await nav
    .getByRole("button", { name: "Expand YouTube feeds" })
    .press("Space");
  await nav
    .getByRole("button", { name: "Channel 4 News, 8 discoveries", exact: true })
    .click();
  await expect(page).toHaveURL(/channel=channel4/);
  await expect(
    page.getByRole("heading", { name: "Channel 4 News", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Shorts", exact: true }).click();
  await expect(page.locator(".yt-card")).toHaveCount(3);
  expect(new URL(page.url()).searchParams.get("format")).toBe("short");
  await nav
    .getByRole("button", {
      name: "News · Business, 0 discoveries",
      exact: true,
    })
    .click();
  await expect(
    page.getByRole("heading", { name: "Your next discovery starts here" }),
  ).toBeVisible();
  expect(new URL(page.url()).searchParams.has("format")).toBe(false);
});

test("pagination errors retain discoveries and retry the same collection", async ({
  page,
}) => {
  const state = await fixture(page);
  state.pageSize = 2;
  await page.addInitScript(() => {
    Reflect.deleteProperty(window, "IntersectionObserver");
  });
  await page.goto("/reader/?view=youtube");
  await expect(page.locator(".yt-card")).toHaveCount(2);
  state.failMore = true;
  await page.getByRole("button", { name: "More discoveries" }).click();
  await expect(page.getByRole("alert")).toContainText(
    "Collection temporarily unavailable",
  );
  await expect(page.locator(".yt-card")).toHaveCount(2);
  state.failMore = false;
  await page.getByRole("button", { name: "Try again" }).click();
  await expect(page.locator(".yt-card")).toHaveCount(4);
});

test("empty, failed and unconfigured collections are explicit", async ({
  page,
}) => {
  const state = await fixture(page);
  state.data = [];
  state.sources = { feeds: [], channels: [], total: 0, linked_count: 0 };
  await page.goto("/reader/?view=youtube");
  await expect(
    page.getByRole("heading", { name: "Your next discovery starts here" }),
  ).toBeVisible();
  await page
    .getByRole("button", {
      name: "From your feeds, 0 discoveries",
      exact: true,
    })
    .click();
  await expect(
    page.getByRole("heading", {
      name: "Your feeds haven't linked a video yet",
    }),
  ).toBeVisible();
  state.fail = true;
  await page
    .getByRole("button", { name: "Refresh YouTube collection" })
    .click();
  await expect(page.getByRole("alert")).toContainText(
    "Collection temporarily unavailable",
  );
  state.fail = false;
  state.data = videos;
  await page.getByRole("button", { name: "Try again" }).click();
  await expect(page.locator(".yt-card")).toHaveCount(1);
});

for (const width of [1440, 390, 320]) {
  test(`YouTube shelves, discovery details and navigation fit at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 1000 });
    await fixture(page);
    await page.goto("/reader/?view=youtube");
    await expect(page.locator(".yt-card")).toHaveCount(8);
    await noOverflow(page);
    await page.screenshot({
      path: testInfo.outputPath(`youtube-feed-${width}.png`),
      fullPage: true,
    });
    await page.getByRole("link", { name: first.title, exact: true }).click();
    await expect(
      page.getByRole("heading", { name: "How it found you" }),
    ).toBeVisible();
    await noOverflow(page);
    await page.screenshot({
      path: testInfo.outputPath(`youtube-detail-${width}.png`),
      fullPage: true,
    });
    if (width < 600)
      await page
        .getByRole("button", { name: "Open navigation", exact: true })
        .click();
    await page
      .getByRole("navigation", { name: "Main navigation" })
      .getByRole("button", {
        name: "News · Top stories, 7 discoveries",
        exact: true,
      })
      .click();
    await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(
      0,
    );
    await expect(page.locator(".yt-card")).toHaveCount(7);
    await noOverflow(page);
  });
}

test("downloaded videos play locally on demand and preserve position in discovery details", async ({
  page,
}) => {
  const state = await fixture(page);
  state.data = [{ ...first, video_url: "/assets/youtube-local.mp4" }];
  let requests = 0;
  const mp4 = readFileSync(
    new URL("./fixtures/reddit-video/archived.mp4", import.meta.url),
  );
  await page.route("**/assets/youtube-local.mp4", (route) => {
    requests++;
    const range = /^bytes=(\d+)-(\d*)$/.exec(
      route.request().headers().range ?? "",
    );
    const start = range ? Number(range[1]) : 0;
    const end = range?.[2]
      ? Math.min(Number(range[2]), mp4.length - 1)
      : mp4.length - 1;

    return route.fulfill({
      status: range ? 206 : 200,
      contentType: "video/mp4",
      headers: {
        "Accept-Ranges": "bytes",
        ...(range
          ? { "Content-Range": `bytes ${start}-${end}/${mp4.length}` }
          : {}),
      },
      body: mp4.subarray(start, end + 1),
    });
  });
  await page.goto("/reader/?view=youtube");
  await expect(page.getByText("Downloaded", { exact: true })).toBeVisible();
  expect(requests).toBe(0);
  await expect(page.locator("video, iframe")).toHaveCount(0);
  await page
    .getByRole("button", { name: `Play ${first.title}`, exact: true })
    .click();
  const video = page.getByLabel(`Downloaded video: ${first.title}`);
  await expect
    .poll(() =>
      video.evaluate((element) => (element as HTMLVideoElement).readyState),
    )
    .toBeGreaterThan(1);
  await video.evaluate((element) => {
    const video = element as HTMLVideoElement;
    video.pause();
    video.currentTime = 0.5;
  });
  await expect
    .poll(() =>
      video.evaluate((element) => (element as HTMLVideoElement).seeking),
    )
    .toBe(false);
  await page.getByRole("link", { name: first.title, exact: true }).click();
  await page
    .getByRole("button", { name: `Play ${first.title}`, exact: true })
    .click();
  await expect
    .poll(() =>
      video.evaluate((element) => (element as HTMLVideoElement).currentTime),
    )
    .toBeGreaterThanOrEqual(0.5);
  await expect(page.locator("iframe")).toHaveCount(0);
  await expect(page.getByText("Playing downloaded video")).toBeVisible();
});

test("failed local video offers explicit remote playback and community links remain safe", async ({
  page,
}) => {
  const state = await fixture(page);
  state.data = [
    { ...first, video_url: "/assets/missing-video.mp4" },
    {
      ...videos[7],
      text: "Read the report at https://news.example.com/story?topic=world.\nDo not interpret <script>alert(1)</script> or javascript:alert(1) as markup.",
    },
  ];
  await page.route("**/assets/missing-video.mp4", (route) =>
    route.fulfill({ status: 404 }),
  );
  await page.goto("/reader/?view=youtube");
  await expect(page.locator(".yt-post-text a")).toHaveAttribute(
    "href",
    "https://news.example.com/story?topic=world",
  );
  await expect(page.locator(".yt-post-text script")).toHaveCount(0);
  await expect(page.locator(".yt-post-text")).toContainText(
    "<script>alert(1)</script>",
  );
  await page
    .getByRole("button", { name: `Play ${first.title}`, exact: true })
    .click();
  await expect(
    page.getByText("The downloaded video couldn't be played."),
  ).toBeVisible();
  await expect(page.locator("iframe")).toHaveCount(0);
  await page
    .getByRole("button", { name: "Play from YouTube", exact: true })
    .click();
  await expect(page.locator("iframe")).toHaveCount(1);
});

test("raw-only source references open their original and processed references retain a separate original link", async ({
  page,
  context,
}) => {
  const state = await fixture(page);
  const raw = {
    ...first.references[0],
    reader_url: "",
    title: "A newly collected source post",
  };
  state.data = [{ ...first, references: [raw, first.references[1]] }];
  await context.route("https://x.com/reporter/status/123", (route) =>
    route.fulfill({
      contentType: "text/html",
      body: "<p>Original source fixture</p>",
    }),
  );
  await page.goto(`/reader/?view=youtube&feed=linked&youtube_item=${first.id}`);
  const references = page.getByRole("region", {
    name: "Linked from collected posts",
  });
  const rawLink = references.getByRole("link", {
    name: "X A newly collected source post",
    exact: true,
  });
  await expect(rawLink).toHaveAttribute("href", raw.url);
  await expect(rawLink).toHaveAttribute("target", "_blank");
  const originalPromise = page.waitForEvent("popup");
  await rawLink.click();
  const original = await originalPromise;
  await expect(original).toHaveURL(raw.url);
  await original.close();
  expect(
    state.requests.some(
      (url) =>
        url.pathname.endsWith("/post") &&
        url.searchParams.get("id") === raw.post_id,
    ),
  ).toBe(false);
  await expect(
    references.getByRole("link", {
      name: "Open original source: Urban design worth watching",
      exact: true,
    }),
  ).toHaveAttribute("href", first.references[1].url);
  await references
    .getByRole("link", {
      name: "lobste.rs Urban design worth watching",
      exact: true,
    })
    .click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await page.goBack();
  await expect(references).toBeVisible();
  expect(new URL(page.url()).searchParams.get("feed")).toBe("linked");
});
