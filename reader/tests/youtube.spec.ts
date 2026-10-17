import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import AxeBuilder from "@axe-core/playwright";
import { type BrowserContext, expect, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const videoUrl = "https://youtu.be/LKrwhjOI8Nc?t=90";
const videoTitle = "There's never been a better time to go into math";
const postText = "Some reflections on AI in math.";
const post = {
  id: "youtube-post",
  domain: "x.com",
  source: "x",
  url: "https://x.com/3blue1brown/status/2111135480116850944",
  author: "Grant Sanderson",
  title: postText,
  excerpt: postText,
  created_at: "2026-10-16T17:41:00Z",
  html: `<p>${postText}</p><p><a href="${videoUrl}">${videoUrl}</a></p>`,
  media: [],
  thumbnail: null,
  x: {
    handle: "3blue1brown",
    name: "Grant Sanderson",
    icon_url: null,
    links: [
      {
        url: videoUrl,
        title: videoTitle,
        description: "Thoughts on the future of math.",
        image_url: "/assets/youtube-thumb.svg",
      },
    ],
    media: [],
    reply_count: 24,
    repost_count: 136,
    like_count: 931,
    view_count: 54000,
    replies_status: "not_collected",
    captured_at: "2026-10-17T00:00:00Z",
  },
} satisfies Post;

async function mockYouTube(
  context: BrowserContext,
  {
    brokenImage = false,
    posts = [post],
    delayedReady = false,
  }: { brokenImage?: boolean; posts?: Post[]; delayedReady?: boolean } = {},
) {
  const playerFixture = readFileSync(
    new URL("./fixtures/youtube-player.html", import.meta.url),
    "utf8",
  );
  const embeds: { url: string; referer: string | undefined }[] = [];
  await context.route(
    "https://www.youtube-nocookie.com/embed/**",
    async (route) => {
      embeds.push({
        url: route.request().url(),
        referer: route.request().headers().referer,
      });
      await route.fulfill({
        contentType: "text/html",
        body: delayedReady
          ? playerFixture.replace('data-ready="true"', 'data-ready="false"')
          : playerFixture,
      });
    },
  );
  await context.route("https://youtu.be/**", (route) =>
    route.fulfill({ body: "YouTube destination" }),
  );
  await context.route("**/assets/youtube-thumb.svg", (route) =>
    route.fulfill(
      brokenImage
        ? { status: 404 }
        : {
            contentType: "image/svg+xml",
            body: '<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180"><rect width="320" height="180" fill="#538573"/></svg>',
          },
    ),
  );
  await context.route("**/assets/playback-*.mp4", (route) =>
    route.fulfill({
      contentType: "video/mp4",
      path: fileURLToPath(
        new URL("./fixtures/reddit-video/archived.mp4", import.meta.url),
      ),
    }),
  );
  await context.route("https://video.reddit.test/**", (route) => {
    const name = new URL(route.request().url()).pathname.slice(1);

    return route.fulfill({
      path: fileURLToPath(
        new URL(`./fixtures/reddit-video/${name}`, import.meta.url),
      ),
      contentType: name.endsWith("m3u8")
        ? "application/vnd.apple.mpegurl"
        : name.endsWith("mp4")
          ? "video/mp4"
          : "video/mp2t",
      headers: { "Access-Control-Allow-Origin": "*" },
    });
  });
  await context.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());

    if (url.pathname.endsWith("/sources")) {
      await route.fulfill({
        json: {
          sources: [
            {
              domain: "x.com",
              name: "X",
              kind: "x",
              count: posts.filter((item) => item.domain === "x.com").length,
              tags: [],
              accounts: [
                {
                  ...post.x,
                  count: posts.filter((item) => item.domain === "x.com").length,
                },
              ],
            },
            ...(posts.some((item) => item.reddit)
              ? [
                  {
                    domain: "reddit.com",
                    name: "Reddit",
                    kind: "reddit",
                    tags: [],
                    count: posts.filter((item) => item.reddit).length,
                    communities: [
                      {
                        name: "videos",
                        count: posts.filter((item) => item.reddit).length,
                      },
                    ],
                  },
                ]
              : []),
          ],
          total_posts: posts.length,
        },
      });
      return;
    }

    await route.fulfill({
      json: url.pathname.endsWith("/post")
        ? posts.find((item) => item.id === url.searchParams.get("id"))
        : { posts, next_cursor: null, limited: false },
    });
  });

  return embeds;
}

for (const width of [1440, 390, 320]) {
  test(`YouTube previews play inline in All, X and the reader at ${width}px`, async ({
    page,
    context,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    const embeds = await mockYouTube(context);

    for (const query of [
      "",
      "view=x&account=3blue1brown",
      "view=x&article=youtube-post&article_source=x.com",
    ]) {
      await page.goto(`/reader/?${query}`);
      const content = page.locator(".x-post-content:visible");
      const trigger = content.getByRole("button", {
        name: `Play video: ${videoTitle}`,
      });
      const originalUrl = page.url();
      await expect(trigger).toBeVisible();
      await expect(content.locator("iframe")).toHaveCount(0);
      await expect(content.locator(".article-body")).not.toContainText(
        videoUrl,
      );
      const before = embeds.length;
      await trigger.click();
      const player = content.locator("iframe");
      await expect(player).toBeVisible();
      await expect(player).toHaveAttribute(
        "title",
        `YouTube video: ${videoTitle}`,
      );
      await expect(player).toHaveAttribute(
        "src",
        `https://www.youtube-nocookie.com/embed/LKrwhjOI8Nc?autoplay=0&enablejsapi=1&origin=${encodeURIComponent(new URL(originalUrl).origin)}&playsinline=1&start=90`,
      );
      await expect(player).toHaveAttribute("allowfullscreen", "");
      await expect(
        page
          .frameLocator(".x-post-content:visible iframe")
          .getByRole("button", { name: "Pause video" }),
      ).toBeVisible();
      expect(embeds.length).toBe(before + 1);
      expect(embeds.at(-1)?.referer).toBe(`${new URL(originalUrl).origin}/`);
      await expect(page).toHaveURL(originalUrl);
      expect(context.pages()).toHaveLength(1);
      expect((await player.boundingBox())?.height).toBeGreaterThanOrEqual(200);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);
      await expect(
        content.getByRole("link", { name: "Watch on YouTube" }),
      ).toHaveAttribute("href", videoUrl);
      if (query.includes("article=")) {
        const resize = page.getByRole("button", { name: "Larger post text" });
        await resize.click();
        await expect(resize).toBeFocused();
        expect(embeds.length).toBe(before + 1);
        await expect(player).toBeVisible();
      }

      await page.screenshot({
        path: testInfo.outputPath(
          `youtube-${width}-${query ? "x" : "all"}.png`,
        ),
        fullPage: true,
      });
      await content.getByRole("button", { name: "Close video" }).click();
      await expect(player).toHaveCount(0);
      await expect(trigger).toBeFocused();
    }

    expect(
      (await new AxeBuilder({ page }).analyze()).violations.filter((item) =>
        ["serious", "critical"].includes(item.impact ?? ""),
      ),
    ).toEqual([]);
  });
}

test("keyboard activation works without a thumbnail, and the external fallback remains available", async ({
  page,
  context,
}) => {
  const embeds = await mockYouTube(context, { brokenImage: true });
  await page.goto("/reader/?view=x");
  const preview = page.locator(".x-youtube-preview");
  const trigger = preview.getByRole("button", {
    name: `Play video: ${videoTitle}`,
  });
  await expect(trigger.locator("img")).toHaveCount(0);
  expect(embeds).toHaveLength(0);

  for (const key of ["Enter", "Space"]) {
    await trigger.focus();
    await trigger.press(key);
    await expect(preview.locator("iframe")).toBeVisible();
    await preview.getByRole("button", { name: "Close video" }).click();
    await expect(trigger).toBeFocused();
  }

  const [tab] = await Promise.all([
    context.waitForEvent("page"),
    preview.getByRole("link", { name: "Watch on YouTube" }).click(),
  ]);
  await tab.waitForURL(videoUrl);
  expect(await tab.evaluate(() => window.opener === null)).toBe(true);
  await tab.close();
});

test("opening a post and going Back unloads players instead of leaving audio in the hidden feed", async ({
  page,
  context,
}) => {
  await mockYouTube(context);
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  await card.getByRole("button", { name: `Play video: ${videoTitle}` }).click();
  await expect(card.locator("iframe")).toBeVisible();
  await card.getByText(postText, { exact: true }).click();
  const reader = page.getByRole("main", { name: "X post reader" });
  await expect(reader).toBeVisible();
  await expect(page.locator("iframe")).toHaveCount(0);
  await reader
    .getByRole("button", { name: `Play video: ${videoTitle}` })
    .click();
  await expect(reader.locator("iframe")).toBeVisible();
  await page.goBack();
  await expect(card).toBeVisible();
  await expect(page.locator("iframe")).toHaveCount(0);
  await expect(
    card.getByRole("button", { name: `Play video: ${videoTitle}` }),
  ).toBeVisible();
});

const mixed: Post = {
  ...post,
  x: {
    ...post.x,
    links: [
      ...post.x.links,
      {
        ...post.x.links[0],
        url: "https://youtu.be/M7lc1UVf-VE",
        title: "Second video",
      },
    ],
    media: [1, 2].map((index) => ({
      type: "video" as const,
      url: `/assets/playback-${index}.mp4`,
      poster_url: null,
      alt: "",
      remote: false,
      aspect_ratio: 16 / 9,
    })),
  },
};

for (const width of [1440, 390]) {
  test(`scrolling out pauses native and YouTube videos without resuming or resetting at ${width}px`, async ({
    page,
    context,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await mockYouTube(context, {
      posts: Array.from({ length: 5 }, (_, index) => ({
        ...mixed,
        id: `mixed-${index}`,
      })),
    });
    await page.goto("/reader/?view=x");
    const card = page.locator(".post-card").first();
    const video = card.locator("video").first();
    await video.scrollIntoViewIfNeeded();
    await video.evaluate(async (v: HTMLVideoElement) => {
      v.muted = true;
      v.loop = true;
      await v.play();
    });
    await expect
      .poll(() => video.evaluate((v: HTMLVideoElement) => v.currentTime))
      .toBeGreaterThan(0.1);
    await video.evaluate((v) =>
      window.scrollBy(0, v.getBoundingClientRect().bottom + 20),
    );
    await expect(video).toHaveJSProperty("paused", true);
    const time = await video.evaluate((v: HTMLVideoElement) => v.currentTime);
    expect(time).toBeGreaterThan(0);
    await video.scrollIntoViewIfNeeded();
    await expect(video).toHaveJSProperty("paused", true);
    expect(await video.evaluate((v: HTMLVideoElement) => v.currentTime)).toBe(
      time,
    );
    await video.evaluate((v: HTMLVideoElement) => v.play());
    await expect(video).toHaveJSProperty("paused", false);

    await card
      .getByRole("button", { name: `Play video: ${videoTitle}` })
      .click();
    const frame = card.locator("iframe");
    const player = frame.contentFrame();
    await expect(player.locator("body")).toHaveAttribute("data-state", "1");
    await expect(video).toHaveJSProperty("paused", true);
    const identity = await frame.getAttribute("id");
    await frame.evaluate((f) =>
      window.scrollBy(0, f.getBoundingClientRect().bottom + 20),
    );
    await expect(player.locator("body")).toHaveAttribute("data-state", "2");
    await frame.scrollIntoViewIfNeeded();
    await expect(player.locator("body")).toHaveAttribute("data-state", "2");
    await expect(frame).toHaveAttribute("id", identity ?? "");
    await player
      .getByRole("button", { name: "Play video", exact: true })
      .click();
    await expect(player.locator("body")).toHaveAttribute("data-state", "1");
  });
}

test("starting any player pauses the previous native or YouTube player, including resuming iframe controls", async ({
  page,
  context,
}) => {
  await page.setViewportSize({ width: 1000, height: 3000 });
  await mockYouTube(context, { posts: [mixed] });
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  const videos = card.locator("video");
  await expect(videos).toHaveCount(2);

  for (const video of await videos.all()) {
    await video.evaluate(async (v: HTMLVideoElement) => {
      v.muted = true;
      v.loop = true;
      await v.play();
    });
    await expect
      .poll(() => video.evaluate((v: HTMLVideoElement) => v.currentTime))
      .toBeGreaterThan(0.1);
  }

  await expect(videos.nth(0)).toHaveJSProperty("paused", true);
  await expect(videos.nth(1)).toHaveJSProperty("paused", false);
  // A delayed play notification from an already-paused element cannot take ownership.
  await videos.nth(0).evaluate((v) => v.dispatchEvent(new Event("play")));
  await expect(videos.nth(1)).toHaveJSProperty("paused", false);
  await card.getByRole("button", { name: `Play video: ${videoTitle}` }).click();
  const first = card.locator("iframe").first().contentFrame();
  await expect(first.locator("body")).toHaveAttribute("data-state", "1");
  await expect(videos.nth(1)).toHaveJSProperty("paused", true);
  await card.getByRole("button", { name: "Play video: Second video" }).click();
  const second = card.locator("iframe").nth(1).contentFrame();
  await expect(second.locator("body")).toHaveAttribute("data-state", "1");
  await expect(first.locator("body")).toHaveAttribute("data-state", "2");
  await videos.nth(0).evaluate((v: HTMLVideoElement) => v.play());
  await expect(second.locator("body")).toHaveAttribute("data-state", "2");
  await first.getByRole("button", { name: "Play video", exact: true }).click();
  await expect(first.locator("body")).toHaveAttribute("data-state", "1");
  await expect(videos.nth(0)).toHaveJSProperty("paused", true);
});

test("a slow YouTube player cannot start over a newer video, but can be resumed manually", async ({
  page,
  context,
}) => {
  await page.setViewportSize({ width: 1000, height: 2400 });
  await mockYouTube(context, { posts: [mixed], delayedReady: true });
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  await card.getByRole("button", { name: `Play video: ${videoTitle}` }).click();
  const player = card.locator("iframe").contentFrame();
  await expect(player.locator("body")).toHaveAttribute("data-ready", "false");
  const video = card.locator("video").first();
  await video.evaluate(async (v: HTMLVideoElement) => {
    v.muted = true;
    v.loop = true;
    await v.play();
  });
  await expect(video).toHaveJSProperty("paused", false);
  await player
    .locator("body")
    .evaluate(() => window.dispatchEvent(new Event("fixture-ready")));
  await expect(player.locator("body")).toHaveAttribute("data-ready", "true");
  await expect(player.locator("body")).toHaveAttribute(
    "data-subscribed",
    "true",
  );
  await expect(player.locator("body")).toHaveAttribute("data-state", "-1");
  await expect(video).toHaveJSProperty("paused", false);
  await player.getByRole("button", { name: "Play video", exact: true }).click();
  await expect(video).toHaveJSProperty("paused", true);
});

test("native playback stops on navigation and stays paused in the restored feed", async ({
  page,
  context,
}) => {
  await mockYouTube(context, { posts: [mixed] });
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  const video = card.locator("video").first();
  await video.scrollIntoViewIfNeeded();
  await video.evaluate(async (v: HTMLVideoElement) => {
    v.muted = true;
    v.loop = true;
    await v.play();
  });
  await expect(video).toHaveJSProperty("paused", false);
  await card.getByText(postText, { exact: true }).click();
  await expect(page.getByRole("main", { name: "X post reader" })).toBeVisible();
  await expect(video).toHaveJSProperty("paused", true);
  await page.goBack();
  await expect(card).toBeVisible();
  await expect(video).toHaveJSProperty("paused", true);
});

function redditPost(remote = false, id = "reddit-video"): Post {
  const video = {
    url: remote
      ? "https://video.reddit.test/playlist.m3u8"
      : "/assets/playback-reddit.mp4",
    hls_url: remote ? "https://video.reddit.test/playlist.m3u8" : null,
    fallback_url: remote ? "https://video.reddit.test/archived.mp4" : null,
    poster_url: "/assets/youtube-thumb.svg",
    remote,
  };

  return {
    ...post,
    id,
    domain: "reddit.com",
    source: "reddit",
    x: undefined,
    url: `https://www.reddit.com/r/videos/comments/${id}/video/`,
    title: `Reddit clip ${id}`,
    html: "",
    excerpt: "",
    thumbnail: video.poster_url,
    reddit: {
      community: "videos",
      score: 12,
      comments: 3,
      flair: null,
      video,
      videos: [video],
    },
  };
}

for (const remote of [false, true]) {
  test(`All coordinates X, YouTube and ${remote ? "streamed" : "local"} Reddit videos in both directions`, async ({
    page,
    context,
  }) => {
    await page.setViewportSize({ width: 1000, height: 3000 });
    await mockYouTube(context, {
      posts: [mixed, redditPost(remote), redditPost(false, "second-reddit")],
    });
    await page.goto("/reader/");
    const reddit = page.locator(".reddit-card");
    await expect(reddit).toHaveCount(2);
    await expect(reddit.first().locator(".thumbnail")).toHaveCount(0);
    await expect(
      reddit.first().getByText("Not collected", { exact: true }),
    ).toHaveCount(0);
    const videos = reddit.locator("video");
    await expect(videos).toHaveCount(2);

    for (const video of await videos.all()) {
      await expect(video).not.toHaveAttribute("src");
      await video.evaluate((v: HTMLVideoElement) => {
        v.volume = 0;
        v.loop = true;
      });
    }

    const x = page.locator(".x-post-content video").first();
    await x.evaluate(async (v: HTMLVideoElement) => {
      v.muted = true;
      v.loop = true;
      await v.play();
    });
    await expect(x).toHaveJSProperty("paused", false);
    await reddit
      .first()
      .getByRole("button", { name: "Play video", exact: true })
      .click();
    await expect
      .poll(() =>
        videos.first().evaluate((v: HTMLVideoElement) => v.currentTime),
      )
      .toBeGreaterThan(0.1);
    await expect(x).toHaveJSProperty("paused", true);
    await page
      .getByRole("button", { name: `Play video: ${videoTitle}` })
      .click();
    const youtube = page.locator("iframe").contentFrame();
    await expect(youtube.locator("body")).toHaveAttribute("data-state", "1");
    await expect(videos.first()).toHaveJSProperty("paused", true);
    await videos.first().evaluate((v: HTMLVideoElement) => v.play());
    await expect(youtube.locator("body")).toHaveAttribute("data-state", "2");
    await reddit
      .nth(1)
      .getByRole("button", { name: "Play video", exact: true })
      .click();
    await expect
      .poll(() =>
        videos.nth(1).evaluate((v: HTMLVideoElement) => v.currentTime),
      )
      .toBeGreaterThan(0.1);
    await expect(videos.first()).toHaveJSProperty("paused", true);
    await x.evaluate((v: HTMLVideoElement) => v.play());
    await expect(videos.nth(1)).toHaveJSProperty("paused", true);
    await expect(page).toHaveURL(/\/reader\/$/);
  });
}

for (const width of [1440, 390]) {
  test(`Reddit pauses offscreen and across reader navigation while keeping its feed position at ${width}px`, async ({
    page,
    context,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    const posts = Array.from({ length: 5 }, (_, i) =>
      redditPost(false, `clip-${i}`),
    );
    await mockYouTube(context, { posts });
    await page.goto("/reader/");
    const card = page.locator(".reddit-card").first();
    const video = card.locator("video");
    await video.evaluate((v: HTMLVideoElement) => {
      v.volume = 0;
      v.loop = true;
    });
    await card.getByRole("button", { name: "Play video", exact: true }).click();
    await expect
      .poll(() => video.evaluate((v: HTMLVideoElement) => v.currentTime))
      .toBeGreaterThan(0.1);
    await video.evaluate((v) =>
      window.scrollBy(0, v.getBoundingClientRect().bottom + 20),
    );
    await expect(video).toHaveJSProperty("paused", true);
    const position = await video.evaluate(
      (v: HTMLVideoElement) => v.currentTime,
    );
    await video.scrollIntoViewIfNeeded();
    await expect(video).toHaveJSProperty("paused", true);
    expect(await video.evaluate((v: HTMLVideoElement) => v.currentTime)).toBe(
      position,
    );
    await video.evaluate((v: HTMLVideoElement) => v.play());
    await card
      .getByRole("link", { name: `Read ${posts[0].title}`, exact: true })
      .click();
    await expect(
      page.getByRole("main", { name: "Article reader" }),
    ).toBeVisible();
    await expect(video).toHaveJSProperty("paused", true);
    const opened = page.locator(".article-page .reddit-video video");
    await opened.evaluate((v: HTMLVideoElement) => {
      v.volume = 0;
    });
    await page
      .locator(".article-page")
      .getByRole("button", { name: "Play video", exact: true })
      .click();
    await expect
      .poll(() => opened.evaluate((v: HTMLVideoElement) => v.currentTime))
      .toBeGreaterThan(0.1);
    await page.goBack();
    await expect(opened).toHaveCount(0);
    await expect(video).toHaveJSProperty("paused", true);
  });
}

for (const result of ["manifest", "fallback"] as const) {
  test(`a delayed Reddit ${result} cannot interrupt a newer video`, async ({
    page,
    context,
  }) => {
    await page.setViewportSize({ width: 1000, height: 2400 });
    await mockYouTube(context, { posts: [mixed, redditPost(true)] });
    let release = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    await context.route(
      "https://video.reddit.test/playlist.m3u8",
      async (route) => {
        await gate;
        if (result === "fallback")
          await route.fulfill({
            status: 404,
            headers: { "Access-Control-Allow-Origin": "*" },
          });
        else await route.fallback();
      },
    );
    await page.goto("/reader/");
    const card = page.locator(".reddit-card");
    const video = card.locator("video");
    await video.evaluate((v: HTMLVideoElement) => {
      v.volume = 0;
      v.loop = true;
    });
    const request = page.waitForRequest(
      "https://video.reddit.test/playlist.m3u8",
    );
    await card.getByRole("button", { name: "Play video", exact: true }).click();
    await request;
    const x = page.locator(".x-post-content video").first();
    await x.evaluate(async (v: HTMLVideoElement) => {
      v.muted = true;
      v.loop = true;
      await v.play();
    });
    release();
    if (result === "fallback") {
      // preload=none keeps a cancelled fallback unloaded until the user resumes.
      await expect(video).toHaveAttribute(
        "src",
        "https://video.reddit.test/archived.mp4",
        { timeout: 15000 },
      );
    } else {
      await expect
        .poll(() => video.evaluate((v: HTMLVideoElement) => v.readyState), {
          timeout: 15000,
        })
        .toBeGreaterThanOrEqual(2);
    }
    await expect(video).toHaveJSProperty("paused", true);
    await expect(x).toHaveJSProperty("paused", false);
    await video.evaluate((v: HTMLVideoElement) => v.play());
    await expect(video).toHaveJSProperty("paused", false);
    await expect(x).toHaveJSProperty("paused", true);
  });
}
