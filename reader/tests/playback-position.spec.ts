import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  type BrowserContext,
  expect,
  type Locator,
  test,
} from "@playwright/test";
import type { Post } from "../src/lib/types";

const x = {
  id: "x-clip",
  domain: "x.com",
  source: "x",
  url: "https://x.com/filmmaker/status/123",
  author: "Filmmaker",
  title: "The X clip",
  excerpt: "The X clip",
  html: "<p>The X clip</p>",
  created_at: "2026-10-17T00:00:00Z",
  thumbnail: null,
  media: [],
  x: {
    handle: "filmmaker",
    name: "Filmmaker",
    icon_url: null,
    links: [],
    media: [
      {
        type: "video",
        url: "/assets/position-one.mp4",
        source_url: "https://x.com/filmmaker/status/123/video/1",
        poster_url: null,
        alt: "",
        remote: false,
        aspect_ratio: 16 / 9,
      },
    ],
    replies_status: "not_collected",
    reply_count: 0,
  },
} satisfies Post;
const youtube: Post = {
  ...x,
  id: "youtube-clip",
  title: "The YouTube clip",
  html: "<p>The YouTube clip</p>",
  x: {
    ...x.x,
    handle: "filmmaker",
    name: "Filmmaker",
    icon_url: null,
    media: [],
    links: [
      {
        url: "https://youtu.be/LKrwhjOI8Nc?t=90",
        title: "A YouTube film",
        description: "",
        image_url: null,
      },
    ],
  },
};
function reddit(remote = false) {
  return {
    ...x,
    id: "reddit-clip",
    domain: "reddit.com",
    source: "reddit",
    url: "https://www.reddit.com/r/videos/comments/clip/film/",
    title: "The Reddit clip",
    html: "<p>The Reddit clip</p>",
    x: undefined,
    reddit: {
      community: "videos",
      score: 1,
      comments: 0,
      flair: null,
      video: {
        url: remote
          ? "https://video.reddit.test/playlist.m3u8"
          : "/assets/position-one.mp4",
        source_url: "https://v.redd.it/captured/HLSPlaylist.m3u8?signature=old",
        hls_url: remote ? "https://video.reddit.test/playlist.m3u8" : null,
        poster_url: null,
        remote,
      },
    },
  } satisfies Post;
}
async function mock(context: BrowserContext, posts: Post[], details = posts) {
  const requests: string[] = [];
  const mp4 = readFileSync(
    new URL("./fixtures/reddit-video/archived.mp4", import.meta.url),
  );
  await context.route("**/assets/position-*.mp4", (route) => {
    requests.push(new URL(route.request().url()).pathname);
    // Match aiohttp FileResponse: Chrome requires range support for MP4 seeking.
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
  await context.route("https://video.reddit.test/**", (route) => {
    requests.push(new URL(route.request().url()).pathname);
    const file = new URL(route.request().url()).pathname.slice(1);
    return route.fulfill({
      contentType: file.endsWith("m3u8")
        ? "application/vnd.apple.mpegurl"
        : "video/mp2t",
      headers: { "Access-Control-Allow-Origin": "*" },
      path: fileURLToPath(
        new URL(`./fixtures/reddit-video/${file}`, import.meta.url),
      ),
    });
  });
  await context.route("https://www.youtube-nocookie.com/embed/**", (route) =>
    route.fulfill({
      contentType: "text/html",
      body: readFileSync(
        new URL("./fixtures/youtube-player.html", import.meta.url),
        "utf8",
      ),
    }),
  );
  await context.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());
    return route.fulfill({
      json: url.pathname.endsWith("/sources")
        ? {
            sources: [
              {
                domain: "x.com",
                name: "X",
                kind: "x",
                tags: [],
                count: posts.length,
                accounts: [
                  {
                    handle: "filmmaker",
                    name: "Filmmaker",
                    icon_url: null,
                    count: posts.length,
                  },
                ],
              },
              {
                domain: "reddit.com",
                name: "Reddit",
                kind: "reddit",
                tags: [],
                count: posts.length,
                communities: [{ name: "videos", count: posts.length }],
              },
            ],
            total_posts: posts.length,
          }
        : url.pathname.endsWith("/post")
          ? details.find(
              (p) =>
                p.id === url.searchParams.get("id") &&
                p.domain === url.searchParams.get("domain"),
            )
          : { posts, next_cursor: null, limited: false },
    });
  });
  return requests;
}
async function play(video: Locator, scope: Locator) {
  await video.scrollIntoViewIfNeeded();
  await video.evaluate((v: HTMLVideoElement) => {
    v.volume = 0;
  });
  const trigger = scope.getByRole("button", {
    name: "Play video",
    exact: true,
  });
  if (await trigger.count()) await trigger.click();
  else await video.evaluate((v: HTMLVideoElement) => v.play());
  await expect(video).toHaveJSProperty("paused", false);
  await expect
    .poll(() => video.evaluate((v: HTMLVideoElement) => v.readyState))
    .toBeGreaterThanOrEqual(2);
  await expect(video).toHaveJSProperty("seeking", false);
}
async function seekAndPause(video: Locator, time: number) {
  await video.evaluate((v: HTMLVideoElement, time) => {
    v.currentTime = time;
    v.pause();
  }, time);
  await expect(video).toHaveJSProperty("seeking", false);
  await expect(video).toHaveJSProperty("paused", true);
  await expectTime(video, time);
}
async function expectTime(video: Locator, time: number) {
  await expect
    .poll(() => video.evaluate((v: HTMLVideoElement) => v.currentTime))
    .toBeGreaterThanOrEqual(time - 0.08);
  await expect
    .poll(() => video.evaluate((v: HTMLVideoElement) => v.currentTime))
    .toBeLessThan(time + 0.35);
}

for (const width of [1440, 390]) {
  for (const kind of ["x", "reddit-local", "reddit-hls"] as const) {
    test(`${kind} retains newer reader progress and backward seeks across Back and Forward at ${width}px`, async ({
      page,
      context,
    }) => {
      await page.setViewportSize({ width, height: 1000 });
      const post = kind === "x" ? x : reddit(kind === "reddit-hls");
      const detail: Post =
        kind === "x"
          ? {
              ...x,
              x: {
                ...x.x,
                media: x.x.media.map((m) => ({
                  ...m,
                  url: "/assets/position-detail.mp4",
                })),
              },
            }
          : {
              ...post,
              reddit: {
                ...reddit(kind === "reddit-hls").reddit,
                video: {
                  ...reddit(kind === "reddit-hls").reddit.video,
                  source_url:
                    "https://v.redd.it/captured/HLSPlaylist.m3u8?signature=new",
                },
              },
            };
      const requests = await mock(context, [post], [detail]);
      await page.goto("/reader/");
      const card = page.locator(".post-card");
      const feed = card.locator("video");
      await play(feed, card);
      await expect
        .poll(() => feed.evaluate((v: HTMLVideoElement) => v.currentTime))
        .toBeGreaterThan(0.1);
      await seekAndPause(feed, 1.25);
      const before = requests.length;
      await card
        .getByRole("link", { name: `Read ${post.title}`, exact: true })
        .click();
      const reader = page.locator(".article-page");
      const opened = reader.locator("video");
      await expect(opened).toHaveJSProperty("paused", true);
      expect(requests.length).toBe(before);
      await play(opened, reader);
      await expectTime(opened, 1.25);
      await seekAndPause(opened, 2);
      await reader
        .getByRole("button", { name: "Back to stories", exact: true })
        .click();
      await expect(card).toBeVisible();
      await expect(feed).toHaveJSProperty("paused", true);
      await expectTime(feed, 2);
      await seekAndPause(feed, 0.5);
      await page.goForward();
      await expect(opened).toHaveJSProperty("paused", true);
      await play(opened, reader);
      await expectTime(opened, 0.5);
    });
  }
  test(`YouTube retains fractional progress, stays on demand, and honors a rewind to zero at ${width}px`, async ({
    page,
    context,
  }) => {
    await page.setViewportSize({ width, height: 1000 });
    await mock(context, [youtube]);
    await page.goto("/reader/");
    const card = page.locator(".post-card");
    await card
      .getByRole("button", { name: "Play video: A YouTube film" })
      .click();
    let frame = card.locator("iframe").contentFrame();
    await expect(frame.locator("body")).toHaveAttribute("data-state", "1");
    await frame
      .getByRole("spinbutton", { name: "Playback position" })
      .fill("125.5");
    await card
      .getByRole("link", { name: `Read ${youtube.title}`, exact: true })
      .click();
    const reader = page.locator(".article-page");
    await expect(page.locator("iframe")).toHaveCount(0);
    await reader
      .getByRole("button", { name: "Play video: A YouTube film" })
      .click();
    frame = reader.locator("iframe").contentFrame();
    await expect(frame.locator("body")).toHaveAttribute("data-time", "125.5");
    await expect(frame.locator("body")).toHaveAttribute("data-state", "1");
    await frame
      .getByRole("spinbutton", { name: "Playback position" })
      .fill("142.25");
    await reader
      .getByRole("button", { name: "Back to stories", exact: true })
      .click();
    await expect(page.locator("iframe")).toHaveCount(0);
    await card
      .getByRole("button", { name: "Play video: A YouTube film" })
      .click();
    frame = card.locator("iframe").contentFrame();
    await expect(frame.locator("body")).toHaveAttribute("data-time", "142.25");
    await frame
      .getByRole("button", { name: "Pause video", exact: true })
      .click();
    await frame
      .getByRole("spinbutton", { name: "Playback position" })
      .fill("0");
    await page.goForward();
    await expect(page.locator("iframe")).toHaveCount(0);
    await reader
      .getByRole("button", { name: "Play video: A YouTube film" })
      .click();
    frame = reader.locator("iframe").contentFrame();
    await expect(frame.locator("body")).toHaveAttribute("data-time", "0");
    await expect(reader.locator("iframe")).toHaveAttribute("src", /start=0/);
  });
}

test("separate X attachments and posts never share a playhead", async ({
  page,
  context,
}) => {
  await page.setViewportSize({ width: 1000, height: 2400 });
  const post: Post = {
    ...x,
    x: {
      ...x.x,
      media: [
        ...x.x.media,
        {
          ...x.x.media[0],
          url: "/assets/position-two.mp4",
          source_url: "https://x.com/filmmaker/status/123/video/2",
        },
      ],
    },
  };
  const other = { ...x, id: "another-post", title: "Another post" };
  await mock(context, [post, other]);
  await page.goto("/reader/");
  const card = page.locator(".post-card").first();
  const first = card.locator("video").nth(0);
  await play(first, card);
  await seekAndPause(first, 1.25);
  const second = card.locator("video").nth(1);
  await play(second, card);
  await seekAndPause(second, 2);
  await card
    .getByRole("link", { name: `Read ${post.title}`, exact: true })
    .click();
  const reader = page.locator(".article-page");
  await play(reader.locator("video").nth(0), reader);
  await expectTime(reader.locator("video").nth(0), 1.25);
  await play(reader.locator("video").nth(1), reader);
  await expectTime(reader.locator("video").nth(1), 2);
  await page.goBack();
  const unrelated = page.locator(".post-card").nth(1);
  await unrelated
    .getByRole("link", { name: `Read ${other.title}`, exact: true })
    .click();
  await play(reader.locator("video"), reader);
  expect(
    await reader
      .locator("video")
      .evaluate((v: HTMLVideoElement) => v.currentTime),
  ).toBeLessThan(1);
});
