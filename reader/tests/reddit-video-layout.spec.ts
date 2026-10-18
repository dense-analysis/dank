import { fileURLToPath } from "node:url";
import {
  type BrowserContext,
  expect,
  type Locator,
  test,
} from "@playwright/test";
import type { Post, RedditVideo } from "../src/lib/types";

const raw = "https://v.redd.it/portrait";
const other = "https://v.redd.it/another";
const asset: RedditVideo = {
  url: "/assets/reddit-portrait.mp4",
  source_url: `${raw}/HLSPlaylist.m3u8?a=signed`,
  poster_url: "/assets/reddit-poster.svg",
  remote: false,
};
function post(video: RedditVideo | null = asset): Post {
  return {
    id: "portrait",
    domain: "reddit.com",
    source: "reddit",
    url: "https://www.reddit.com/r/videos/comments/portrait/clip/",
    author: "filmmaker",
    title: "A portrait clip",
    excerpt: `A clip. ${raw} ${other}`,
    html: `<p>A clip.</p><p><a href="${raw}">${raw}</a></p><p>${raw}/HLSPlaylist.m3u8?a=new</p><p><a href="${other}">${other}</a></p><p>Read <a href="${raw}">the discussion</a>.</p>`,
    created_at: "2026-10-17T00:00:00Z",
    thumbnail: null,
    media: [],
    reddit: { community: "videos", score: 3, comments: 2, flair: null, video },
  };
}
async function mock(context: BrowserContext, detail = post(), ratio = 9 / 16) {
  const state = { requests: 0, fail: false, posterFail: false };
  await context.route("**/assets/reddit-poster.svg", (route) =>
    route.fulfill(
      state.posterFail
        ? { status: 404 }
        : {
            contentType: "image/svg+xml",
            body: `<svg xmlns="http://www.w3.org/2000/svg" width="${320 * ratio}" height="320"><rect width="100%" height="100%" fill="#538573"/></svg>`,
          },
    ),
  );
  await context.route("**/assets/reddit-portrait.mp4", (route) => {
    state.requests++;
    return route.fulfill(
      state.fail
        ? { status: 404 }
        : {
            contentType: "video/mp4",
            path: fileURLToPath(
              new URL("./fixtures/x-video/portrait.mp4", import.meta.url),
            ),
          },
    );
  });
  await context.route("**/api/reader/**", (route) => {
    const path = new URL(route.request().url()).pathname;
    return route.fulfill({
      json: path.endsWith("/sources")
        ? {
            sources: [
              {
                domain: "reddit.com",
                name: "Reddit",
                kind: "reddit",
                tags: [],
                count: 1,
                communities: [{ name: "videos", count: 1 }],
              },
            ],
            total_posts: 1,
          }
        : path.endsWith("/post")
          ? detail
          : { posts: [detail], next_cursor: null, limited: false },
    });
  });
  return state;
}
async function expectRatio(video: Locator, ratio: number) {
  await expect
    .poll(async () => {
      const box = await video.boundingBox();
      expect(box?.height).toBeLessThanOrEqual(482);
      return box ? Math.abs(box.width / box.height - ratio) : 1;
    })
    .toBeLessThan(0.02);
}
for (const width of [1440, 390, 320]) {
  test(`Reddit portrait players and matching URL cleanup work in All, Reddit and opened posts at ${width}px`, async ({
    page,
    context,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    const state = await mock(context);
    for (const query of ["", "view=reddit", "view=reddit&community=videos"]) {
      await page.goto(`/reader/?${query}`);
      const card = page.locator(".reddit-card");
      await expect(card).toBeVisible();
      await expectRatio(card.locator("video"), 9 / 16);
      await expect(card.locator(".post-open p")).toContainText("A clip.");
      await expect(card.locator(".post-open p")).not.toContainText(raw);
      await expect(card.locator(".post-open p")).toContainText(other);
      expect(state.requests).toBe(0);
      await card
        .getByRole("link", { name: "Read A portrait clip", exact: true })
        .click();
      const reader = page.getByRole("main", { name: "Article reader" });
      await expectRatio(reader.locator("video"), 9 / 16);
      await expect(
        reader.getByRole("link", { name: raw, exact: true }),
      ).toHaveCount(0);
      await expect(
        reader.getByRole("link", { name: "the discussion", exact: true }),
      ).toHaveAttribute("href", raw);
      await expect(
        reader.getByRole("link", { name: other, exact: true }),
      ).toBeVisible();
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
      expect(state.requests).toBe(0);
    }
    await page.screenshot({
      path: testInfo.outputPath(`reddit-portrait-${width}.png`),
      fullPage: true,
    });
  });
}
for (const ratio of [1, 16 / 9]) {
  test(`Reddit poster proportions ${ratio} remain natural before playback`, async ({
    page,
    context,
  }) => {
    const state = await mock(context, post(), ratio);
    await page.goto("/reader/");
    await expectRatio(page.locator(".reddit-card video"), ratio);
    expect(state.requests).toBe(0);
  });
}
for (const poster of ["wrong-ratio", "unavailable"] as const) {
  test(`native video dimensions correct a ${poster} poster without restarting playback`, async ({
    page,
    context,
  }) => {
    const state = await mock(context, post(), 16 / 9);
    state.posterFail = poster === "unavailable";
    await page.goto("/reader/?article=portrait&article_source=reddit.com");
    const reader = page.getByRole("main", { name: "Article reader" });
    const video = reader.locator("video");
    await video.evaluate((v: HTMLVideoElement) => {
      v.volume = 0;
      v.loop = true;
    });
    await reader
      .getByRole("button", { name: "Play video", exact: true })
      .click();
    await expectRatio(video, 9 / 16);
    await expect
      .poll(() => video.evaluate((v: HTMLVideoElement) => v.currentTime))
      .toBeGreaterThan(0.1);
    await video.evaluate((v) => v.setAttribute("data-retained", "true"));
    const time = await video.evaluate((v: HTMLVideoElement) => v.currentTime);
    await reader.getByRole("button", { name: "Larger article text" }).click();
    await expect(video).toHaveAttribute("data-retained", "true");
    expect(
      await video.evaluate((v: HTMLVideoElement) => v.currentTime),
    ).toBeGreaterThanOrEqual(time);
    expect(state.requests).toBe(1);
  });
}
for (const detail of [false, true]) {
  test(`failed Reddit videos restore their raw URL in the ${detail ? "reader" : "feed"}`, async ({
    page,
    context,
  }) => {
    const state = await mock(context);
    state.fail = true;
    await page.goto(
      detail
        ? "/reader/?article=portrait&article_source=reddit.com"
        : "/reader/",
    );
    const scope = page.locator(detail ? ".article-page" : ".reddit-card");
    await scope
      .getByRole("button", { name: "Play video", exact: true })
      .click();
    await expect(scope.getByRole("status")).toContainText("couldn’t be played");
    if (detail)
      await expect(
        scope.getByRole("link", { name: raw, exact: true }),
      ).toBeVisible();
    else await expect(scope.locator(".post-open p")).toContainText(raw);
    await expect(
      scope.getByRole("link", {
        name: detail ? "Read original" : `Open original: ${post().title}`,
        exact: true,
      }),
    ).toHaveAttribute("href", post().url);
    expect(state.requests).toBe(1);
  });
}
test("Reddit links remain readable when no video was captured", async ({
  page,
  context,
}) => {
  await mock(context, post(null));
  await page.goto("/reader/");
  await expect(page.locator(".post-open p")).toContainText(raw);
  await page
    .getByRole("link", { name: "Read A portrait clip", exact: true })
    .click();
  await expect(
    page.locator(".article-page").getByRole("link", { name: raw, exact: true }),
  ).toBeVisible();
});

test("URL-only feed excerpts leave the title and player without placeholder text", async ({
  page,
  context,
}) => {
  await mock(context, {
    ...post(),
    excerpt: raw,
    html: `<p><a href="${raw}">${raw}</a></p><img src="/assets/reddit-poster.svg">`,
  });
  await page.goto("/reader/");
  await expect(page.locator(".reddit-card video")).toBeVisible();
  await expect(page.locator(".post-open p")).toHaveCount(0);
  await expect(page.locator(".post-open h2")).toHaveText("A portrait clip");
});

for (const width of [1440, 390]) {
  test(`native Reddit captions keep only the post source link at ${width}px`, async ({
    page,
    context,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await mock(context, post({ ...asset, remote: true }));

    for (const query of [
      "",
      "view=reddit&community=videos",
      "article=portrait&article_source=reddit.com",
    ]) {
      await page.goto(`/reader/?${query}`);
      const detail = query.includes("article=");
      const scope = page.locator(detail ? ".article-page" : ".reddit-card");
      await expect(scope.locator(".video-caption")).toHaveText(
        "Remote video - Reddit",
      );
      await expect(
        scope.locator(".video-caption .remote-content-label svg"),
      ).toHaveCount(1);
      await expect(scope.locator(".video-caption a")).toHaveCount(0);
      await expect(
        scope.getByRole("link", {
          name: detail ? "Read original" : `Open original: ${post().title}`,
          exact: true,
        }),
      ).toHaveAttribute("href", post().url);
    }
  });
}

test("a Reddit video from another site retains its own caption source link", async ({
  page,
  context,
}) => {
  const external = "https://media.example.com/clip.mp4";
  await mock(context, post({ ...asset, source_url: external, remote: true }));
  await page.goto("/reader/?view=reddit");
  const card = page.locator(".reddit-card");
  await expect(card.locator(".video-caption")).toHaveText(
    "Remote video - media.example.com",
  );
  await expect(
    card.getByRole("link", { name: "Watch on media.example.com", exact: true }),
  ).toHaveAttribute("href", external);
  await expect(
    card.getByRole("link", {
      name: `Open original: ${post().title}`,
      exact: true,
    }),
  ).toHaveAttribute("href", post().url);
});
