import { fileURLToPath } from "node:url";
import {
  type BrowserContext,
  expect,
  type Locator,
  test,
} from "@playwright/test";
import type { Post, XMedia } from "../src/lib/types";

const sourceUrl = "https://x.com/mathematician/status/123456/video/1";
const otherUrl = "https://x.com/another/status/654321/video/1";
const youtubeUrl = "https://www.youtube.com/watch?v=math-explanation";
const postText = "A portrait animation of the mathematical puzzle.";
const media: XMedia & { source_url: string; aspect_ratio?: number } = {
  type: "video",
  url: "/assets/x-portrait.mp4",
  poster_url: "/assets/x-portrait.svg",
  alt: "A portrait mathematical animation",
  remote: false,
  source_url: sourceUrl,
  aspect_ratio: 9 / 16,
};

function videoPost(asset = media): Post {
  return {
    id: "123456",
    domain: "x.com",
    source: "x",
    url: "https://x.com/mathematician/status/123456",
    author: "A Mathematician",
    title: postText,
    excerpt: postText,
    html: `<p>${postText}</p><p><a href="${sourceUrl}">${sourceUrl}</a></p><p><a href="${otherUrl}">${otherUrl}</a></p><p>Read <a href="${sourceUrl}">the original video discussion</a>.</p>`,
    created_at: "2026-10-16T17:41:00Z",
    thumbnail: null,
    media: [],
    x: {
      handle: "mathematician",
      name: "A Mathematician",
      icon_url: null,
      media: [asset],
      links: [
        {
          url: youtubeUrl,
          title: "A linked explanation",
          description: "This separate YouTube preview must remain available.",
          image_url: null,
        },
      ],
      reply_count: 3,
      replies_status: "not_collected",
    },
  };
}

async function mockVideo(context: BrowserContext, post = videoPost()) {
  const current = { post, requests: 0, failVideo: false };
  await context.route("**/assets/x-portrait.svg", (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="180" height="320"><rect width="180" height="320" fill="#538573"/></svg>',
    }),
  );
  await context.route("**/assets/x-portrait.mp4", (route) => {
    current.requests += 1;

    return route.fulfill(
      current.failVideo
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
    const url = new URL(route.request().url());

    return route.fulfill({
      json: url.pathname.endsWith("/sources")
        ? {
            sources: [
              {
                domain: "x.com",
                name: "X",
                kind: "x",
                count: 1,
                tags: [],
                accounts: [{ ...current.post.x, count: 1 }],
              },
            ],
            total_posts: 1,
          }
        : url.pathname.endsWith("/post")
          ? current.post
          : { posts: [current.post], next_cursor: null, limited: false },
    });
  });

  return current;
}

async function expectRatio(video: Locator, ratio: number) {
  await expect(video).toBeVisible();
  await expect
    .poll(async () => {
      const box = await video.boundingBox();

      return box ? Math.abs(box.width / box.height - ratio) : 1;
    })
    .toBeLessThan(0.02);
}

async function expectLinks(scope: Locator, rawVisible = false) {
  const raw = scope.getByRole("link", { name: sourceUrl, exact: true });

  if (rawVisible) {
    await expect(raw).toBeVisible();
    await expect(raw).toHaveAttribute("href", sourceUrl);
  } else {
    await expect(raw).toHaveCount(0);
  }

  await expect(
    scope.getByRole("link", { name: otherUrl, exact: true }),
  ).toHaveAttribute("href", otherUrl);
  await expect(
    scope.getByRole("link", {
      name: "the original video discussion",
      exact: true,
    }),
  ).toHaveAttribute("href", sourceUrl);
  await expect(
    scope.getByRole("link", { name: "Open link: A linked explanation" }),
  ).toHaveAttribute("href", youtubeUrl);
}

for (const width of [1440, 390, 320]) {
  test(`portrait videos retain their proportions and hide only their raw reference at ${width}px`, async ({
    page,
    context,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    const current = await mockVideo(context);

    for (const query of ["", "view=x", "view=x&account=mathematician"]) {
      await page.goto(`/reader/?${query}`);
      const card = page.locator(".post-card");
      const video = card.locator("video");
      await expectRatio(video, 9 / 16);
      await expectLinks(card);
      expect(current.requests).toBe(0);
      const bounds = await video.boundingBox();
      const content = await card.locator(".x-post-content").boundingBox();
      expect(bounds?.height).toBeLessThanOrEqual(640);
      expect(bounds?.width).toBeLessThanOrEqual(content?.width ?? 0);

      if (width === 1440) {
        expect(bounds?.width).toBeLessThan((content?.width ?? 0) * 0.75);
      }

      expect(
        await card
          .locator(".article-body")
          .evaluate((element) => Number(getComputedStyle(element).fontWeight)),
      ).toBeGreaterThanOrEqual(500);
      await card.getByText(postText, { exact: true }).click();
      const reader = page.getByRole("main", { name: "X post reader" });
      await expectRatio(reader.locator("video"), 9 / 16);
      await expectLinks(reader);
      expect(current.requests).toBe(0);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);
    }

    await page.screenshot({
      path: testInfo.outputPath(`x-portrait-reader-${width}.png`),
      fullPage: true,
    });
  });
}

for (const [orientation, ratio] of [
  ["landscape", 16 / 9],
  ["square", 1],
] as const) {
  test(`${orientation} video dimensions are preserved before playback`, async ({
    page,
    context,
  }) => {
    const current = await mockVideo(
      context,
      videoPost({ ...media, aspect_ratio: ratio }),
    );
    await page.goto("/reader/?view=x");
    const card = page.locator(".post-card");
    await expectRatio(card.locator("video"), ratio);
    await card.getByText(postText, { exact: true }).click();
    await expectRatio(
      page.getByRole("main", { name: "X post reader" }).locator("video"),
      ratio,
    );
    expect(current.requests).toBe(0);
  });
}

test("portrait metadata sizes older saved videos and native controls leave the feed open", async ({
  page,
  context,
}) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await mockVideo(context, videoPost({ ...media, aspect_ratio: undefined }));
  await page.goto("/reader/?view=x&account=mathematician");
  const origin = page.url();
  const video = page.locator(".post-card video");
  await video.scrollIntoViewIfNeeded();
  await video.evaluate((element) => {
    const player = element as HTMLVideoElement;
    player.muted = true;
    player.load();
  });
  await expect(video).toHaveJSProperty("videoWidth", 180);
  await expect(video).toHaveJSProperty("videoHeight", 320);
  await expectRatio(video, 9 / 16);
  const box = await video.boundingBox();
  expect(box).not.toBeNull();
  await video.hover();
  await video.click({ position: { x: 24, y: (box?.height ?? 0) - 48 } });
  await expect
    .poll(() =>
      video.evaluate((element) => (element as HTMLVideoElement).currentTime),
    )
    .toBeGreaterThan(0);
  await expect(page).toHaveURL(origin);
  await expect(page.getByRole("main", { name: "X post reader" })).toHaveCount(
    0,
  );
  await video.evaluate((element) => (element as HTMLVideoElement).pause());
});

test("video failures restore the paired raw reference and original-post fallback", async ({
  page,
  context,
}) => {
  const current = await mockVideo(context);
  current.failVideo = true;

  for (const query of [
    "view=x",
    "view=x&article=123456&article_source=x.com",
  ]) {
    await page.goto(`/reader/?${query}`);
    const content = page.locator(".x-post-content:visible");
    await expectLinks(content);
    await content.locator("video").evaluate((element) => {
      (element as HTMLVideoElement).load();
    });
    await expect(
      content.getByText("Video unavailable", { exact: true }),
    ).toBeVisible();
    await expectLinks(content, true);
    await expect(
      content.getByRole("link", { name: "View on X", exact: true }),
    ).toHaveAttribute("href", current.post.url);
  }
});

test("unsupported video sources retain raw references even when a poster is available", async ({
  page,
  context,
}) => {
  await mockVideo(
    context,
    videoPost({ ...media, url: "https://untrusted.example/video.mp4" }),
  );
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  await expectLinks(card, true);
  await expect(card.locator("video")).toHaveCount(0);
  await expect(
    card.getByText("Video unavailable", { exact: true }),
  ).toBeVisible();
});

test("portrait reply videos use the same natural player dimensions as the post", async ({
  page,
  context,
}) => {
  const original = videoPost();
  const reply = {
    ...original,
    id: "reply",
    parent_id: original.id,
    depth: 0,
    is_author: false,
    missing_parent: false,
  };
  const post: Post = {
    ...original,
    x: {
      ...original.x,
      handle: "mathematician",
      name: "A Mathematician",
      conversation: {
        status: "captured",
        captured_at: "2026-10-17T00:00:00Z",
        reported_replies: 1,
        has_more: false,
        reason: null,
        limit: 10,
        context: [],
        replies: [reply],
      },
    },
  };
  await mockVideo(context, post);
  await page.goto("/reader/?view=x&article=123456&article_source=x.com");
  const conversation = page.getByRole("region", {
    name: "Conversation",
    exact: true,
  });
  await expectRatio(conversation.locator("video"), 9 / 16);
  await expectLinks(conversation);
});
