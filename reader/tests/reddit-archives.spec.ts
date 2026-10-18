import path from "node:path";
import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post, RedditCommentCapture, RedditVideo } from "../src/lib/types";

const capture: RedditCommentCapture = {
  status: "captured",
  sort: "best",
  limit: 50,
  total_available: 298,
  has_more: true,
  comments: [
    {
      id: "first",
      parent_id: "t3_video",
      author: "space_reader",
      body: "A remarkable landing.",
      body_html:
        "<p>A <strong>remarkable</strong> landing.</p><script>window.badComment = true</script><iframe src='https://unsafe.example'></iframe>",
      score: 42,
      created_utc: 1792180200,
      permalink:
        "https://www.reddit.com/r/accelerate/comments/video/thread/first/",
      is_op: false,
      depth: 0,
    },
    {
      id: "reply",
      parent_id: "t1_first",
      author: "stealth_post",
      body: "The timing is extraordinary.",
      body_html: "<p>The timing is extraordinary.</p>",
      score: 12,
      created_utc: 1792180260,
      permalink:
        "https://www.reddit.com/r/accelerate/comments/video/thread/reply/",
      is_op: true,
      depth: 1,
    },
    {
      id: "nested",
      parent_id: "t1_reply",
      author: "another_reader",
      body: "Thanks for sharing the source.",
      body_html: "<p>Thanks for sharing the source.</p>",
      score: null,
      created_utc: null,
      permalink:
        "https://www.reddit.com/r/accelerate/comments/video/thread/nested/",
      is_op: false,
      depth: 5,
    },
  ],
};
const remoteVideo: RedditVideo = {
  source_url: "https://v.redd.it/landing",
  url: "https://video.reddit.test/playlist.m3u8",
  hls_url: "https://video.reddit.test/playlist.m3u8",
  poster_url: null,
  remote: true,
};
const post: Post = {
  id: "video",
  domain: "reddit.com",
  url: "https://www.reddit.com/r/accelerate/comments/video/landing/",
  author: "stealth_post",
  title: "The ballet of thruster timing",
  excerpt: "A remarkable landing.",
  html: "<p>A closer look at the landing, with the original source preserved.</p>",
  created_at: "2026-10-16T17:45:00Z",
  source: "reddit",
  thumbnail: null,
  media: [],
  reddit: {
    community: "accelerate",
    score: 242,
    comments: 298,
    flair: null,
    video: remoteVideo,
    comment_capture: capture,
  },
};

const reddit = post.reddit;
if (!reddit) throw new Error("Expected Reddit fixture");

async function mockArchive(page: Page, detail: Post = post) {
  const streams: string[] = [];
  await page.route("https://video.reddit.test/**", async (route) => {
    const name = new URL(route.request().url()).pathname.slice(1);
    streams.push(name);
    await route.fulfill({
      path: path.join(import.meta.dirname, "fixtures/reddit-video", name),
      contentType: name.endsWith("m3u8")
        ? "application/vnd.apple.mpegurl"
        : name.endsWith("mp4")
          ? "video/mp4"
          : "video/mp2t",
      headers: { "Access-Control-Allow-Origin": "*" },
    });
  });
  await page.route("**/assets/archived.mp4", (route) =>
    route.fulfill({
      path: path.join(
        import.meta.dirname,
        "fixtures/reddit-video/archived.mp4",
      ),
      contentType: "video/mp4",
    }),
  );
  await page.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());
    const listPost = {
      ...detail,
      reddit: detail.reddit
        ? { ...detail.reddit, video: undefined, comment_capture: undefined }
        : null,
    };
    return route.fulfill({
      json: url.pathname.endsWith("/sources")
        ? {
            sources: [
              {
                domain: "reddit.com",
                name: "Reddit",
                kind: "reddit",
                tags: [],
                count: 1,
                communities: [{ name: "accelerate", count: 1 }],
              },
            ],
            total_posts: 1,
          }
        : url.pathname.endsWith("/post")
          ? detail
          : { posts: [listPost], next_cursor: null, limited: false },
    });
  });
  return streams;
}

async function openPost(page: Page) {
  await page.goto("/reader/?view=reddit&community=accelerate");
  await page
    .getByRole("link", { name: `Read ${post.title}`, exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: post.title, exact: true }),
  ).toBeVisible();
}

for (const width of [1280, 390]) {
  test(`reads sanitized threaded comment snapshots and remote video at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    const streams = await mockArchive(page);
    await openPost(page);
    await expect(page.getByText("Remote video - Reddit")).toBeVisible();
    await expect(page.getByText("Best · saved snapshot")).toBeVisible();
    await expect(
      page.getByText("3 saved of 298", { exact: false }),
    ).toBeVisible();
    await expect(page.locator(".reddit-comment")).toHaveCount(3);
    await expect(
      page.locator(".reddit-comment iframe, .reddit-comment script"),
    ).toHaveCount(0);
    await expect(page.getByText("OP", { exact: true })).toBeVisible();
    await expect(
      page.getByRole("link", { name: "More comments on Reddit" }),
    ).toHaveAttribute("href", post.url);
    expect(streams).toEqual([]);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: testInfo.outputPath(`reddit-archive-${width}.png`),
      fullPage: true,
    });
    const results = await new AxeBuilder({ page }).analyze();
    expect(
      results.violations.filter((item) =>
        ["serious", "critical"].includes(item.impact ?? ""),
      ),
    ).toEqual([]);
  });
}

test("plays remote HLS with audio only after pressing play", async ({
  page,
}) => {
  const streams = await mockArchive(page, { ...post, html: "" });
  await openPost(page);
  await expect(page.getByText("Full text not collected")).toHaveCount(0);
  const video = page.getByLabel("Reddit video");
  await video.evaluate((element: HTMLVideoElement) => {
    element.volume = 0;
  });
  expect(streams).toEqual([]);
  await page.getByRole("button", { name: "Play video", exact: true }).click();
  await expect
    .poll(() =>
      video.evaluate((element: HTMLVideoElement) => element.currentTime),
    )
    .toBeGreaterThan(0.2);
  expect(streams).toContain("playlist.m3u8");
  expect(streams).toContain("segment.mpeg");
  expect(
    await video.evaluate(
      (element) =>
        (element as HTMLVideoElement & { webkitAudioDecodedByteCount: number })
          .webkitAudioDecodedByteCount,
    ),
  ).toBeGreaterThan(0);
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(video).toHaveCount(0);
});

test("plays archived video locally without duplicate media or a remote label", async ({
  page,
}) => {
  const streams = await mockArchive(page, {
    ...post,
    html: "",
    media: [{ url: "/assets/archived.mp4", content_type: "video/mp4" }],
    reddit: {
      ...reddit,
      video: { ...remoteVideo, url: "/assets/archived.mp4", remote: false },
    },
  });
  await openPost(page);
  await expect(page.locator("video")).toHaveCount(1);
  await expect(page.getByText("Remote video - Reddit")).toHaveCount(0);
  await expect(page.locator(".reddit-video figcaption")).toBeHidden();
  await page.locator("video").evaluate((element: HTMLVideoElement) => {
    element.volume = 0;
  });
  await page.getByRole("button", { name: "Play video", exact: true }).click();
  await expect
    .poll(() =>
      page
        .locator("video")
        .evaluate((element: HTMLVideoElement) => element.currentTime),
    )
    .toBeGreaterThan(0.2);
  expect(streams).toEqual([]);
});

test("offers the original when a stream fails", async ({ page }) => {
  await mockArchive(page);
  await page.route("https://video.reddit.test/**", (route) =>
    route.fulfill({ status: 404 }),
  );
  await openPost(page);
  await page.getByRole("button", { name: "Play video", exact: true }).click();
  await expect(
    page
      .getByRole("status")
      .filter({ hasText: "This video couldn’t be played here" }),
  ).toBeVisible({ timeout: 15000 });
  await expect(
    page.getByRole("link", { name: "Read original", exact: true }),
  ).toHaveAttribute("href", post.url);
});

for (const status of [
  undefined,
  "disabled",
  "failed",
  "not_captured",
  "deferred",
] as const) {
  test(`explains ${status ?? "historical"} comments without claiming an empty conversation`, async ({
    page,
  }) => {
    await mockArchive(page, {
      ...post,
      reddit: {
        ...reddit,
        video: null,
        comment_capture: status
          ? { ...capture, status, comments: [] }
          : undefined,
      },
    });
    await openPost(page);
    await expect(
      page.getByRole("heading", { name: "Comments not captured" }),
    ).toBeVisible();
    await expect(
      page.getByText("Reddit reported 298 comments at collection.", {
        exact: false,
      }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Read comments on Reddit" }),
    ).toHaveAttribute("href", post.url);
    await expect(
      page.getByText("No comments when this post was collected."),
    ).toHaveCount(0);
  });
}

test("distinguishes captured-empty, unknown historical totals, and retained comments", async ({
  page,
}) => {
  await mockArchive(page, {
    ...post,
    reddit: {
      ...reddit,
      comments: 0,
      video: null,
      comment_capture: {
        ...capture,
        total_available: 0,
        comments: [],
        has_more: false,
      },
    },
  });
  await openPost(page);
  await expect(
    page.getByText("No comments when this post was collected."),
  ).toBeVisible();
  await page.unroute("**/api/reader/**");
  await mockArchive(page, {
    ...post,
    reddit: {
      ...reddit,
      comments: null,
      video: null,
      comment_capture: undefined,
    },
  });
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "Comments not captured" }),
  ).toBeVisible();
  await page.unroute("**/api/reader/**");
  await mockArchive(page, {
    ...post,
    reddit: {
      ...reddit,
      video: null,
      comment_capture: {
        ...capture,
        captured_at: "2026-10-16T17:45:00Z",
        latest_attempt_status: "failed",
      },
    },
  });
  await page.reload();
  await expect(
    page.getByText("The latest collection couldn’t refresh comments.", {
      exact: false,
    }),
  ).toBeVisible();
  await expect(page.locator(".reddit-comment")).toHaveCount(3);
});

test("persists display preferences without implying collection settings changed", async ({
  page,
}) => {
  await mockArchive(page);
  await openPost(page);
  await page.getByRole("button", { name: "Settings", exact: true }).click();
  await page
    .getByRole("checkbox", { name: "Show captured comments" })
    .uncheck();
  await page
    .getByRole("checkbox", { name: "Show remote content labels" })
    .uncheck();
  await expect(
    page.getByText("These are reading preferences.", { exact: false }),
  ).toBeVisible();
  await page.reload();
  await expect(
    page.getByRole("checkbox", { name: "Show captured comments" }),
  ).not.toBeChecked();
  await expect(
    page.getByRole("checkbox", { name: "Show remote content labels" }),
  ).not.toBeChecked();
  await openPost(page);
  await expect(page.getByText("Remote video - Reddit")).toHaveCount(0);
  await expect(page.locator(".reddit-video figcaption")).toBeHidden();
  await expect(page.locator(".reddit-comment")).toHaveCount(0);
  await expect(
    page.getByText("Comments are hidden in your reading preferences."),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Show comments", exact: true })
    .click();
  await expect(page.locator(".reddit-comment")).toHaveCount(3);
});

test("falls back to a verified MP4 with audio when HLS fails", async ({
  page,
}) => {
  await mockArchive(page, {
    ...post,
    reddit: {
      ...reddit,
      video: {
        ...remoteVideo,
        fallback_url: "https://video.reddit.test/archived.mp4",
      },
    },
  });
  await page.route("https://video.reddit.test/playlist.m3u8", (route) =>
    route.fulfill({ status: 404 }),
  );
  await openPost(page);
  const video = page.getByLabel("Reddit video");
  await video.evaluate((element: HTMLVideoElement) => {
    element.volume = 0;
  });
  await page.getByRole("button", { name: "Play video", exact: true }).click();
  await expect
    .poll(() =>
      video.evaluate((element: HTMLVideoElement) => element.currentTime),
    )
    .toBeGreaterThan(0.2);
  await expect(video).toHaveJSProperty(
    "currentSrc",
    "https://video.reddit.test/archived.mp4",
  );
  expect(
    await video.evaluate(
      (element) =>
        (element as HTMLVideoElement & { webkitAudioDecodedByteCount: number })
          .webkitAudioDecodedByteCount,
    ),
  ).toBeGreaterThan(0);
  await expect(
    page.getByText("This video couldn’t be played here.", { exact: false }),
  ).toHaveCount(0);
});

test("keeps archived comments when a refresh is deferred", async ({ page }) => {
  await mockArchive(page, {
    ...post,
    reddit: {
      ...reddit,
      video: null,
      comment_capture: {
        ...capture,
        captured_at: "2026-10-16T17:45:00Z",
        latest_attempt_status: "deferred",
      },
    },
  });
  await openPost(page);
  await expect(
    page.getByText(
      "Post details and comments are awaiting a separate refresh.",
      { exact: false },
    ),
  ).toBeVisible();
  await expect(page.locator(".reddit-comment")).toHaveCount(3);
});

test("older Reddit video attachments share playback with the Reddit stream player", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1000, height: 2400 });
  await mockArchive(page, {
    ...post,
    media: [{ url: "/assets/archived.mp4", content_type: "video/mp4" }],
  });
  await openPost(page);
  const attachment = page.getByLabel("Article video", { exact: true });
  const stream = page.getByLabel("Reddit video", { exact: true });
  await attachment.evaluate(async (v: HTMLVideoElement) => {
    v.volume = 0;
    v.loop = true;
    await v.play();
  });
  await expect
    .poll(() => attachment.evaluate((v: HTMLVideoElement) => v.currentTime))
    .toBeGreaterThan(0.1);
  await stream.evaluate((v: HTMLVideoElement) => {
    v.volume = 0;
    v.loop = true;
  });
  await page.getByRole("button", { name: "Play video", exact: true }).click();
  await expect
    .poll(() => stream.evaluate((v: HTMLVideoElement) => v.currentTime))
    .toBeGreaterThan(0.1);
  await expect(attachment).toHaveJSProperty("paused", true);
  await attachment.evaluate((v: HTMLVideoElement) => v.play());
  await expect(stream).toHaveJSProperty("paused", true);
});
