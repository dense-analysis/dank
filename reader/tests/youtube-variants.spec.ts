import { expect, type Page, test } from "@playwright/test";
import type { YouTubeItem } from "../src/lib/youtubeCollection";

const video: YouTubeItem = {
  id: "video:dQw4w9WgXcQ",
  video_id: "dQw4w9WgXcQ",
  kind: "video",
  url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  title: "The current observed title",
  text: "",
  channel: {
    id: "channel",
    name: "The channel",
    url: "https://www.youtube.com/@channel",
  },
  thumbnail_url: "/assets/thumbnail-new.jpg",
  duration_text: "5:12",
  views_text: "10K views",
  published_text: "1 day ago",
  observed_at: "2026-10-17T10:00:00Z",
  detail_capture: { status: "disabled" },
  discoveries: [],
  references: [],
  reference_count: 0,
  thumbnail_variants: [
    {
      url: "/assets/thumbnail-new.jpg",
      first_seen: "2026-10-17T10:00:00Z",
      last_seen: "2026-10-17T10:00:00Z",
    },
    {
      url: "/assets/thumbnail-old.jpg",
      first_seen: "2026-10-15T10:00:00Z",
      last_seen: "2026-10-16T10:00:00Z",
    },
  ],
  title_history: [
    {
      title: "The current observed title",
      first_seen: "2026-10-17T10:00:00Z",
      last_seen: "2026-10-17T10:00:00Z",
    },
    {
      title: "An earlier observed title",
      first_seen: "2026-10-15T10:00:00Z",
      last_seen: "2026-10-16T10:00:00Z",
    },
  ],
};

async function fixture(page: Page, item = video, broken: string[] = []) {
  const requests: string[] = [];
  await page.route("**/api/reader/**", (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith("/youtube/sources"))
      return route.fulfill({
        json: { feeds: [], channels: [], total: 1, linked_count: 0 },
      });
    if (path.endsWith("/youtube"))
      return route.fulfill({
        json: { items: [item], total: 1, next_cursor: null },
      });

    return route.fulfill({ json: { sources: [], total_posts: 0 } });
  });
  await page.route(/\/assets\/thumbnail-|i\.ytimg\.com/, (route) => {
    const url = route.request().url();
    requests.push(url);
    if (broken.some((part) => url.includes(part)))
      return route.fulfill({ status: 404 });
    const color = url.includes("old") ? "#7245a1" : "#328372";

    return route.fulfill({
      contentType: "image/svg+xml",
      body: `<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360"><rect width="640" height="360" fill="${color}"/><text x="30" y="180" font-size="50" fill="white">${url.includes("old") ? "Previous image" : "Current image"}</text></svg>`,
    });
  });
  await page.route("https://www.youtube-nocookie.com/**", (route) =>
    route.fulfill({ contentType: "text/html", body: "<p>Player fixture</p>" }),
  );
  await page.clock.install();
  await page.goto("/reader/?view=youtube");
  await expect(page.locator(".yt-card")).toHaveCount(1);
  await expect(
    page.locator(".yt-thumbnail-versions img.is-current"),
  ).toBeVisible();
  await expect
    .poll(() =>
      page
        .locator(".yt-thumbnail-versions img")
        .evaluateAll((images) =>
          images.every((image) => (image as HTMLImageElement).complete),
        ),
    )
    .toBe(true);

  return requests;
}

test("hover crossfades captured images without changing the title or starting a video", async ({
  page,
}) => {
  await fixture(page);
  const play = page.getByRole("button", {
    name: `Play ${video.title}`,
    exact: true,
  });
  await expect(page.locator(".yt-thumbnail-count")).toHaveText("2 thumbnails");
  await expect(page.locator("iframe, video")).toHaveCount(0);
  await play.hover();
  await page.clock.runFor(2500);
  await expect(
    page.locator(".yt-thumbnail-versions .is-current"),
  ).toHaveAttribute("src", /thumbnail-old/);
  await expect(page.locator(".yt-thumbnail-versions .is-current")).toHaveCSS(
    "transition-duration",
    "0.5s, 0.24s",
  );
  await expect(page.locator(".yt-card h3")).toHaveText(video.title);
  await expect(page.locator("iframe, video")).toHaveCount(0);
  await page.mouse.move(0, 0);
  await page.clock.runFor(6000);
  await expect(
    page.locator(".yt-thumbnail-versions .is-current"),
  ).toHaveAttribute("src", /thumbnail-new/);
  await play.click();
  await expect(page.locator("iframe")).toHaveCount(1);
  await expect(page.locator(".yt-thumbnail-versions")).toHaveCount(0);
});

test("keyboard focus previews versions and details retain one video with dated title history", async ({
  page,
}) => {
  await fixture(page);
  await page
    .getByRole("button", { name: `Play ${video.title}`, exact: true })
    .focus();
  await page.clock.runFor(2500);
  await expect(
    page.locator(".yt-thumbnail-versions .is-current"),
  ).toHaveAttribute("src", /thumbnail-old/);
  await page
    .getByRole("button", { name: `Discovery details for ${video.title}` })
    .click();
  await expect(page.locator(".yt-card")).toHaveCount(1);
  const titles = page.getByRole("region", { name: "Observed titles" });
  await expect(titles.getByText(video.title, { exact: true })).toBeVisible();
  await expect(
    titles.getByText("An earlier observed title", { exact: true }),
  ).toBeVisible();
  await expect(titles.locator("time")).toHaveCount(4);
  await expect(
    page.getByRole("region", { name: "Saved thumbnails" }).locator("img"),
  ).toHaveCount(2);
});

test("reduced motion keeps the preview static but preserves explicit history access", async ({
  page,
}) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await fixture(page);
  await page
    .getByRole("button", { name: `Play ${video.title}`, exact: true })
    .hover();
  await page.clock.runFor(6000);
  await expect(
    page.locator(".yt-thumbnail-versions .is-current"),
  ).toHaveAttribute("src", /thumbnail-new/);
  await expect(page.locator(".yt-thumbnail-versions .is-current")).toHaveCSS(
    "transition-duration",
    "0s",
  );
  await page
    .getByRole("button", { name: `Discovery details for ${video.title}` })
    .click();
  await expect(
    page.getByRole("region", { name: "Saved thumbnails" }).getByRole("link"),
  ).toHaveCount(2);
});

test("failed primary thumbnail uses the canonical fallback without inventing variants", async ({
  page,
}) => {
  const requests = await fixture(
    page,
    { ...video, thumbnail_variants: [], title_history: [] },
    ["thumbnail-new"],
  );
  await expect(
    page.locator(".yt-thumbnail-versions .is-current"),
  ).toHaveAttribute("src", "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg");
  await expect(page.locator(".yt-thumbnail-count")).toHaveCount(0);
  expect(requests.filter((url) => url.includes("hqdefault"))).toHaveLength(1);
});

test("a broken alternate is excluded instead of fading to an empty image", async ({
  page,
}) => {
  await fixture(page, video, ["thumbnail-old"]);
  await page
    .getByRole("button", { name: `Play ${video.title}`, exact: true })
    .hover();
  await page.clock.runFor(6000);
  await expect(page.locator(".yt-thumbnail-versions img")).toHaveCount(1);
  await expect(page.locator(".yt-thumbnail-count")).toHaveCount(0);
  await expect(
    page.locator(".yt-thumbnail-versions .is-current"),
  ).toHaveAttribute("src", /thumbnail-new/);
  await page
    .getByRole("button", { name: `Discovery details for ${video.title}` })
    .click();
  const history = page.getByRole("region", { name: "Saved thumbnails" });
  await expect(history.getByText("Thumbnail unavailable")).toBeVisible();
  await expect(history.getByRole("link")).toHaveCount(1);
  await expect(history.locator("time")).toHaveCount(4);
});

for (const width of [1440, 390, 320]) {
  test(`version history fits at ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    await fixture(page);
    await page
      .getByRole("button", { name: `Discovery details for ${video.title}` })
      .click();
    await expect(
      page.getByRole("region", { name: "Observed titles" }),
    ).toBeVisible();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: testInfo.outputPath(`versions-${width}.png`),
      fullPage: true,
    });
  });
}

test("preview pauses when the page is hidden and resumes without starting playback", async ({
  page,
}) => {
  await fixture(page);
  await page
    .getByRole("button", { name: `Play ${video.title}`, exact: true })
    .focus();
  await page.clock.runFor(2500);
  await expect(
    page.locator(".yt-thumbnail-versions .is-current"),
  ).toHaveAttribute("src", /thumbnail-old/);
  await page.evaluate(() => {
    Object.defineProperty(document, "visibilityState", {
      value: "hidden",
      configurable: true,
    });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await page.clock.runFor(2500);
  await expect(
    page.locator(".yt-thumbnail-versions .is-current"),
  ).toHaveAttribute("src", /thumbnail-new/);
  await page.evaluate(() => {
    Object.defineProperty(document, "visibilityState", {
      value: "visible",
      configurable: true,
    });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await page.clock.runFor(2500);
  await expect(
    page.locator(".yt-thumbnail-versions .is-current"),
  ).toHaveAttribute("src", /thumbnail-old/);
  await expect(page.locator("iframe, video")).toHaveCount(0);
});
