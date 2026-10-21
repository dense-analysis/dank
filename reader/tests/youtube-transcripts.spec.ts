import { expect, type Page, test } from "@playwright/test";
import type { RefreshJob } from "../src/lib/refresh";
import type { YouTubeItem } from "../src/lib/youtubeCollection";
import type { YouTubeTranscript } from "../src/lib/youtubeTranscript";

const id = "L_6pAVHYoFM";
const item: YouTubeItem = {
  id: `video:${id}`,
  video_id: id,
  kind: "video",
  url: `https://www.youtube.com/watch?v=${id}`,
  title: "A collected video with captions",
  text: "",
  channel: { id: "channel", name: "Example channel", url: "" },
  thumbnail_url: null,
  duration_text: "3:00",
  views_text: "",
  published_text: "",
  observed_at: "2026-10-19T12:00:00Z",
  discoveries: [],
  references: [],
  reference_count: 0,
  detail_capture: { status: "disabled" },
};
const blank: YouTubeTranscript = {
  video_id: id,
  status: "not_collected",
  observed_at: "",
  retry_after: "",
  language: "",
  language_name: "",
  generated: false,
  segments: [],
};
const saved: YouTubeTranscript = {
  ...blank,
  status: "captured",
  observed_at: "2026-10-19T12:00:00Z",
  language: "en",
  language_name: "English",
  segments: [
    { start: 1.2, duration: 4, text: "Welcome to the saved transcript." },
    { start: 65, duration: 3, text: "<script>Literal caption text</script>" },
  ],
};
const detail = `/reader/?view=youtube&youtube_item=video:${id}`;

async function fixture(page: Page, initial = blank) {
  const state = { result: initial, posts: 0, reads: 0, fail: false };
  await page.route("**/api/reader/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith(`/youtube/transcript/${id}`)) {
      if (route.request().method() === "POST") {
        state.posts++;
        if (state.fail)
          return route.fulfill({
            status: 503,
            json: { error: "Transcript collection is busy; try again shortly" },
          });
        state.result = saved;
      } else state.reads++;

      return route.fulfill({ json: state.result });
    }
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

  return state;
}

for (const width of [390, 1440]) {
  test(`first-open captions are saved and reused at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 1000 });
    const state = await fixture(page);
    await page.goto("/reader/?view=youtube");
    await expect(page.locator(".yt-card")).toHaveCount(1);
    expect(state.reads).toBe(0);
    expect(state.posts).toBe(0);
    await page.goto(detail);
    const transcript = page.getByRole("region", { name: "Video transcript" });
    await expect(
      transcript.getByText("Welcome to the saved transcript."),
    ).toBeVisible();
    await expect(
      transcript.getByText("English · Creator captions"),
    ).toBeVisible();
    await expect(
      transcript.getByText("<script>Literal caption text</script>"),
    ).toBeVisible();
    await expect(
      transcript.getByRole("link", { name: "1:05 on YouTube" }),
    ).toHaveAttribute("href", `https://www.youtube.com/watch?v=${id}&t=65s`);
    expect(state.posts).toBe(1);
    await page.reload();
    await expect(
      transcript.getByText("Welcome to the saved transcript."),
    ).toBeVisible();
    expect(state.posts).toBe(1);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await transcript.screenshot({
      path: testInfo.outputPath(`transcript-${width}.png`),
    });
  });
}

test("saved automatic captions need no collection request", async ({
  page,
}) => {
  const state = await fixture(page, { ...saved, generated: true });
  await page.goto(detail);
  await expect(page.getByText("English · Automatic captions")).toBeVisible();
  expect(state.posts).toBe(0);
});

for (const status of ["failed", "unavailable"] as const) {
  test(`${status} captions respect their retry delay`, async ({ page }) => {
    const state = await fixture(page, {
      ...blank,
      status,
      observed_at: "2026-10-19T12:00:00Z",
      retry_after: "2099-01-01T00:00:00Z",
    });
    await page.goto(detail);
    await expect(
      page.getByRole("button", { name: "Try transcript again" }),
    ).toBeDisabled();
    await expect(
      page.getByText(
        status === "failed"
          ? "Couldn't fetch captions from YouTube. This doesn't mean the video has no transcript."
          : "No supported captions were available when this video was checked.",
      ),
    ).toBeVisible();
    expect(state.posts).toBe(0);
  });
}

test("an expired unavailable result checks again on opening", async ({
  page,
}) => {
  const state = await fixture(page, {
    ...blank,
    status: "unavailable",
    observed_at: "2000-01-01T00:00:00Z",
    retry_after: "2000-01-02T00:00:00Z",
  });
  await page.goto(detail);
  await expect(
    page.getByText("Welcome to the saved transcript."),
  ).toBeVisible();
  expect(state.posts).toBe(1);
});

test("transport errors do not loop and allow an explicit retry", async ({
  page,
}) => {
  const state = await fixture(page);
  state.fail = true;
  await page.goto(detail);
  await expect(
    page.getByText("Transcript collection is busy; try again shortly"),
  ).toBeVisible();
  expect(state.posts).toBe(1);
  state.fail = false;
  await page.getByRole("button", { name: "Try transcript again" }).click();
  await expect(
    page.getByText("Welcome to the saved transcript."),
  ).toBeVisible();
  expect(state.posts).toBe(2);
});

test("accepting a completed video refresh reloads saved captions without collecting them again", async ({
  page,
}) => {
  const state = await fixture(page, {
    ...blank,
    status: "unavailable",
    observed_at: "2026-10-19T12:00:00Z",
    retry_after: "2099-01-01T00:00:00Z",
  });
  const jobs: RefreshJob[] = [];
  await page.route("**/api/reader/refresh", async (route) => {
    if (route.request().method() === "POST") {
      expect(route.request().postDataJSON()).toEqual({
        scope: { kind: "post", domain: "youtube.com", post_id: `video:${id}` },
      });
      jobs[0] = {
        id: "video-refresh",
        scope: { kind: "post", domain: "youtube.com", post_id: `video:${id}` },
        label: "This video",
        status: "running",
        message: "Refreshing video details.",
        created_at: "2026-10-22T12:00:00Z",
        finished_at: null,
        posts_saved: 0,
      };
      return route.fulfill({ status: 202, json: { job: jobs[0] } });
    }

    return route.fulfill({ json: { jobs } });
  });
  await page.goto(detail);
  const transcript = page.getByRole("region", { name: "Video transcript" });
  await expect(
    transcript.getByText(
      "No supported captions were available when this video was checked.",
    ),
  ).toBeVisible();
  const readsBefore = state.reads;
  await page
    .getByRole("button", { name: "Refresh this YouTube item", exact: true })
    .click();
  await expect.poll(() => jobs[0]?.status).toBe("running");
  state.result = saved;
  jobs[0] = {
    ...jobs[0],
    status: "completed",
    message: "Video and captions saved.",
    posts_saved: 1,
  };
  await expect(
    page.getByRole("button", { name: "Show updates" }),
  ).toBeVisible();
  await expect(
    transcript.getByText(
      "No supported captions were available when this video was checked.",
    ),
  ).toBeVisible();
  expect(state.reads).toBe(readsBefore);
  await page.getByRole("button", { name: "Show updates" }).click();
  await expect(
    transcript.getByText("Welcome to the saved transcript."),
  ).toBeVisible();
  expect(state.reads).toBe(readsBefore + 1);
  expect(state.posts).toBe(0);
});
