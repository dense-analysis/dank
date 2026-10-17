import { expect, type Page, test } from "@playwright/test";
import type { YouTubeItem } from "../src/lib/youtubeCollection";

const id = "NnV_cWeoo5Q";
const title = "Kernel Recipes 2026 - Security in the LLM age";
const url = `/reader/?view=youtube&feed=linked&youtube_item=video:${id}`;
const captured = "2026-10-17T20:00:00Z";
const later = "2099-01-01T00:00:00Z";

function item(): YouTubeItem {
  return {
    id: `video:${id}`,
    video_id: id,
    kind: "video",
    url: `https://www.youtube.com/watch?v=${id}`,
    title: `YouTube video · ${id}`,
    text: "",
    channel: { id: "", name: "", url: "" },
    thumbnail_url: null,
    duration_text: "",
    views_text: "",
    published_text: "",
    observed_at: captured,
    discoveries: [],
    references: [],
    reference_count: 0,
    detail_capture: { status: "disabled" },
    metadata_capture: {
      status: "disabled",
      provider: "",
      observed_at: "",
      captured_at: "",
      retry_after: "",
      available: false,
    },
  };
}

async function fixture(page: Page, initial = item()) {
  const state = { video: initial, posts: 0, fail: false, unavailable: false };
  await page.route("**/api/reader/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith(`/youtube/metadata/${id}`)) {
      state.posts++;
      expect(route.request().method()).toBe("POST");
      if (state.fail) {
        await route.fulfill({
          status: 503,
          json: { error: "Collection is temporarily unavailable." },
        });
        return;
      }
      state.video = {
        ...state.video,
        title: state.unavailable ? state.video.title : title,
        channel: state.unavailable
          ? state.video.channel
          : {
              id: "",
              name: "Kernel Recipes",
              url: "https://www.youtube.com/@KernelRecipes",
            },
        metadata_capture: {
          status: state.unavailable ? "unavailable" : "captured",
          available: !state.unavailable,
          provider: "youtube_oembed",
          observed_at: captured,
          captured_at: state.unavailable ? "" : captured,
          retry_after: later,
        },
      };
      await route.fulfill({
        json: { ...state.video.metadata_capture, video_id: id, cached: false },
      });
    } else if (path.endsWith("/youtube/sources")) {
      await route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 1 },
      });
    } else if (path.endsWith("/sources")) {
      await route.fulfill({ json: { sources: [], total_posts: 0 } });
    } else if (path.endsWith("/youtube")) {
      await route.fulfill({
        json: { items: [state.video], total: 1, next_cursor: null },
      });
    } else {
      await route.fulfill({
        json: { posts: [], next_cursor: null, limited: false },
      });
    }
  });
  return state;
}

test("linked detail automatically saves genuine video metadata and updates the title", async ({
  page,
}) => {
  const state = await fixture(page);
  await page.goto(url);
  await expect(
    page.getByRole("heading", { name: title, exact: true }),
  ).toBeVisible();
  await expect(page.getByText("Kernel Recipes", { exact: true })).toBeVisible();
  await expect(page.getByText("Metadata saved", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Refresh video details" }),
  ).toBeDisabled();
  expect(state.posts).toBe(1);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: title, exact: true }),
  ).toBeVisible();
  expect(state.posts).toBe(1);
});

test("network errors do not loop and can be retried explicitly", async ({
  page,
}) => {
  const state = await fixture(page);
  state.fail = true;
  await page.goto(url);
  await expect(
    page.getByText("Collection is temporarily unavailable."),
  ).toBeVisible();
  expect(state.posts).toBe(1);
  state.fail = false;
  await page.getByRole("button", { name: "Get video details" }).click();
  await expect(
    page.getByRole("heading", { name: title, exact: true }),
  ).toBeVisible();
  expect(state.posts).toBe(2);
});

test("provider failures honor retry timing without claiming metadata was captured", async ({
  page,
}) => {
  const state = await fixture(page);
  state.unavailable = true;
  await page.goto(url);
  await expect(
    page.getByText("Video details aren't available from YouTube right now."),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Get video details" }),
  ).toBeDisabled();
  await expect(page.getByText("Metadata saved", { exact: true })).toHaveCount(
    0,
  );
  expect(state.posts).toBe(1);
});

test("saved metadata remains visible after an unsuccessful refresh", async ({
  page,
}) => {
  const video = item();
  video.title = title;
  video.metadata_capture = {
    status: "failed",
    provider: "youtube_oembed",
    available: true,
    captured_at: captured,
    observed_at: captured,
    retry_after: later,
  };
  const state = await fixture(page, video);
  await page.goto(url);
  await expect(
    page.getByRole("heading", { name: title, exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText(
      "The latest refresh couldn't complete. Previously saved details are still shown.",
    ),
  ).toBeVisible();
  await expect(page.getByText("Metadata saved", { exact: true })).toBeVisible();
  expect(state.posts).toBe(0);
});
