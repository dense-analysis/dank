import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { RefreshJob, RefreshScope } from "../src/lib/refresh";
import type { Post } from "../src/lib/types";

const saved: Post = {
  id: "post-one",
  domain: "reddit.com",
  url: "https://www.reddit.com/r/LocalLLaMA/comments/post-one/",
  author: "reader",
  title: "Already collected discussion",
  excerpt: "A discussion you can keep reading.",
  html: "<p>A discussion you can keep reading.</p>",
  created_at: "2026-10-22T10:00:00Z",
  source: "reddit",
  thumbnail: null,
  media: [],
  reddit: { community: "LocalLLaMA", score: 3, comments: 0, flair: null },
};

async function fixture(page: Page) {
  const state = {
    jobs: [] as RefreshJob[],
    requests: [] as RefreshScope[],
    postRequests: 0,
    jobsRequests: 0,
    posts: [saved],
    reject: false,
    statusFail: false,
    postsFail: false,
  };
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());

    if (url.pathname.endsWith("/refresh")) {
      if (route.request().method() === "POST") {
        const scope = route.request().postDataJSON().scope as RefreshScope;
        state.requests.push(scope);

        if (state.reject) {
          await route.fulfill({
            status: 409,
            json: {
              detail:
                "The refresh queue is full. Try again when a refresh finishes.",
            },
          });
          return;
        }

        const job: RefreshJob = {
          id: `job-${state.requests.length}`,
          scope,
          label:
            scope.kind === "post"
              ? "This post"
              : scope.community
                ? `r/${scope.community}`
                : scope.kind,
          status: "running",
          message: "Collecting fresh posts. Keep reading while this runs.",
          created_at: "2026-10-22T10:00:00Z",
          finished_at: null,
          posts_saved: 0,
        };
        state.jobs.unshift(job);
        await route.fulfill({ status: 202, json: { job } });
      } else {
        state.jobsRequests++;
        await route.fulfill({
          status: state.statusFail ? 503 : 200,
          json: state.statusFail
            ? { detail: "Temporarily unavailable" }
            : { jobs: state.jobs },
        });
      }
      return;
    }

    if (url.pathname.endsWith("/youtube/sources")) {
      await route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 0 },
      });
      return;
    }

    if (url.pathname.endsWith("/youtube/items")) {
      await route.fulfill({ json: { items: [], next_cursor: null, total: 0 } });
      return;
    }

    if (url.pathname.endsWith("/sources")) {
      await route.fulfill({
        json: {
          sources: [
            {
              domain: "reddit.com",
              name: "Reddit",
              kind: "reddit",
              tags: [],
              count: state.posts.length,
              communities: [{ name: "LocalLLaMA", count: state.posts.length }],
            },
          ],
          total_posts: state.posts.length,
        },
      });
      return;
    }

    if (url.pathname.endsWith("/post")) {
      await route.fulfill({
        json:
          state.posts.find((post) => post.id === url.searchParams.get("id")) ??
          saved,
      });
      return;
    }

    state.postRequests++;

    if (state.postsFail) {
      await route.fulfill({
        status: 503,
        json: { detail: "Saved stories temporarily unavailable." },
      });
      return;
    }

    await route.fulfill({
      json: { posts: state.posts, next_cursor: null, limited: false },
    });
  });

  return state;
}

test("refresh collects only the community and holds updates until accepted while navigation keeps working", async ({
  page,
}) => {
  const state = await fixture(page);
  await page.goto(
    "/reader/?view=reddit&community=LocalLLaMA&q=discussion&flair=News",
  );
  const refresh = page.getByRole("button", {
    name: "Refresh r/LocalLLaMA",
    exact: true,
  });
  await refresh.click();
  await expect
    .poll(() => state.requests)
    .toEqual([{ kind: "reddit", community: "LocalLLaMA" }]);
  await expect(refresh).toBeDisabled();
  await expect(
    page.getByRole("complementary", { name: "Background refresh" }),
  ).toContainText("Refreshing");
  await page
    .getByRole("link", {
      name: "Read Already collected discussion",
      exact: true,
    })
    .click();
  await expect(page).toHaveURL(/article=post-one/);
  await expect(
    page
      .getByText("A discussion you can keep reading.", { exact: true })
      .first(),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Refresh this post", exact: true }),
  ).toBeEnabled();
  const requestsBefore = state.postRequests;
  state.posts = [
    { ...saved, id: "new-post", title: "Fresh discussion just collected" },
    saved,
  ];
  state.jobs[0] = {
    ...state.jobs[0],
    status: "completed",
    message: "1 post saved.",
    posts_saved: 1,
    finished_at: "2026-10-22T10:01:00Z",
  };
  await expect(
    page.getByRole("button", { name: "Show updates", exact: true }),
  ).toBeVisible();
  expect(state.postRequests).toBe(requestsBefore);
  await expect(
    page.getByText("Fresh discussion just collected", { exact: true }),
  ).toHaveCount(0);
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page.locator(".post-card")).toHaveCount(1);
  await page.getByRole("button", { name: "Show updates", exact: true }).click();
  await expect(
    page.getByRole("link", {
      name: "Read Fresh discussion just collected",
      exact: true,
    }),
  ).toBeVisible();
  await expect(
    page.getByRole("complementary", { name: "Background refresh" }),
  ).toBeHidden();
});

test("uncollected post refresh targets only the post and preserves its notice until accepted", async ({
  page,
}) => {
  const state = await fixture(page);
  state.posts = [{ ...saved, html: "", excerpt: "" }];
  await page.goto(
    "/reader/?view=reddit&community=LocalLLaMA&article=post-one&article_source=reddit.com",
  );
  await expect(
    page.getByRole("heading", { name: "Full text not collected", exact: true }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Refresh this post", exact: true })
    .click();
  await expect
    .poll(() => state.requests)
    .toEqual([{ kind: "post", domain: "reddit.com", post_id: "post-one" }]);
  state.posts = [saved];
  state.jobs[0] = { ...state.jobs[0], status: "completed", posts_saved: 1 };
  await expect(
    page.getByRole("button", { name: "Show updates" }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Full text not collected", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Show updates" }).click();
  await expect(
    page
      .getByText("A discussion you can keep reading.", { exact: true })
      .first(),
  ).toBeVisible();
});

test("reload recovers a queued refresh, reports partial completion, and remembers dismissal", async ({
  page,
}) => {
  const state = await fixture(page);
  await page.goto("/reader/");
  await page
    .getByRole("button", { name: "Refresh all sources", exact: true })
    .click();
  await expect.poll(() => state.jobs.length).toBe(1);
  state.jobs[0].status = "queued";
  await page.reload();
  await expect(
    page.getByText("Waiting to refresh", { exact: true }),
  ).toBeVisible();
  state.jobs[0] = {
    ...state.jobs[0],
    status: "partial",
    message: "Reddit was blocked; other sources finished.",
    posts_saved: 2,
  };
  await expect(
    page.getByText("Refresh partly complete", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Show updates" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Dismiss refresh: all" }).click();
  await page.reload();
  await expect(
    page.getByRole("complementary", { name: "Background refresh" }),
  ).toBeHidden();
  expect(state.requests).toHaveLength(1);
});

test("submission and progress failures keep content and recovery controls available", async ({
  page,
}) => {
  const state = await fixture(page);
  state.reject = true;
  await page.goto("/reader/");
  await page
    .getByRole("button", { name: "Refresh all sources", exact: true })
    .click();
  await expect(page.getByRole("alert")).toContainText("refresh queue is full");
  await expect(
    page.getByRole("link", { name: `Read ${saved.title}`, exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Dismiss refresh error" }).click();
  state.reject = false;
  await page
    .getByRole("button", { name: "Refresh all sources", exact: true })
    .click();
  await expect.poll(() => state.jobs.length).toBe(1);
  state.statusFail = true;
  await expect(
    page.getByText(/Progress is temporarily unavailable/),
  ).toBeVisible({ timeout: 10000 });
  state.statusFail = false;
  state.jobs[0] = {
    ...state.jobs[0],
    status: "interrupted",
    message: "The server restarted. Refresh again to continue.",
  };
  await page.getByRole("button", { name: "Check again" }).click();
  await expect(
    page.getByText("Refresh interrupted", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Refresh all sources", exact: true }),
  ).toBeEnabled();
  await expect(page.getByRole("button", { name: "Show updates" })).toHaveCount(
    0,
  );
});

for (const width of [320, 1280]) {
  test(`background refresh is accessible and stays out of navigation at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 800 });
    await page.emulateMedia({ reducedMotion: "reduce" });
    await fixture(page);
    await page.goto("/reader/?view=reddit&community=LocalLLaMA");
    await page
      .getByRole("button", { name: "Refresh r/LocalLLaMA", exact: true })
      .click();
    if (width <= 760) {
      await expect(
        page.getByRole("button", { name: "Show refresh details" }),
      ).toBeVisible();
      await expect(page.locator(".refresh-notice")).toHaveCount(0);
      await page.screenshot({
        path: testInfo.outputPath(`refresh-${width}-compact.png`),
      });
      await page.getByRole("button", { name: "Show refresh details" }).click();
    }

    await expect(page.locator(".refresh-notice .refresh-spinning")).toHaveCSS(
      "animation-name",
      "none",
    );
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath(`refresh-${width}.png`),
    });
    await page.getByRole("button", { name: "Hide refresh details" }).click();
    await expect(
      page.getByText("1 refresh in progress", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("Collecting fresh posts. Keep reading while this runs.", {
        exact: true,
      }),
    ).toHaveCount(0);
    await page.getByRole("button", { name: "Show refresh details" }).click();
    await expect(
      page.getByText("Collecting fresh posts. Keep reading while this runs.", {
        exact: true,
      }),
    ).toBeVisible();
  });
}

test("linked videos explain their original source scope", async ({ page }) => {
  const state = await fixture(page);
  await page.goto("/reader/?view=youtube&feed=linked");
  const refresh = page.getByRole("button", {
    name: "Refresh linked videos",
    exact: true,
  });
  await expect(refresh).toBeDisabled();
  await expect(refresh).toHaveAttribute(
    "title",
    "Refresh the original sources to update linked videos",
  );
  expect(state.requests).toHaveLength(0);
});

for (const [url, scope] of [
  ["platform=reddit", { kind: "all", platforms: ["reddit"] }],
  [
    "platform=youtube&platform=stories&platform=reddit",
    { kind: "all", platforms: ["reddit", "rss", "youtube"] },
  ],
  [
    "platform=reddit&domain=reddit.com&tag=news",
    {
      kind: "all",
      platforms: ["reddit"],
      domains: ["reddit.com"],
      tags: ["news"],
    },
  ],
  [
    "platform=youtube&youtube_item=video%3AdQw4w9WgXcQ",
    { kind: "post", domain: "youtube.com", post_id: "video:dQw4w9WgXcQ" },
  ],
  ["view=reddit", { kind: "reddit" }],
  ["view=x&account=reporter", { kind: "x", account: "reporter" }],
  [
    "view=stories&domain=example.com&tag=news",
    { kind: "rss", domains: ["example.com"], tags: ["news"] },
  ],
  ["view=hacker_news&hn_filter=top10", { kind: "hacker_news" }],
  ["view=youtube&channel=UC123", { kind: "youtube", channel_id: "UC123" }],
  ["view=youtube&feed=hype", { kind: "youtube", feed_id: "hype" }],
  [
    "view=youtube&youtube_item=video%3AdQw4w9WgXcQ",
    { kind: "post", domain: "youtube.com", post_id: "video:dQw4w9WgXcQ" },
  ],
] as const) {
  test(`the header submits the collection scope for ${url}`, async ({
    page,
  }) => {
    const state = await fixture(page);
    await page.goto(`/reader/?${url}`);
    await page.locator(".topbar .refresh-button").click();
    await expect.poll(() => state.requests).toEqual([scope]);
  });
}

test("collapsed progress announces completion and offers updates", async ({
  page,
}) => {
  const state = await fixture(page);
  await page.goto("/reader/");
  await page
    .getByRole("button", { name: "Refresh all sources", exact: true })
    .click();
  await expect.poll(() => state.jobs.length).toBe(1);
  await page.getByRole("button", { name: "Hide refresh details" }).click();
  state.jobs[0].status = "processing";
  state.jobs[0].message = "Preparing saved posts for reading.";
  await page.getByRole("button", { name: "Show refresh details" }).click();
  await expect(
    page.getByText("Preparing updates", { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Hide refresh details" }).click();
  state.jobs[0].status = "completed";
  await expect(
    page.getByText("Refresh complete", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Show updates" }),
  ).toBeVisible();
});

test("failed update loading keeps the collected feed readable and allows retry", async ({
  page,
}) => {
  const state = await fixture(page);
  await page.goto("/reader/");
  await page
    .getByRole("button", { name: "Refresh all sources", exact: true })
    .click();
  await expect.poll(() => state.jobs.length).toBe(1);
  state.jobs[0].status = "completed";
  await expect(
    page.getByRole("button", { name: "Show updates" }),
  ).toBeVisible();
  state.postsFail = true;
  await page.getByRole("button", { name: "Show updates" }).click();
  await expect(
    page.getByText(
      "Updates could not be loaded. Your saved content is still available; try again.",
      { exact: true },
    ),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: `Read ${saved.title}`, exact: true }),
  ).toBeVisible();
  state.postsFail = false;
  state.posts = [
    { ...saved, id: "new-post", title: "Newly collected report" },
    saved,
  ];
  await page.getByRole("button", { name: "Show updates" }).click();
  await expect(
    page.getByRole("link", {
      name: "Read Newly collected report",
      exact: true,
    }),
  ).toBeVisible();
});
