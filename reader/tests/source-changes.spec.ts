import { expect, type Page, test } from "@playwright/test";
import type { Source } from "../src/lib/types";

// The interval the reader checks the saved sources at.
const POLL_MS = 5000;

function saved(id: number, domain: string) {
  return {
    id: `00000000-0000-0000-0000-00000000000${id}`,
    enabled: true,
    domain,
    accounts: [],
    feeds: [],
    tags: [],
    adapters: null,
    reddit: null,
    hacker_news: null,
  };
}

function inSidebar(domain: string): Source {
  return {
    domain,
    name: domain,
    kind: "rss",
    count: 1,
    tags: [],
    configured: true,
  };
}

/**
 * A server whose saved sources another tab can edit. The first `failing`
 * requests for the source list answer `failStatus`.
 */
async function fixture(
  page: Page,
  {
    inDatabase,
    failing = 0,
    failStatus = 503,
  }: { inDatabase: boolean; failing?: number; failStatus?: number },
) {
  const state = { edited: false, settingsRequests: 0, sourcesRequests: 0 };
  await page.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());
    const sources = state.edited
      ? [saved(1, "first.example"), saved(2, "second.example")]
      : [saved(1, "first.example")];

    if (url.pathname === "/api/reader/categories") {
      return route.fulfill({ json: { categories: [] } });
    }

    if (url.pathname.endsWith("/source-settings")) {
      state.settingsRequests += 1;

      return state.settingsRequests > failing
        ? route.fulfill({ json: { sources, in_database: inDatabase } })
        : route.fulfill({ status: failStatus, json: { error: "No" } });
    }

    if (url.pathname.endsWith("/source-icon")) {
      return route.fulfill({ status: 404 });
    }

    if (url.pathname.endsWith("/youtube/sources")) {
      return route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 0 },
      });
    }

    if (url.pathname.endsWith("/sources")) {
      state.sourcesRequests += 1;

      return route.fulfill({
        json: {
          sources: sources.map((source) => inSidebar(source.domain)),
          total_posts: sources.length,
          blog_external_count: 0,
          in_database: inDatabase,
        },
      });
    }

    return route.fulfill({
      json: { posts: [], next_cursor: null, limited: false },
    });
  });
  await page.clock.install();
  await page.goto("/reader/");
  await expect(source(page, "first.example")).toBeVisible();
  await expect.poll(() => state.settingsRequests).toBe(inDatabase ? 1 : 0);
  // Let the first answer reach the query cache.
  await page.clock.runFor(100);

  return state;
}

function source(page: Page, domain: string) {
  return page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: `${domain}, 1 collected posts` });
}

test("a source saved in another tab reaches the sidebar within five seconds", async ({
  page,
}) => {
  const state = await fixture(page, { inDatabase: true });
  const loaded = state.sourcesRequests;

  await page.clock.runFor(POLL_MS);
  await expect.poll(() => state.settingsRequests).toBe(2);
  // Nothing changed, so the sidebar's own heavier request isn't repeated.
  expect(state.sourcesRequests).toBe(loaded);

  state.edited = true;
  await page.clock.runFor(POLL_MS);
  await expect(source(page, "second.example")).toBeVisible();
});

test("coming back to the tab checks for source edits at once", async ({
  page,
}) => {
  const state = await fixture(page, { inDatabase: true });

  state.edited = true;
  // The browser's own event bubbles from the document to the window.
  await page.evaluate(() => {
    document.dispatchEvent(new Event("visibilitychange", { bubbles: true }));
  });
  // Well before the next poll is due.
  await expect.poll(() => state.settingsRequests, { timeout: 1000 }).toBe(2);
  await page.clock.runFor(100);
  await expect(source(page, "second.example")).toBeVisible();
});

test("config.toml's sources are not watched, since they change only on restart", async ({
  page,
}) => {
  const state = await fixture(page, { inDatabase: false });

  await page.clock.runFor(3 * POLL_MS);
  expect(state.settingsRequests).toBe(0);
});

test("a server without saved sources is asked once", async ({ page }) => {
  // The aiohttp server reads the sources table but has no source-settings
  // route until DEN-51.
  const state = await fixture(page, {
    inDatabase: true,
    failing: Number.POSITIVE_INFINITY,
    failStatus: 404,
  });

  await page.clock.runFor(3 * POLL_MS);
  expect(state.settingsRequests).toBe(1);
  await expect(source(page, "first.example")).toBeVisible();
});

test("a passing outage on the first check doesn't stop later ones", async ({
  page,
}) => {
  const state = await fixture(page, { inDatabase: true, failing: 1 });

  state.edited = true;
  await page.clock.runFor(POLL_MS);
  await expect(source(page, "second.example")).toBeVisible();
});
