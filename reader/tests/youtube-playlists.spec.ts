import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { RefreshJob, RefreshScope } from "../src/lib/refresh";
import type {
  YouTubePlaylist,
  YouTubePlaylistEntry,
} from "../src/lib/youtubePlaylists";

const path = "/reader/?view=youtube&youtube_view=playlists";
const playlist: YouTubePlaylist = {
  id: "PLsaved",
  title: "A journey through sound",
  url: "https://www.youtube.com/playlist?list=PLsaved",
  thumbnail_url: "https://i.ytimg.com/vi/aaaaaaaaaaa/hqdefault.jpg",
  channel: {
    id: "music",
    name: "Listening room",
    url: "https://www.youtube.com/@listening",
  },
  video_count: 4,
  collected_count: 4,
  status: "complete",
  complete: true,
  observed_at: "2026-10-25T12:00:00Z",
  checked_at: "2026-10-25T12:00:00Z",
  order: "Manual order",
  snapshot_id: "snapshot-one",
};
function entry(
  position: number,
  id: string,
  title: string,
  available = true,
): YouTubePlaylistEntry {
  return {
    id: `video:${id}`,
    kind: "video",
    video_id: available ? id : null,
    title,
    text: "",
    url: `https://www.youtube.com/watch?v=${id}`,
    channel: playlist.channel,
    thumbnail_url: playlist.thumbnail_url,
    duration_text: "4:15",
    views_text: "100 views",
    published_text: "",
    observed_at: playlist.observed_at,
    detail_capture: { status: "disabled" },
    discoveries: [
      {
        feed_url: playlist.url,
        feed_name: playlist.title,
        feed_kind: "playlist",
        tab: "",
        shelf: "",
        rank: position,
        region: "",
        playlist_id: playlist.id,
        observed_at: playlist.observed_at,
        run_id: "run-one",
      },
    ],
    references: [],
    reference_count: 0,
    playlist_entry_id: `entry-${position}`,
    playlist_position: position,
    available,
  };
}
async function fixture(page: Page) {
  const state = {
    playlists: [playlist],
    entries: [
      entry(1, "bbbbbbbbbbb", "Second release, first in playlist"),
      entry(2, "aaaaaaaaaaa", "First release, second in playlist"),
      entry(3, "bbbbbbbbbbb", "Second release, first in playlist"),
      entry(4, "unavailable", "Private video", false),
    ],
    fail: false,
    failMore: false,
    failLibraryMore: false,
    libraryPageSize: 30,
    libraryRequests: [] as { sort: string; cursor: string | null }[],
    pageSize: 2,
    videoRequests: [] as { q: string; cursor: string | null }[],
    scopes: [] as RefreshScope[],
    jobs: [] as RefreshJob[],
  };
  await page.route("https://i.ytimg.com/**", (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="800" height="450"><rect width="800" height="450" fill="#496e79"/><circle cx="500" cy="180" r="130" fill="#bd9e6c"/></svg>',
    }),
  );
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/refresh")) {
      if (route.request().method() === "POST") {
        const { scope } = route.request().postDataJSON() as {
          scope: RefreshScope;
        };
        state.scopes.push(scope);
        const job: RefreshJob = {
          id: "job-one",
          scope,
          label: "YouTube playlist",
          status: "completed",
          message: "Playlist checked",
          created_at: playlist.checked_at,
          finished_at: playlist.checked_at,
          posts_saved: 0,
        };
        state.jobs = [job];
        return route.fulfill({ status: 202, json: { job } });
      }
      return route.fulfill({ json: { jobs: state.jobs } });
    }
    if (url.pathname.endsWith("/youtube/sources"))
      return route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 0 },
      });
    if (url.pathname.endsWith("/youtube/playlists")) {
      const cursor = url.searchParams.get("cursor");
      const sort = url.searchParams.get("sort") ?? "recent";
      state.libraryRequests.push({ sort, cursor });
      if (state.fail || (state.failLibraryMore && cursor))
        return route.fulfill({
          status: 503,
          json: { detail: "Playlists temporarily unavailable" },
        });
      const rows = state.playlists.filter((item) =>
        item.title
          .toLowerCase()
          .includes((url.searchParams.get("q") ?? "").toLowerCase()),
      );
      if (sort === "az") rows.sort((a, b) => a.title.localeCompare(b.title));
      const offset = Number(cursor ?? 0);
      return route.fulfill({
        json: {
          playlists: rows.slice(offset, offset + state.libraryPageSize),
          total: rows.length,
          next_cursor:
            offset + state.libraryPageSize < rows.length
              ? String(offset + state.libraryPageSize)
              : null,
        },
      });
    }
    if (url.pathname.includes("/youtube/playlists/")) {
      const q = url.searchParams.get("q") ?? "";
      state.videoRequests.push({ q, cursor: url.searchParams.get("cursor") });
      const matching = state.entries.filter((item) =>
        `${item.title} ${item.channel.name} ${item.channel.url}`
          .toLowerCase()
          .includes(q.toLowerCase()),
      );
      if (state.fail || (state.failMore && url.searchParams.has("cursor")))
        return route.fulfill({
          status: 503,
          json: { detail: "Playlist videos temporarily unavailable" },
        });
      const offset = Number(url.searchParams.get("cursor") ?? 0);
      return route.fulfill({
        json: {
          playlist:
            state.playlists.find((item) =>
              url.pathname.endsWith(`/${item.id}`),
            ) ?? state.playlists[0],
          items: matching.slice(offset, offset + state.pageSize),
          total: matching.length,
          next_cursor:
            offset + state.pageSize < matching.length
              ? String(offset + state.pageSize)
              : null,
        },
      });
    }
    if (url.pathname.endsWith("/youtube"))
      return route.fulfill({
        json: {
          items: state.entries
            .filter((item) => item.id === url.searchParams.get("id"))
            .slice(0, 1),
          total: 1,
          next_cursor: null,
        },
      });
    if (url.pathname.endsWith("/sources"))
      return route.fulfill({ json: { sources: [], total_posts: 0 } });
    return route.fulfill({
      json: { posts: [], next_cursor: null, limited: false },
    });
  });
  return state;
}

test("sidebar library to ordered playlist to video and Back preserves URL and duplicate entries", async ({
  page,
}) => {
  await fixture(page);
  await page.goto("/reader/?view=youtube");
  await page.getByRole("button", { name: "Playlists", exact: true }).click();
  await expect(page).toHaveURL(/youtube_view=playlists/);
  await page.getByRole("link", { name: playlist.title, exact: true }).click();
  await expect(page).toHaveURL(/youtube_playlist=PLsaved/);
  await page.locator(".yt-playlist-entry").last().scrollIntoViewIfNeeded();
  await expect(page.locator(".yt-playlist-entry h3")).toHaveText([
    "Second release, first in playlist",
    "First release, second in playlist",
    "Second release, first in playlist",
    "Private video",
  ]);
  await expect(page.locator(".yt-playlist-position")).toHaveText([
    "1",
    "2",
    "3",
    "4",
  ]);
  await expect(page.locator(".yt-playlist-unavailable button")).toHaveCount(0);
  await page
    .getByRole("link", {
      name: "First release, second in playlist",
      exact: true,
    })
    .click();
  await expect(page).toHaveURL(/youtube_playlist=PLsaved.*youtube_item=/);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "First release, second in playlist" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Back to playlist", exact: true })
    .click();
  await expect(page).not.toHaveURL(/youtube_item=/);
  await expect(
    page.getByRole("heading", { name: playlist.title }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Back to playlists", exact: true })
    .click();
  await expect(page).toHaveURL(/youtube_view=playlists$/);
});

test("playlist refresh keeps saved order until Show updates, then loads changed order", async ({
  page,
}) => {
  const state = await fixture(page);
  state.pageSize = 10;
  await page.goto(`${path}&youtube_playlist=PLsaved`);
  await expect(page.locator(".yt-playlist-entry h3").first()).toHaveText(
    "Second release, first in playlist",
  );
  state.entries = [
    entry(1, "aaaaaaaaaaa", "First release, second in playlist"),
    entry(2, "bbbbbbbbbbb", "Second release, first in playlist"),
  ];
  await page
    .getByRole("button", { name: "Refresh this YouTube playlist" })
    .click();
  expect(state.scopes).toEqual([{ kind: "youtube", playlist_id: "PLsaved" }]);
  await expect(page.locator(".yt-playlist-entry h3").first()).toHaveText(
    "Second release, first in playlist",
  );
  await page.getByRole("button", { name: "Show updates" }).click();
  await expect(page).toHaveURL(
    /youtube_view=playlists&youtube_playlist=PLsaved/,
  );
  await expect(page.locator(".yt-playlist-entry h3")).toHaveText([
    "First release, second in playlist",
    "Second release, first in playlist",
  ]);
});

test("search, empty library, partial capture and pagination failures stay explicit", async ({
  page,
}) => {
  const state = await fixture(page);
  await page.goto(path);
  await page
    .getByRole("searchbox", { name: "Search playlists" })
    .fill("missing");
  await page
    .getByRole("searchbox", { name: "Search playlists" })
    .press("Enter");
  await expect(
    page.getByRole("heading", { name: "No playlists match" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Clear search" }).click();
  state.playlists = [{ ...playlist, complete: false, status: "partial" }];
  state.failMore = true;
  await page.getByRole("link", { name: playlist.title, exact: true }).click();
  await expect(page.getByText(/Partial playlist\. Some videos/)).toBeVisible();
  await page.locator(".yt-playlist-entry").last().scrollIntoViewIfNeeded();
  await expect(
    page
      .getByRole("alert")
      .filter({ hasText: "Playlist videos temporarily unavailable" }),
  ).toBeVisible();
  await expect(page.locator(".yt-playlist-entry")).toHaveCount(2);
  state.failMore = false;
  await page
    .getByRole("button", { name: "Retry playlist", exact: true })
    .click();
  await expect(page.locator(".yt-playlist-entry")).toHaveCount(4);
  state.playlists = [];
  await page.goto(path);
  await expect(
    page.getByRole("heading", { name: "No playlists collected yet" }),
  ).toBeVisible();
});

test("mobile playlists and videos fit and retain accessible navigation", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await fixture(page);
  await page.goto(path);
  await page.getByRole("link", { name: playlist.title, exact: true }).click();
  await expect(
    page.getByRole("heading", { name: playlist.title }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.screenshot({
    path: "test-results/youtube-playlist-mobile.png",
    fullPage: true,
  });
});

test("newly discovered playlists offer targeted collection and complete empty playlists stay distinct", async ({
  page,
}) => {
  const state = await fixture(page);
  state.playlists = [
    {
      ...playlist,
      status: "not_collected",
      complete: false,
      snapshot_id: "",
      collected_count: 0,
    },
  ];
  state.entries = [];
  await page.goto(`${path}&youtube_playlist=PLsaved`);
  await expect(
    page.getByRole("heading", { name: "No saved videos yet" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Collect playlist", exact: true })
    .click();
  expect(state.scopes).toEqual([{ kind: "youtube", playlist_id: "PLsaved" }]);
  state.playlists = [{ ...playlist, collected_count: 0, video_count: 0 }];
  await page.getByRole("button", { name: "Show updates" }).click();
  await expect(
    page.getByRole("heading", { name: "This playlist is empty" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Collect playlist", exact: true }),
  ).toHaveCount(0);
});

test("library failure can retry and playlist cards retain native background links", async ({
  page,
}) => {
  const state = await fixture(page);
  state.fail = true;
  await page.goto(path);
  await expect(
    page
      .getByRole("alert")
      .filter({ hasText: "Playlists temporarily unavailable" }),
  ).toBeVisible();
  state.fail = false;
  await page
    .getByRole("button", { name: "Retry playlists", exact: true })
    .click();
  const card = page.locator(".yt-playlist-card");
  const popupPromise = page.context().waitForEvent("page");
  await card
    .locator(".yt-playlist-owner")
    .click({ modifiers: ["ControlOrMeta"] });
  const popup = await popupPromise;
  await popup.waitForURL(/youtube_playlist=PLsaved/);
  await expect(page).toHaveURL(/youtube_view=playlists$/);
  await popup.close();
  await page.screenshot({
    path: "test-results/youtube-playlist-desktop.png",
    fullPage: true,
  });
});

test("recent library order defaults, A–Z survives navigation, and scrolling loads every page", async ({
  page,
}) => {
  const state = await fixture(page);
  state.playlists = Array.from({ length: 65 }, (_, i) => ({
    ...playlist,
    id: `PL${64 - i}`,
    title: `Playlist ${String(64 - i).padStart(3, "0")}`,
  }));
  await page.goto(path);
  const cards = page.locator(".yt-playlist-card");
  const sort = page.getByRole("combobox", { name: "Sort playlists" });
  await expect(sort).toHaveValue("recent");
  await expect(cards).toHaveCount(30);
  await expect(cards.first().locator("h3")).toHaveText("Playlist 064");
  await expect(page.getByRole("link", { name: /^View playlist/ })).toHaveCount(
    0,
  );
  await expect(
    page.getByRole("button", { name: "More playlists" }),
  ).toHaveCount(0);
  await sort.selectOption("az");
  await expect(page).toHaveURL(/playlist_sort=az/);
  await expect(cards.first().locator("h3")).toHaveText("Playlist 000");
  await cards.first().locator(".yt-playlist-artwork").click();
  await expect(
    page.getByRole("heading", { name: "Playlist 000", exact: true }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Back to playlists", exact: true })
    .click();
  await page.reload();
  await expect(sort).toHaveValue("az");
  await expect(cards.first().locator("h3")).toHaveText("Playlist 000");
  await sort.selectOption("recent");
  await expect(cards.first().locator("h3")).toHaveText("Playlist 064");
  await cards.last().scrollIntoViewIfNeeded();
  await expect(cards).toHaveCount(60);
  await cards.last().scrollIntoViewIfNeeded();
  await expect(cards).toHaveCount(65);
  await expect(cards.last().locator("h3")).toHaveText("Playlist 000");
  await expect(page.locator(".load-more")).toHaveCount(0);
  expect(
    state.libraryRequests.some((r) => r.sort === "recent" && r.cursor === "60"),
  ).toBe(true);
});

test("failed automatic playlist pagination retains cards and waits for explicit retry", async ({
  page,
}) => {
  const state = await fixture(page);
  state.playlists = Array.from({ length: 35 }, (_, i) => ({
    ...playlist,
    id: `PL${i}`,
    title: `List ${i}`,
  }));
  state.failLibraryMore = true;
  await page.goto(path);
  const cards = page.locator(".yt-playlist-card");
  await expect(cards).toHaveCount(30);
  await cards.last().scrollIntoViewIfNeeded();
  await expect(
    page
      .getByRole("alert")
      .filter({ hasText: "Playlists temporarily unavailable" }),
  ).toBeVisible();
  await expect(cards).toHaveCount(30);
  const attempts = state.libraryRequests.length;
  await page.waitForTimeout(500);
  expect(state.libraryRequests.length).toBe(attempts);
  state.failLibraryMore = false;
  await page
    .getByRole("button", { name: "Retry playlists", exact: true })
    .click();
  await expect(cards).toHaveCount(35);
});

test("playlist video search covers unloaded titles and channels, preserving order and navigation", async ({
  page,
}) => {
  const state = await fixture(page);
  state.pageSize = 30;
  state.entries = Array.from({ length: 80 }, (_, i) =>
    entry(i + 1, `v${String(i).padStart(10, "0")}`, `Video ${i + 1}`),
  );
  state.entries[69] = entry(70, "latevideo01", "A distant telescope");
  state.entries[74] = {
    ...entry(75, "latevideo02", "Second discovery"),
    channel: { ...playlist.channel, name: "Telescope Observatory" },
  };
  state.entries[79] = entry(80, "latevideo01", "A distant telescope");
  state.playlists = [{ ...playlist, collected_count: 80, video_count: 80 }];
  await page.goto(`${path}&q=journey&playlist_sort=az`);
  await page.getByRole("link", { name: playlist.title, exact: true }).click();
  await expect(
    page.getByRole("link", { name: "Back to playlists", exact: true }),
  ).toHaveCount(0);
  const search = page.getByRole("searchbox", {
    name: "Search playlist videos",
  });
  await expect(search).toHaveValue("");
  await expect(page.locator(".yt-playlist-entry")).toHaveCount(30);
  await search.fill("TELESCOPE");
  await search.press("Enter");
  await expect(page).toHaveURL(/playlist_q=TELESCOPE/);
  await expect(page.locator(".yt-playlist-position")).toHaveText([
    "70",
    "75",
    "80",
  ]);
  await expect(
    page.getByText("3 matching videos", { exact: true }),
  ).toBeVisible();
  await page
    .getByRole("link", { name: "Second discovery", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Back to playlist", exact: true })
    .click();
  await expect(search).toHaveValue("TELESCOPE");
  await page.reload();
  await expect(search).toHaveValue("TELESCOPE");
  await expect(page.locator(".yt-playlist-position")).toHaveText([
    "70",
    "75",
    "80",
  ]);
  state.pageSize = 1;
  await search.fill("distant");
  await search.press("Enter");
  await page.locator(".yt-playlist-entry").last().scrollIntoViewIfNeeded();
  await expect(page.locator(".yt-playlist-position")).toHaveText(["70", "80"]);
  expect(
    state.videoRequests.some(
      (request) => request.q === "distant" && request.cursor === "1",
    ),
  ).toBe(true);
  await search.fill("no-such-video");
  await search.press("Enter");
  await expect(
    page.getByRole("heading", { name: "No videos match", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("0 matching videos", { exact: true }),
  ).toBeVisible();
  state.pageSize = 30;
  await page.getByRole("button", { name: "Clear search", exact: true }).click();
  await expect(search).toBeFocused();
  await expect(page.locator(".yt-playlist-entry")).toHaveCount(30);
  await page
    .getByRole("button", { name: "Back to playlists", exact: true })
    .click();
  await expect(page).toHaveURL(/q=journey&playlist_sort=az$/);
  await expect(
    page.getByRole("searchbox", { name: "Search playlists" }),
  ).toHaveValue("journey");
});

for (const width of [1440, 390]) {
  test(`playlist footers sit at the card bottom and retain metadata at ${width}px`, async ({
    page,
  }) => {
    const state = await fixture(page);
    await page.route("https://www.youtube-nocookie.com/embed/**", (route) =>
      route.fulfill({
        contentType: "text/html",
        body: "<html><body>Player</body></html>",
      }),
    );
    state.pageSize = 10;
    state.entries = [
      {
        ...entry(1, "aaaaaaaaaaa", "A video with saved listing information"),
        views_text: "9.5K views",
        published_text: "2d ago",
      },
      {
        ...entry(
          2,
          "bbbbbbbbbbb",
          "A longer title that should wrap safely without moving the actions into the middle of this card",
        ),
        channel: { id: "", name: "", url: "" },
        views_text: "",
        published_text: "",
      },
    ];
    await page.setViewportSize({ width, height: 1000 });
    await page.goto(`${path}&youtube_playlist=PLsaved`);
    const cards = page.locator(".yt-playlist-entry .yt-card");
    await expect(cards).toHaveCount(2);
    await expect(cards.first().locator(".yt-video-meta")).toHaveText(
      "9.5K views · 2d ago",
    );
    await expect(cards.first().locator(".yt-video-meta")).toHaveAttribute(
      "title",
      /Captured/,
    );
    for (const card of await cards.all()) {
      const layout = await card.evaluate((node) => {
        const card = node.getBoundingClientRect();
        const footer = node
          .querySelector(".yt-card-footer")
          ?.getBoundingClientRect();
        const meta = node
          .querySelector(".yt-video-meta")
          ?.getBoundingClientRect();
        const title = node.querySelector("h3")?.getBoundingClientRect();
        if (!footer || !meta || !title)
          throw new Error("Missing card sections");
        return {
          gap: card.bottom - footer.bottom,
          ordered: title.bottom <= meta.top && meta.bottom <= footer.top,
        };
      });
      expect(layout.gap).toBeGreaterThanOrEqual(9);
      expect(layout.gap).toBeLessThan(17);
      expect(layout.ordered).toBe(true);
    }
    await cards
      .first()
      .getByRole("button", {
        name: "Play A video with saved listing information",
        exact: true,
      })
      .click();
    await expect(cards.first().locator(".yt-player-actions")).toBeVisible();
    const playing = await cards.first().evaluate((node) => {
      const card = node.getBoundingClientRect();
      const footer = node
        .querySelector(".yt-card-footer")
        ?.getBoundingClientRect();
      const controls = node
        .querySelector(".yt-player-actions")
        ?.getBoundingClientRect();
      if (!footer || !controls) throw new Error("Missing playback footer");
      return {
        gap: card.bottom - footer.bottom,
        separate: controls.bottom <= footer.top,
      };
    });
    expect(playing.gap).toBeLessThan(17);
    expect(playing.separate).toBe(true);
    await cards
      .first()
      .getByRole("button", { name: "Close video", exact: true })
      .click();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  });
}
