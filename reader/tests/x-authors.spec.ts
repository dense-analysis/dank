import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";
import type { XAuthor } from "../src/lib/xAuthors";

function post(id: string, handle = "alice"): Post {
  return {
    id,
    domain: "x.com",
    url: `https://x.com/${handle}/status/${id}`,
    author: "Shared name",
    title: `Space observation ${id}`,
    excerpt: `Space observation ${id}`,
    html: `<p>Space observation ${id}</p>`,
    created_at: "2026-10-25T12:00:00Z",
    source: "x",
    thumbnail: null,
    media: [],
    x: { handle, name: "Shared name", icon_url: null },
  };
}
const groups: XAuthor[] = [
  {
    handle: "alice",
    name: "Shared name",
    icon_url: null,
    count: 7,
    posts: [post("10"), post("11"), post("12")],
  },
  {
    handle: "bob",
    name: "Shared name",
    icon_url: null,
    count: 3,
    posts: [post("20", "bob")],
  },
];

async function fixture(page: Page) {
  const state = {
    fail: false,
    empty: false,
    paginated: false,
    authors: [] as URLSearchParams[],
    posts: [] as URLSearchParams[],
    refreshes: [] as unknown[],
  };
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/x/authors")) {
      state.authors.push(url.searchParams);
      const next = url.searchParams.has("cursor");
      await route.fulfill({
        status: state.fail ? 503 : 200,
        json: state.fail
          ? { error: "Authors unavailable" }
          : {
              authors: state.empty
                ? []
                : state.paginated
                  ? [groups[next ? 1 : 0]]
                  : groups,
              total: state.empty ? 0 : 2,
              unattributed_posts: state.empty ? 0 : 1,
              next_cursor:
                state.paginated && !next && !state.empty
                  ? "next-authors"
                  : null,
            },
      });
    } else if (url.pathname.endsWith("/youtube/sources")) {
      await route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 0 },
      });
    } else if (url.pathname.endsWith("/sources")) {
      await route.fulfill({
        json: {
          sources: [
            {
              domain: "x.com",
              name: "X",
              kind: "x",
              count: 20,
              tags: [],
              for_you_count: 10,
              accounts: [],
            },
          ],
          total_posts: 20,
        },
      });
    } else if (url.pathname.endsWith("/refresh")) {
      if (route.request().method() === "POST") {
        const scope = route.request().postDataJSON().scope;
        state.refreshes.push(scope);
        await route.fulfill({
          status: 202,
          json: {
            job: {
              id: "capture",
              scope,
              label: "X For You",
              status: "running",
              message: "Collecting",
              created_at: "2026-10-25T12:00:00Z",
              finished_at: null,
              posts_saved: 0,
            },
          },
        });
      } else await route.fulfill({ json: { jobs: [] } });
    } else if (url.pathname.endsWith("/posts")) {
      state.posts.push(url.searchParams);
      const items =
        url.searchParams.get("account") === "alice"
          ? [post("10")]
          : [post("10"), post("20", "bob")];
      await route.fulfill({
        json: { posts: items, next_cursor: null, limited: false },
      });
    } else if (url.pathname.endsWith("/post")) {
      await route.fulfill({ json: post(url.searchParams.get("id") ?? "10") });
    } else await route.fulfill({ json: {} });
  });
  return state;
}

for (const width of [390, 1280]) {
  test(`Authors counts, examples, author filter and Back at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    const state = await fixture(page);
    await page.goto(
      "/reader/?view=x&x_feed=for_you&x_tab=authors&q=space&after=2026-10-01",
    );
    await expect(
      page.getByRole("button", { name: "Authors", exact: true }),
    ).toHaveAttribute("aria-pressed", "true");
    await expect(
      page.getByRole("heading", { name: "2 authors in these recommendations" }),
    ).toBeVisible();
    const alice = page.getByRole("region", { name: "@alice", exact: true });
    await expect(
      alice.getByText("7 recommendations", { exact: true }),
    ).toBeVisible();
    await expect(alice.locator(".x-author-example")).toHaveCount(3);
    await expect(
      page.getByText("1 post has no saved author identity."),
    ).toBeVisible();
    expect(state.posts).toHaveLength(0);
    await expect(
      page.getByRole("combobox", { name: "Order stories" }),
    ).toHaveCount(0);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
    ).toBe(false);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath(`authors-${width}.png`),
    });
    await alice.locator(".x-author-example").first().click();
    await expect(page).toHaveURL(/x_tab=authors.*article=10/);
    await page.goBack();
    await expect(
      page.getByRole("heading", { name: "2 authors in these recommendations" }),
    ).toBeVisible();
    await page
      .getByRole("link", { name: "View @alice recommendations" })
      .click();
    await expect(page).toHaveURL(/account=alice/);
    expect(new URL(page.url()).searchParams.get("x_feed")).toBe("for_you");
    expect(new URL(page.url()).searchParams.get("q")).toBe("space");
    expect(new URL(page.url()).searchParams.get("after")).toBe("2026-10-01");
    await expect(page.locator(".x-post-card")).toHaveCount(1);
    await page
      .getByRole("button", {
        name: "Refresh X For You recommendations",
        exact: true,
      })
      .click();
    expect(state.refreshes).toEqual([{ kind: "x", feed_id: "for_you" }]);
    await page
      .getByRole("button", { name: "Remove recommended author filter" })
      .click();
    await expect(page.locator(".x-post-card")).toHaveCount(2);
    expect(new URL(page.url()).searchParams.get("x_feed")).toBe("for_you");
  });
}

test("Authors can be opened from Posts and have explicit empty and retry states", async ({
  page,
}) => {
  const state = await fixture(page);
  state.fail = true;
  await page.goto("/reader/?view=x&x_feed=for_you");
  await page.getByRole("button", { name: "Authors", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Authors unavailable");
  await expect(
    page.getByRole("heading", { name: "No authors match" }),
  ).toHaveCount(0);
  state.fail = false;
  state.empty = true;
  await page.getByRole("button", { name: "Retry authors" }).click();
  await expect(
    page.getByRole("heading", { name: "No authors match" }),
  ).toBeVisible();
});

test("Authors paginate independently of Posts with no duplicate groups", async ({
  page,
}) => {
  const state = await fixture(page);
  state.paginated = true;
  await page.goto("/reader/?view=x&x_feed=for_you&x_tab=authors");
  await expect(page.locator(".x-author-group")).toHaveCount(2);
  expect(
    state.authors.some((query) => query.get("cursor") === "next-authors"),
  ).toBe(true);
  expect(state.posts).toHaveLength(0);
  await expect(page.getByRole("button", { name: "More authors" })).toHaveCount(
    0,
  );
});
