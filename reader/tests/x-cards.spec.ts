import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const dispatch: Post = {
  id: "one",
  domain: "x.com",
  url: "https://x.com/long_handle_123/status/one",
  author: "Shared display name",
  title: "A collected X dispatch",
  excerpt: "A dispatch from an account outside the configured sidebar.",
  html: "<p>The captured dispatch.</p>",
  created_at: "2026-10-17T12:00:00Z",
  source: "x",
  x: {
    handle: "long_handle_123",
    name: "Shared display name",
    icon_url: "https://pbs.twimg.com/profile_images/1/avatar.jpg",
  },
  thumbnail: null,
  media: [],
};
const posts: Post[] = [
  dispatch,
  {
    ...dispatch,
    id: "missing",
    url: "https://x.com/missing/status/missing",
    title: "No captured profile image",
    x: { handle: "missing", name: "Shared display name", icon_url: null },
  },
  {
    ...dispatch,
    id: "broken",
    url: "https://x.com/broken/status/broken",
    title: "Unavailable profile image",
    x: {
      handle: "broken",
      name: "Shared display name",
      icon_url: "https://pbs.twimg.com/profile_images/2/broken.jpg",
    },
  },
  {
    ...dispatch,
    id: "blog",
    domain: "example.com",
    url: "https://example.com/article",
    title: "Blog article",
    source: "rss",
    x: undefined,
  },
];

async function mockCards(page: Page) {
  await page.context().route("https://pbs.twimg.com/**", (route) =>
    route.fulfill(
      route.request().url().endsWith("broken.jpg")
        ? { status: 404 }
        : {
            contentType: "image/svg+xml",
            body: '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"><rect width="32" height="32" fill="#538573"/></svg>',
          },
    ),
  );
  await page.context().route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;

    if (url.pathname.endsWith("/sources")) {
      await route.fulfill({
        json: {
          sources: [
            {
              domain: "x.com",
              name: "X",
              kind: "x",
              count: 3,
              tags: [],
              accounts: [],
            },
          ],
          total_posts: posts.length,
        },
      });
      return;
    }

    if (url.pathname.endsWith("/post")) {
      await route.fulfill({
        json: posts.find((post) => post.id === params.get("id")),
      });
      return;
    }

    await route.fulfill({
      json: {
        posts: posts.filter(
          (post) =>
            (params.get("kind") === "all" ||
              post.source === params.get("kind")) &&
            (!params.get("account") ||
              post.x?.handle === params.get("account")),
        ),
        next_cursor: null,
        limited: false,
      },
    });
  });
}

function card(page: Page, title: string) {
  return page.locator(".post-card").filter({
    has: page.getByRole("link", { name: `Read ${title}`, exact: true }),
  });
}

for (const width of [1440, 390, 320]) {
  test(`X cards show their own identity in All, X and account feeds at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    await mockCards(page);

    for (const query of ["", "view=x", "view=x&account=long_handle_123"]) {
      await page.goto(`/reader/?${query}`);
      const identity = card(page, dispatch.title).getByRole("link", {
        name: "Posts from @long_handle_123",
        exact: true,
      });
      await expect(identity).toBeVisible();
      await expect(identity).toContainText("Shared display name");
      await expect(identity).toContainText("@long_handle_123");
      await expect(identity).toHaveAttribute(
        "href",
        "/reader/?view=x&account=long_handle_123",
      );
      await expect(identity.locator("img")).toHaveJSProperty(
        "naturalWidth",
        32,
      );
      await expect(identity.locator("img")).toHaveAttribute("alt", "");
      await expect(
        card(page, dispatch.title).getByRole("link", {
          name: "Stories from x",
          exact: true,
        }),
      ).toHaveCount(0);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);

      if (!query) {
        await expect(
          card(page, "Blog article").getByRole("link", {
            name: "Stories from example",
            exact: true,
          }),
        ).toBeVisible();

        for (const handle of ["missing", "broken"]) {
          const fallback = page
            .locator(".post-card")
            .getByRole("link", { name: `Posts from @${handle}`, exact: true });
          await expect(fallback.locator(".account-avatar")).toHaveText(
            handle[0].toUpperCase(),
          );
          await expect(fallback.locator("img")).toHaveCount(0);
        }

        await page.screenshot({
          path: testInfo.outputPath(`x-card-all-${width}.png`),
          fullPage: true,
        });
      }
    }

    expect(
      (await new AxeBuilder({ page }).analyze()).violations.filter((item) =>
        ["serious", "critical"].includes(item.impact ?? ""),
      ),
    ).toEqual([]);
  });
}

test("card identity supports keyboard and new-tab navigation without confusing display names", async ({
  page,
  context,
}) => {
  await mockCards(page);
  await page.goto("/reader/?q=dispatch&sort=oldest");
  const origin = page.url();
  const link = card(page, dispatch.title).getByRole("link", {
    name: "Posts from @long_handle_123",
    exact: true,
  });
  const newPagePromise = context.waitForEvent("page");
  await link.click({ button: "middle" });
  const opened = await newPagePromise;
  await opened.waitForURL("**/reader/?view=x&account=long_handle_123");
  await opened.close();
  await expect(page).toHaveURL(origin);
  await link.focus();
  await link.press("Enter");
  await expect(page).toHaveURL(/view=x&account=long_handle_123$/);
  await expect(page.locator(".post-card")).toHaveCount(1);
  await expect(
    page.getByRole("heading", { name: "@long_handle_123", exact: true }),
  ).toBeVisible();
  await page.goBack();
  await expect(page).toHaveURL(origin);
  await expect(card(page, dispatch.title)).toBeVisible();
});

for (const width of [1440, 390, 320]) {
  test(`X card space opens the post in every feed at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await mockCards(page);

    for (const query of ["", "view=x", "view=x&account=long_handle_123"]) {
      await page.goto(`/reader/?${query}`);
      const originalUrl = page.url();
      const post = card(page, dispatch.title);
      const timestamp = post.getByRole("link", {
        name: `Read ${dispatch.title}`,
        exact: true,
      });
      await expect(post).toBeVisible();

      for (const target of [
        post,
        post.locator(".article-body"),
        post.locator(".article-body p"),
        post.locator(".x-post-text-link"),
        post.getByTitle("Likes not captured", { exact: true }),
      ]) {
        await expect(target).toHaveCSS("cursor", "pointer");
      }

      await post.click({ position: { x: 8, y: 8 } });
      await expect(
        page.getByRole("main", { name: "X post reader" }),
      ).toBeVisible();
      await expect(page).toHaveURL(/article=one(?:&|$)/);
      await page
        .getByRole("button", { name: "Back to stories", exact: true })
        .click();
      await expect(page).toHaveURL(originalUrl);
      await expect(timestamp).toBeFocused();
    }
  });
}

test("non-link engagement metrics open the post once", async ({ page }) => {
  await mockCards(page);
  await page.goto("/reader/?view=x");
  const post = card(page, dispatch.title);
  await post
    .getByTitle("Likes not captured", { exact: true })
    .locator("svg")
    .click();
  await expect(page.getByRole("main", { name: "X post reader" })).toBeVisible();
  await page.goBack();
  await expect(page).toHaveURL(/view=x$/);
  await expect(post).toBeVisible();
});

test("dragging text selection into card space keeps the feed open", async ({
  page,
}) => {
  await mockCards(page);
  await page.goto("/reader/?view=x");
  const originalUrl = page.url();
  const post = card(page, dispatch.title);
  const start = await post.locator(".article-body p").evaluate((element) => {
    const range = document.createRange();
    range.selectNodeContents(element);
    const box = range.getBoundingClientRect();

    return { x: box.x + 2, y: box.y + box.height / 2 };
  });
  const box = await post.boundingBox();
  expect(box).not.toBeNull();
  if (!box) throw new Error("Missing card bounds");

  await page.mouse.move(start.x, start.y);
  await page.mouse.down();
  await page.mouse.move(box.x + 8, box.y + box.height - 8, { steps: 12 });
  await page.mouse.up();
  await expect
    .poll(() => page.evaluate(() => window.getSelection()?.toString()))
    .toContain("captured dispatch");
  await expect(page).toHaveURL(originalUrl);
  await expect(page.getByRole("main", { name: "X post reader" })).toHaveCount(
    0,
  );
});

test("Cmd/Ctrl-clicking card space and engagement opens a separate reader tab", async ({
  page,
  context,
}) => {
  await mockCards(page);
  await page.goto(
    "/reader/?view=x&account=long_handle_123&q=dispatch&sort=oldest&after=2026-10-01",
  );
  const originalUrl = page.url();
  const post = card(page, dispatch.title);

  for (const target of ["space", "engagement"] as const) {
    const [tab] = await Promise.all([
      context.waitForEvent("page"),
      target === "space"
        ? post.click({ position: { x: 8, y: 8 }, modifiers: ["ControlOrMeta"] })
        : post
            .getByTitle("Likes not captured", { exact: true })
            .locator("svg")
            .click({ modifiers: ["ControlOrMeta"] }),
    ]);
    await expect(
      tab.getByRole("main", { name: "X post reader" }),
    ).toBeVisible();
    expect(Object.fromEntries(new URL(tab.url()).searchParams)).toMatchObject({
      view: "x",
      account: "long_handle_123",
      q: "dispatch",
      sort: "oldest",
      after: "2026-10-01",
      article: dispatch.id,
      article_source: dispatch.domain,
    });
    expect(await tab.evaluate(() => window.opener === null)).toBe(true);
    await tab.close();
    await expect(page).toHaveURL(originalUrl);
    await expect(post).toBeVisible();
    await expect(page.getByRole("main", { name: "X post reader" })).toHaveCount(
      0,
    );
    expect(context.pages()).toHaveLength(1);
  }
});
