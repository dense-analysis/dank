import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post, Source } from "../src/lib/types";

const blog: Post = {
  id: "blog",
  domain: "example.com",
  url: "https://example.com/story",
  author: "Shared display name",
  title: "Blog analysis",
  excerpt: "Collected analysis from a publication.",
  html: "<p>Collected analysis from a publication.</p>",
  created_at: "2026-10-16T10:00:00Z",
  source: "rss",
  thumbnail: null,
  media: [],
};
const dispatches: Post[] = Array.from({ length: 4 }, (_, index) => ({
  ...blog,
  id: `dispatch-${index}`,
  domain: "x.com",
  url: `https://x.com/reporter/status/dispatch-${index}`,
  title: `City dispatch ${index + 1}`,
  source: "x",
}));
const observer: Post = {
  ...dispatches[0],
  id: "observer",
  url: "https://x.com/observer/status/observer",
  title: "City dispatch from another account",
};
const reddit: Post = {
  ...blog,
  id: "reddit",
  domain: "reddit.com",
  url: "https://www.reddit.com/r/LocalLLaMA/comments/reddit/",
  title: "Community discussion",
  source: "reddit",
  reddit: { community: "LocalLLaMA", score: 12, comments: 3, flair: null },
};
const posts = [blog, dispatches[0], reddit, observer, ...dispatches.slice(1)];
const sources: Source[] = [
  {
    domain: "example.com",
    name: "Example News",
    kind: "rss",
    count: 1,
    tags: ["news"],
  },
  {
    domain: "x.com",
    name: "X",
    kind: "x",
    count: 5,
    tags: ["social"],
    accounts: [
      {
        handle: "reporter",
        name: "City Reporter",
        count: 4,
        icon_url: "https://pbs.twimg.com/profile_images/123/reporter.jpg",
      },
      {
        handle: "observer",
        name: "City Observer",
        count: 1,
        icon_url: "/assets/observer.svg",
      },
      {
        handle: "quiet_account",
        name: "No posts yet",
        count: 0,
        icon_url: null,
      },
      {
        handle: "broken",
        name: "Unavailable icon",
        count: 0,
        icon_url: "https://pbs.twimg.com/profile_images/123/broken.jpg",
      },
      {
        handle: "untrusted",
        name: "Untrusted icon",
        count: 0,
        icon_url: "https://untrusted.example/icon.svg",
      },
    ],
  },
  {
    domain: "reddit.com",
    name: "Reddit",
    kind: "reddit",
    count: 1,
    tags: ["community"],
    communities: [{ name: "LocalLLaMA", count: 1 }],
  },
];

async function mockCollections(page: Page, xEnabled = true) {
  const state = { requests: [] as URL[], pageSize: posts.length };
  const icon = {
    contentType: "image/svg+xml",
    body: '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"><rect width="32" height="32" fill="#538573"/></svg>',
  };
  await page.route("**/assets/observer.svg", (route) => route.fulfill(icon));
  await page.route("https://pbs.twimg.com/**", (route) =>
    route.fulfill(
      route.request().url().endsWith("broken.jpg") ? { status: 404 } : icon,
    ),
  );
  await page.route("https://untrusted.example/**", (route) => route.abort());
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    state.requests.push(url);

    if (url.pathname.endsWith("/sources")) {
      await route.fulfill({
        json: {
          sources: sources.filter((source) => xEnabled || source.kind !== "x"),
          total_posts: posts.length,
        },
      });
      return;
    }

    if (url.pathname.endsWith("/post")) {
      await route.fulfill({
        json: posts.find(
          (post) =>
            post.id === params.get("id") &&
            post.domain === params.get("domain"),
        ),
      });
      return;
    }

    const selected = posts.filter((post) => {
      const kind = params.get("kind");
      const domains = params.getAll("domain");
      const account = new URL(post.url).pathname.split("/")[1].toLowerCase();

      return (
        (kind === "all" ||
          (kind === "stories"
            ? post.source !== "x" && post.source !== "reddit"
            : post.source === kind)) &&
        (!domains.length || domains.includes(post.domain)) &&
        (!params.get("account") || account === params.get("account")) &&
        (!params.get("community") ||
          post.reddit?.community === params.get("community")) &&
        (!params.get("author") || post.author === params.get("author")) &&
        (!params.get("q") ||
          post.title
            .toLowerCase()
            .includes(params.get("q")?.toLowerCase() ?? ""))
      );
    });
    const offset = Number(params.get("cursor") ?? 0);
    const end = offset + state.pageSize;
    await route.fulfill({
      json: {
        posts: selected.slice(offset, end),
        next_cursor: end < selected.length ? String(end) : null,
        limited: false,
      },
    });
  });

  return state;
}

function story(page: Page, post: Post) {
  return page.getByRole("link", { name: `Read ${post.title}`, exact: true });
}

async function noOverflow(page: Page) {
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
}

test("All mixes collections while Blogs, X and Reddit keep their own feeds", async ({
  page,
}) => {
  const state = await mockCollections(page);
  await page.goto("/reader/");
  const nav = page.getByRole("navigation", { name: "Main navigation" });
  await expect(story(page, blog)).toBeVisible();
  await expect(story(page, dispatches[0])).toBeVisible();
  await expect(story(page, reddit)).toBeVisible();
  await expect(
    nav.getByRole("button", { name: "All", exact: true }),
  ).toHaveAttribute("aria-current", "page");
  await expect(
    page.getByRole("textbox", { name: "Search everything" }),
  ).toBeVisible();

  for (const [label, kind, count] of [
    ["Blogs", "stories", 1],
    ["X", "x", 5],
    ["Reddit", "reddit", 1],
  ] as const) {
    await nav.getByRole("button", { name: label, exact: true }).click();
    await expect(page.locator(".post-card")).toHaveCount(count);
    await expect(
      page.getByRole("heading", { name: label, exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("textbox", {
        name: `Search ${label === "Blogs" ? "blogs" : label}`,
      }),
    ).toBeVisible();
    expect(new URL(page.url()).searchParams.get("view")).toBe(kind);
    await expect(
      nav.getByRole("button", { name: label, exact: true }),
    ).toHaveAttribute("aria-current", "page");
    await expect(
      story(
        page,
        kind === "stories" ? blog : kind === "x" ? dispatches[0] : reddit,
      ),
    ).toBeVisible();
    await expect(
      story(page, kind === "stories" ? dispatches[0] : blog),
    ).toHaveCount(0);
  }

  expect([
    ...new Set(
      state.requests
        .filter((url) => url.pathname.endsWith("/posts"))
        .map((url) => url.searchParams.get("kind")),
    ),
  ]).toEqual(["all", "stories", "x", "reddit"]);
});

test("X accounts have accessible disclosure, safe icons and initial fallbacks", async ({
  page,
}) => {
  const untrustedRequests: string[] = [];
  page.on("request", (request) => {
    if (request.url().startsWith("https://untrusted.example/"))
      untrustedRequests.push(request.url());
  });
  await mockCollections(page);
  await page.goto("/reader/");
  const nav = page.getByRole("navigation", { name: "Main navigation" });
  const reporter = nav.getByRole("button", {
    name: "@reporter, 4 collected posts",
    exact: true,
  });
  await expect(reporter).toBeVisible();
  await expect(reporter).toHaveAttribute("title", "City Reporter (@reporter)");
  await expect(reporter.locator("img")).toHaveJSProperty("naturalWidth", 32);
  await expect(reporter.locator("img")).toHaveAttribute("alt", "");
  await expect(
    nav.getByRole("button", { name: /@observer,/ }).locator("img"),
  ).toHaveJSProperty("naturalWidth", 32);

  for (const [handle, initial] of [
    ["quiet_account", "Q"],
    ["broken", "B"],
    ["untrusted", "U"],
  ]) {
    const account = nav.getByRole("button", {
      name: `@${handle}, 0 collected posts`,
      exact: true,
    });
    await expect(account.locator(".identity-avatar")).toHaveText(initial);
    await expect(account.locator("img")).toHaveCount(0);
  }

  expect(untrustedRequests).toEqual([]);
  const toggle = nav.getByRole("button", { name: "Collapse X accounts" });
  await expect(toggle).toHaveAttribute("aria-expanded", "true");
  const controls = await toggle.getAttribute("aria-controls");
  expect(controls).toBeTruthy();
  await toggle.focus();
  await toggle.press("Enter");
  await expect(
    nav.getByRole("button", { name: "Expand X accounts" }),
  ).toHaveAttribute("aria-expanded", "false");
  await expect(page.locator(`[id="${controls}"]`)).toBeHidden();
  await expect(reporter).toBeHidden();
  await nav.getByRole("button", { name: "Expand X accounts" }).press("Space");
  await reporter.click();
  await expect(reporter).toHaveAttribute("aria-current", "page");
  await expect(
    nav.getByRole("button", { name: "X", exact: true }),
  ).not.toHaveAttribute("aria-current", "page");
  await expect(
    page.getByRole("heading", { name: "@reporter", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Search @reporter" }),
  ).toBeVisible();
  expect(
    (await new AxeBuilder({ page }).analyze()).violations.filter((item) =>
      ["serious", "critical"].includes(item.impact ?? ""),
    ),
  ).toEqual([]);
});

test("account search, author, loaded pages and history survive article navigation and refresh", async ({
  page,
}) => {
  const state = await mockCollections(page);
  state.pageSize = 2;
  await page.addInitScript(() => {
    Reflect.deleteProperty(window, "IntersectionObserver");
  });
  await page.goto(
    "/reader/?view=x&account=REPORTER&after=2026-10-01&before=2026-10-31",
  );
  await expect(page.locator(".post-card")).toHaveCount(2);
  await page
    .getByRole("textbox", { name: "Search @reporter" })
    .fill("dispatch");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await page
    .getByRole("combobox", { name: "Order stories" })
    .selectOption("oldest");
  await page
    .locator(".post-card")
    .first()
    .getByRole("link", { name: "Stories by Shared display name", exact: true })
    .click();
  await expect(page.locator(".post-card")).toHaveCount(2);
  await page.getByRole("button", { name: "More stories", exact: true }).click();
  await expect(page.locator(".post-card")).toHaveCount(4);
  await expect(story(page, observer)).toHaveCount(0);
  const timeline = page.url();
  const params = new URL(timeline).searchParams;
  expect(Object.fromEntries(params)).toMatchObject({
    view: "x",
    account: "reporter",
    q: "dispatch",
    sort: "oldest",
    author: "Shared display name",
    author_match: "exact",
    after: "2026-10-01",
    before: "2026-10-31",
  });
  const lastPage = state.requests
    .filter((url) => url.pathname.endsWith("/posts"))
    .at(-1);
  expect(Object.fromEntries(lastPage?.searchParams ?? [])).toMatchObject({
    kind: "x",
    account: "reporter",
    cursor: "2",
    author: "Shared display name",
  });
  const target = story(page, dispatches[3]);
  await target.click();
  const article = page.getByRole("main", { name: "Article reader" });
  await expect(article).toBeVisible();
  await page.goBack();
  await expect(page).toHaveURL(timeline);
  await expect(page.locator(".post-card")).toHaveCount(4);
  await expect(target).toBeFocused();
  await page.goForward();
  await expect(article).toBeVisible();
  await page.reload();
  await expect(
    article.getByRole("heading", { name: dispatches[3].title, exact: true }),
  ).toBeVisible();
  await article
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page).toHaveURL(timeline);
  await expect(
    page.getByRole("textbox", { name: "Search @reporter" }),
  ).toHaveValue("dispatch");
  await expect(page.locator(".post-card")).toHaveCount(2);
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: /@observer,/ })
    .click();
  await expect(story(page, observer)).toBeVisible();
  await expect(page.locator(".post-card")).toHaveCount(1);
  const switched = state.requests
    .filter((url) => url.pathname.endsWith("/posts"))
    .at(-1)?.searchParams;
  expect(switched?.get("account")).toBe("observer");
  expect(switched?.has("cursor")).toBe(false);
  expect(switched?.has("author")).toBe(false);
  expect(switched?.has("q")).toBe(false);
});

test("configured accounts without posts remain selectable and X can be absent", async ({
  page,
}) => {
  await mockCollections(page);
  await page.goto("/reader/");
  const nav = page.getByRole("navigation", { name: "Main navigation" });
  await nav
    .getByRole("button", {
      name: "@quiet_account, 0 collected posts",
      exact: true,
    })
    .click();
  await expect(
    page.getByRole("heading", { name: "@quiet_account", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "No collected posts found" }),
  ).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Search @quiet_account" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Show all X posts" }).click();
  await expect(page.locator(".post-card")).toHaveCount(5);
  expect(new URL(page.url()).searchParams.has("account")).toBe(false);
  await mockCollections(page, false);
  await page.reload();
  await expect(nav.getByRole("button", { name: "X", exact: true })).toHaveCount(
    0,
  );
  await expect(nav.getByRole("button", { name: /X accounts/ })).toHaveCount(0);
});

for (const width of [1440, 390, 320]) {
  test(`collection navigation and X reading fit at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 844 });
    await mockCollections(page);
    await page.goto("/reader/");

    if (width < 600)
      await page
        .getByRole("button", { name: "Open navigation", exact: true })
        .click();

    const nav = page.getByRole("navigation", { name: "Main navigation" });
    await expect(
      nav.getByRole("button", { name: /@quiet_account,/ }),
    ).toBeVisible();
    await noOverflow(page);
    await page.screenshot({
      path: testInfo.outputPath(`collections-navigation-${width}.png`),
    });
    await nav.getByRole("button", { name: /@reporter,/ }).click();
    await expect(
      page.getByRole("dialog", { name: "Navigation", exact: true }),
    ).toHaveCount(0);
    await expect(page.locator(".post-card")).toHaveCount(4);
    await expect(
      page.getByRole("heading", { name: "@reporter", exact: true }),
    ).toBeVisible();
    await noOverflow(page);
    await page.screenshot({
      path: testInfo.outputPath(`x-account-${width}.png`),
      fullPage: true,
    });
    await story(page, dispatches[0]).click();
    await expect(
      page.getByRole("main", { name: "Article reader" }),
    ).toBeVisible();
    await noOverflow(page);
    await page
      .getByRole("button", { name: "Back to stories", exact: true })
      .click();
    await expect(
      page.getByRole("heading", { name: "@reporter", exact: true }),
    ).toBeVisible();
    await noOverflow(page);
  });
}

test("source links and source rows select their collection", async ({
  page,
}) => {
  await mockCollections(page);

  for (const [post, view] of [
    [blog, "stories"],
    [dispatches[0], "x"],
  ] as const) {
    await page.goto("/reader/");
    const card = page.locator(".post-card").filter({ has: story(page, post) });
    const link = card.locator(".source-link");
    await expect(link).toHaveAttribute("href", new RegExp(`view=${view}`));
    await link.click();
    expect(new URL(page.url()).searchParams.get("view")).toBe(view);
    expect(new URL(page.url()).searchParams.getAll("domain")).toEqual([
      post.domain,
    ]);
    await expect(
      story(page, post.source === "x" ? blog : dispatches[0]),
    ).toHaveCount(0);
    await story(page, post).click();
    const article = page.getByRole("main", { name: "Article reader" });
    await article.locator(".source-link").click();
    await expect(article).toHaveCount(0);
    expect(new URL(page.url()).searchParams.get("view")).toBe(view);
    await page.goto("/reader/?view=sources");
    await page.locator(".source-row").filter({ hasText: post.domain }).click();
    expect(new URL(page.url()).searchParams.get("view")).toBe(view);
    await expect(story(page, post)).toBeVisible();
  }
});
