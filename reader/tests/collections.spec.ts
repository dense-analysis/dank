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
  html: `<p>City dispatch ${index + 1}</p>`,
  source: "x",
  x: { handle: "reporter", name: "City Reporter", icon_url: null },
}));
const observer: Post = {
  ...dispatches[0],
  id: "observer",
  url: "https://x.com/observer/status/observer",
  title: "City dispatch from another account",
  html: "<p>City dispatch from another account</p>",
  x: { handle: "observer", name: "City Observer", icon_url: null },
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
        total_posts: 10161,
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

for (const width of [320, 390, 1280]) {
  test(`account header keeps identity, collection totals and overview navigation at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 800 });
    await mockCollections(page);
    await page.goto(
      "/reader/?view=x&account=reporter&q=dispatch&author=Shared+display+name",
    );
    const header = page.locator(".topbar");
    await expect(
      header.getByRole("heading", { name: "City Reporter", exact: true }),
    ).toBeVisible();
    await expect(header.getByText("@reporter", { exact: true })).toBeVisible();
    await expect(header.locator(".collection-count-values")).toHaveText(
      "4/10.1Kposts",
    );
    await expect(header.locator(".collection-count-pill")).toHaveAttribute(
      "title",
      "4 posts collected / 10,161 total posts reported by X at last capture",
    );
    const name = await header.locator("h1").boundingBox();
    const handle = await header.locator(".collection-handle").boundingBox();
    const count = await header.locator(".collection-count-pill").boundingBox();
    expect(name).not.toBeNull();
    expect(handle).not.toBeNull();
    expect(count).not.toBeNull();
    expect(Math.abs((name?.y ?? 0) - (handle?.y ?? 0))).toBeLessThan(5);
    expect((handle?.x ?? 0) + (handle?.width ?? 0)).toBeLessThan(count?.x ?? 0);
    await noOverflow(page);
    await page.evaluate(() => window.scrollTo(0, 300));
    expect((await header.boundingBox())?.y).toBe(0);
    await expect(header.locator(".collection-count-values")).toHaveText(
      "4/10.1Kposts",
    );
    await page.screenshot({
      path: testInfo.outputPath(`account-header-${width}.png`),
    });
    await header
      .getByRole("button", { name: "Back to X posts", exact: true })
      .click();
    await expect(page).toHaveURL(/\/reader\/\?view=x$/);
    await expect(page.locator(".post-card")).toHaveCount(5);
    await expect(
      page.getByRole("textbox", { name: "Search X", exact: true }),
    ).toHaveValue("");
  });
}

test("collection headers show honest missing totals and return to the Reddit overview", async ({
  page,
}) => {
  await mockCollections(page);
  await page.goto("/reader/?view=x&account=quiet_account");
  const header = page.locator(".topbar");
  await expect(header.locator(".collection-count-values")).toHaveText(
    "0collected",
  );
  await expect(header.locator(".collection-count-pill")).toHaveAttribute(
    "title",
    "0 posts collected",
  );
  await page.goto("/reader/?view=reddit&community=LocalLLaMA");
  await expect(header.locator(".collection-count-values")).toHaveText(
    "1collected",
  );
  await header
    .getByRole("button", { name: "Back to Reddit", exact: true })
    .click();
  await expect(page).toHaveURL(/\/reader\/\?view=reddit$/);
  await expect(
    page.getByRole("textbox", { name: "Search Reddit", exact: true }),
  ).toBeVisible();
});

async function mockCollections(
  page: Page,
  xEnabled = true,
  availableSources = sources,
) {
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
          sources: availableSources.filter(
            (source) => xEnabled || source.kind !== "x",
          ),
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
    name: /City Reporter.*@reporter.*4 collected posts/,
    exact: true,
  });
  await expect(reporter).toBeVisible();
  await expect(reporter).toHaveAttribute("title", "City Reporter (@reporter)");
  await expect(reporter.locator(".collection-name")).toHaveText(
    "City Reporter",
  );
  await expect(reporter).not.toHaveText(/@reporter/);
  await expect(reporter.locator("img")).toHaveJSProperty("naturalWidth", 32);
  await expect(reporter.locator("img")).toHaveAttribute("alt", "");
  await expect(
    nav
      .getByRole("button", { name: /@observer.*collected posts/ })
      .locator("img"),
  ).toHaveJSProperty("naturalWidth", 32);

  for (const [handle, initial] of [
    ["quiet_account", "Q"],
    ["broken", "B"],
    ["untrusted", "U"],
  ]) {
    const account = nav.getByRole("button", {
      name: new RegExp(`@${handle}.*0 collected posts`),
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
    page.getByRole("heading", { name: "City Reporter", exact: true }),
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

for (const width of [390, 1280]) {
  test(`sidebar expansion choices survive refresh and navigation at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 800 });
    await mockCollections(page);
    await page.goto("/reader/");
    const nav = page.getByRole("navigation", { name: "Main navigation" });

    async function openNavigation() {
      if (width < 600)
        await page.getByRole("button", { name: "Open navigation" }).click();
    }

    await openNavigation();
    await nav.getByRole("button", { name: "Collapse X accounts" }).click();
    await expect(page.locator("#x-accounts")).toBeHidden();
    await expect(page.locator("#reddit-communities")).toBeVisible();
    await page.reload();
    await openNavigation();
    await expect(
      nav.getByRole("button", { name: "Expand X accounts" }),
    ).toHaveAttribute("aria-expanded", "false");
    await expect(page.locator("#x-accounts")).toBeHidden();
    await expect(page.locator("#reddit-communities")).toBeVisible();

    await nav.getByRole("button", { name: "Expand X accounts" }).click();
    await nav
      .getByRole("button", { name: "Collapse Reddit communities" })
      .click();
    await nav.getByRole("button", { name: "X", exact: true }).click();
    await expect(page).toHaveURL(/view=x/);
    await openNavigation();
    await expect(page.locator("#x-accounts")).toBeVisible();
    await expect(page.locator("#reddit-communities")).toBeHidden();
    await page.reload();
    await openNavigation();
    await expect(page.locator("#x-accounts")).toBeVisible();
    await expect(
      nav.getByRole("button", { name: "Expand Reddit communities" }),
    ).toHaveAttribute("aria-expanded", "false");
    await expect(page.locator("#reddit-communities")).toBeHidden();
  });
}

test("invalid sidebar storage falls back without resetting other group choices", async ({
  page,
}) => {
  await mockCollections(page);
  await page.goto("/reader/");
  await page.evaluate(() => {
    localStorage.setItem(
      "dank-reader:collection-expanded:v1:x-accounts",
      "invalid",
    );
    localStorage.setItem(
      "dank-reader:collection-expanded:v1:reddit-communities",
      "false",
    );
  });
  await page.reload();
  await expect(page.locator("#x-accounts")).toBeVisible();
  await expect(page.locator("#reddit-communities")).toBeHidden();
  await page.getByRole("button", { name: "Collapse X accounts" }).click();
  await page.reload();
  await expect(page.locator("#x-accounts")).toBeHidden();
  await expect(page.locator("#reddit-communities")).toBeHidden();
});

for (const method of ["getItem", "setItem"] as const) {
  test(`sidebar controls remain usable when storage ${method} fails`, async ({
    page,
  }) => {
    await mockCollections(page);
    await page.addInitScript((method) => {
      Object.defineProperty(Storage.prototype, method, {
        value: () => {
          throw new DOMException("Storage unavailable", "SecurityError");
        },
      });
    }, method);
    await page.goto("/reader/");
    await page.getByRole("button", { name: "Collapse X accounts" }).click();
    await expect(page.locator("#x-accounts")).toBeHidden();
    await page.getByRole("button", { name: "Expand X accounts" }).click();
    await expect(page.locator("#x-accounts")).toBeVisible();
    await page
      .getByRole("button", { name: "Collapse Reddit communities" })
      .click();
    await expect(page.locator("#reddit-communities")).toBeHidden();
  });
}

test("account search, author, loaded pages and history survive article navigation and refresh", async ({
  page,
}) => {
  const state = await mockCollections(page);
  state.pageSize = 2;
  await page.addInitScript(() => {
    Reflect.deleteProperty(window, "IntersectionObserver");
  });
  await page.goto(
    "/reader/?view=x&account=REPORTER&after=2026-10-01&before=2026-10-31&author=Shared+display+name&author_match=exact",
  );
  await expect(page.locator(".post-card")).toHaveCount(2);
  await page
    .getByRole("textbox", { name: "Search @reporter" })
    .fill("dispatch");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await page
    .getByRole("combobox", { name: "Order stories" })
    .selectOption("oldest");
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
  const article = page.getByRole("main", { name: "X post reader" });
  await expect(article).toBeVisible();
  await page.goBack();
  await expect(page).toHaveURL(timeline);
  await expect(page.locator(".post-card")).toHaveCount(4);
  await expect(target).toBeFocused();
  await page.goForward();
  await expect(article).toBeVisible();
  await page.reload();
  await expect(
    article.getByText(dispatches[3].title, { exact: true }),
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
    .getByRole("button", { name: /@observer.*collected posts/ })
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
      name: /No posts yet.*@quiet_account.*0 collected posts/,
      exact: true,
    })
    .click();
  await expect(
    page.getByRole("heading", { name: "No posts yet", exact: true }),
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
      nav.getByRole("button", { name: /@quiet_account.*collected posts/ }),
    ).toBeVisible();
    await noOverflow(page);
    await page.screenshot({
      path: testInfo.outputPath(`collections-navigation-${width}.png`),
    });
    await nav
      .getByRole("button", { name: /@reporter.*collected posts/ })
      .click();
    await expect(
      page.getByRole("dialog", { name: "Navigation", exact: true }),
    ).toHaveCount(0);
    await expect(page.locator(".post-card")).toHaveCount(4);
    await expect(
      page.getByRole("heading", { name: "City Reporter", exact: true }),
    ).toBeVisible();
    await noOverflow(page);
    await page.screenshot({
      path: testInfo.outputPath(`x-account-${width}.png`),
      fullPage: true,
    });
    await story(page, dispatches[0]).click();
    await expect(
      page.getByRole("main", { name: "X post reader" }),
    ).toBeVisible();
    await noOverflow(page);
    await page
      .getByRole("button", { name: "Back to stories", exact: true })
      .click();
    await expect(
      page.getByRole("heading", { name: "City Reporter", exact: true }),
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
    const link =
      post.source === "x"
        ? card.getByRole("link", { name: "Posts from @reporter", exact: true })
        : card.locator(".source-link");
    await expect(link).toHaveAttribute("href", new RegExp(`view=${view}`));
    await link.click();
    expect(new URL(page.url()).searchParams.get("view")).toBe(view);

    if (post.source === "x") {
      expect(new URL(page.url()).searchParams.get("account")).toBe("reporter");
    } else {
      expect(new URL(page.url()).searchParams.getAll("domain")).toEqual([
        post.domain,
      ]);
    }

    await expect(
      story(page, post.source === "x" ? blog : dispatches[0]),
    ).toHaveCount(0);
    await story(page, post).click();
    const article = page.getByRole("main", {
      name: post.source === "x" ? "X post reader" : "Article reader",
    });

    if (post.source === "x") {
      await article
        .getByRole("link", { name: "Posts from @reporter", exact: true })
        .click();
    } else {
      await article.locator(".source-link").click();
    }

    await expect(article).toHaveCount(0);
    expect(new URL(page.url()).searchParams.get("view")).toBe(view);
    await page.goto("/reader/?view=sources");
    await page.locator(".source-row").filter({ hasText: post.domain }).click();
    expect(new URL(page.url()).searchParams.get("view")).toBe(view);
    await expect(story(page, post)).toBeVisible();
  }
});

for (const width of [1440, 390, 320]) {
  test(`expanded collection lists allow about fifteen rows and remain reachable at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 1100 });
    const manySources = sources.map((source) => {
      if (source.kind === "x") {
        return {
          ...source,
          accounts: Array.from({ length: 20 }, (_, index) => ({
            handle: `account_${index + 1}`,
            name: index === 0 ? "Grant Sanderson" : `Reporter ${index + 1}`,
            icon_url: null,
            count: index,
          })),
        };
      }

      if (source.kind === "reddit") {
        return {
          ...source,
          communities: Array.from({ length: 20 }, (_, index) => ({
            name: `community_${index + 1}`,
            count: index,
          })),
        };
      }

      return source;
    });
    await mockCollections(page, true, manySources);
    await page.goto("/reader/");

    if (width < 600)
      await page
        .getByRole("button", { name: "Open navigation", exact: true })
        .click();

    const nav = page.getByRole("navigation", { name: "Main navigation" });
    const grant = nav.getByRole("button", {
      name: /Grant Sanderson.*@account_1.*0 collected posts/,
    });
    await expect(grant.locator(".collection-name")).toHaveText(
      "Grant Sanderson",
    );

    for (const id of ["x-accounts", "reddit-communities"]) {
      const list = page.locator(`[id="${id}"]`);
      const capacity = await list.evaluate((element) => {
        const row = element.querySelector("li");

        return {
          rows:
            element.clientHeight / (row?.getBoundingClientRect().height ?? 1),
          scrolls: element.scrollHeight > element.clientHeight,
        };
      });
      expect(capacity.rows).toBeGreaterThanOrEqual(14);
      expect(capacity.rows).toBeLessThanOrEqual(17);
      expect(capacity.scrolls).toBe(true);
      const lastItem = list.getByRole("button").last();
      await lastItem.scrollIntoViewIfNeeded();
      await expect(lastItem).toBeInViewport();
    }

    const sourceLink = nav.getByRole("button", {
      name: "Sources",
      exact: true,
    });
    await sourceLink.scrollIntoViewIfNeeded();
    await expect(sourceLink).toBeInViewport();
    await noOverflow(page);
    await sourceLink.click();
    await expect(page).toHaveURL(/view=sources/);
  });
}

for (const width of [320, 390, 1280]) {
  test(`community header shows the archive estimate and dated provenance at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 800 });
    const enriched = sources.map((source) =>
      source.kind !== "reddit"
        ? source
        : {
            ...source,
            communities: [
              {
                name: "LocalLLaMA",
                count: 238,
                post_total: {
                  count: 1866004,
                  source: "arctic_shift" as const,
                  updated_at: "2026-10-11T09:26:38Z",
                  captured_at: "2026-10-17T15:00:00Z",
                },
              },
            ],
          },
    );
    await mockCollections(page, true, enriched);
    await page.goto(
      "/reader/?view=reddit&community=LocalLLaMA&q=missing&after=2026-10-01",
    );
    const header = page.locator(".topbar");
    const pill = header.locator(".collection-count-pill");
    await expect(header.locator(".collection-count-values")).toHaveText(
      "238/≈1.8Mposts",
    );
    await pill.hover();
    await expect(pill).toHaveAttribute(
      "title",
      "238 posts collected / approximately 1,866,004 total posts, based on Arctic Shift's archive. Archive count updated 11 Oct 2026, 09:26 UTC; retrieved 17 Oct 2026, 15:00 UTC. Archive coverage may be incomplete.",
    );
    await expect(pill.locator(".visually-hidden")).toContainText(
      "Arctic Shift",
    );
    await expect(pill).not.toHaveAttribute("title", /reported by X/);
    await noOverflow(page);
    await page.screenshot({
      path: testInfo.outputPath(`community-total-${width}.png`),
    });
    await header
      .getByRole("button", { name: "Back to Reddit", exact: true })
      .click();
    await expect(page).toHaveURL(/\/reader\/\?view=reddit$/);
  });
}

test("a successful zero archive count remains distinct from an unknown total", async ({
  page,
}) => {
  const enriched = sources.map((source) =>
    source.kind !== "reddit"
      ? source
      : {
          ...source,
          communities: [
            {
              name: "LocalLLaMA",
              count: 0,
              post_total: {
                count: 0,
                source: "arctic_shift" as const,
                updated_at: "2026-10-11T09:26:38Z",
                captured_at: "2026-10-17T15:00:00Z",
              },
            },
          ],
        },
  );
  await mockCollections(page, true, enriched);
  await page.goto("/reader/?view=reddit&community=LocalLLaMA");
  await expect(page.locator(".topbar .collection-count-values")).toHaveText(
    "0/≈0posts",
  );
});
