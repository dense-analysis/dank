import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import { parseFilters } from "../src/lib/navigation";
import type { Post, Source } from "../src/lib/types";

const legacyStorageKey = "dank-reader:library:v1";
const readStorageKey = "dank-reader:read:v1";
const maliciousHtml = `
  <p>Collected article body with a safe link.</p>
  <script>window.readerAttack = true</script>
  <img src="/missing-test-image.png" onerror="window.readerAttack = true">
  <a href="javascript:window.readerAttack=true">Unsafe link</a>
  <a href="https://example.com/supporting-story">Supporting story</a>
  <iframe srcdoc="<script>parent.readerAttack=true</script>"></iframe>
  <form><input name="location"><button>Injected action</button></form>
  <p style="position:fixed" id="main">Untrusted style</p>
`;
const article: Post = {
  id: "shared-id",
  domain: "example.com",
  url: "https://example.com/housing",
  author: "Ada Writer",
  title: "Housing policy, explained",
  excerpt: "A clear account of housing policy and the decisions ahead.",
  html: maliciousHtml,
  created_at: "2026-10-08T10:00:00Z",
  source: "rss",
  thumbnail: null,
  media: [],
};
const socialPost: Post = {
  ...article,
  domain: "x.com",
  url: "https://x.com/reporter/status/shared-id",
  author: "@reporter",
  title: "Housing dispatch from the city",
  html: "<p>A short eyewitness housing dispatch.</p>",
  source: "x",
};
const sources: Source[] = [
  {
    domain: "example.com",
    name: "Example News",
    tags: ["politics", "news"],
    count: 15,
  },
  { domain: "x.com", name: "X", tags: ["social"], count: 1 },
];

interface MockReader {
  posts: Post[];
  failPosts: boolean;
  requests: URL[];
  pageSize: number | null;
}

async function mockReader(page: Page): Promise<MockReader> {
  const state: MockReader = {
    posts: [
      article,
      socialPost,
      ...Array.from({ length: 13 }, (_, index) => ({
        ...article,
        id: `article-${index}`,
        title: `Housing report ${index + 1}`,
        html: "<p>A collected report ready to read.</p>".repeat(30),
      })),
    ],
    failPosts: false,
    requests: [],
    pageSize: null,
  };

  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    state.requests.push(url);

    if (url.pathname.endsWith("/sources")) {
      await route.fulfill({
        json: { sources, total_posts: state.posts.length },
      });
      return;
    }
    if (url.pathname.endsWith("/post")) {
      const post = state.posts.find(
        (item) =>
          item.domain === url.searchParams.get("domain") &&
          item.id === url.searchParams.get("id"),
      );
      await route.fulfill({
        status: post ? 200 : 404,
        json: post ?? { detail: "Story not found." },
      });
      return;
    }
    if (state.failPosts) {
      await route.fulfill({
        status: 503,
        json: { detail: "Collection temporarily unavailable." },
      });
      return;
    }

    const domains = url.searchParams.getAll("domain");
    const tags = url.searchParams.getAll("tag");
    const query = url.searchParams.get("q")?.toLowerCase() ?? "";
    const posts = state.posts.filter(
      (post) =>
        (!domains.length || domains.includes(post.domain)) &&
        (!tags.length ||
          sources.some(
            (source) =>
              source.domain === post.domain &&
              source.tags.some((tag) => tags.includes(tag)),
          )) &&
        (!query ||
          `${post.title} ${post.excerpt}`.toLowerCase().includes(query)),
    );
    const offset = Number(url.searchParams.get("cursor") ?? 0);
    const end = state.pageSize ? offset + state.pageSize : posts.length;
    await route.fulfill({
      json: {
        posts: posts.slice(offset, end),
        next_cursor: end < posts.length ? String(end) : null,
        limited: false,
      },
    });
  });

  return state;
}

function story(page: Page, title = article.title) {
  return page.getByRole("link", { name: `Read ${title}`, exact: true });
}

async function expectNoHorizontalOverflow(page: Page) {
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
}

test("opens a full article page and restores filters, loaded pages, focus and scroll through Back and Forward", async ({
  page,
}) => {
  const state = await mockReader(page);
  state.pageSize = 5;
  const filters = new URLSearchParams({
    q: "housing",
    mode: "words",
    sort: "oldest",
    domain: "example.com",
    tag: "politics",
    author: "Ada",
    after: "2026-10-01",
    before: "2026-10-08",
  });
  await page.goto(`/reader/?${filters}`);
  await expect(story(page)).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Search stories" }),
  ).toHaveValue("housing");
  await expect(
    page.getByRole("combobox", { name: "Order stories" }),
  ).toHaveValue("oldest");
  const request = state.requests.find((url) => url.pathname.endsWith("/posts"));
  expect(parseFilters(request?.searchParams ?? new URLSearchParams())).toEqual(
    parseFilters(filters),
  );

  await page.getByRole("button", { name: "More stories" }).click();
  const target = story(page, "Housing report 5");
  await target.scrollIntoViewIfNeeded();
  const scroll = await page.evaluate(() => window.scrollY);
  expect(scroll).toBeGreaterThan(100);
  await target.click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(
    page.getByRole("heading", { name: "All stories", exact: true }),
  ).not.toBeVisible();
  await expect(page.locator("body")).not.toHaveCSS("overflow", "hidden");
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(0);
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page.getByRole("main", { name: "Article reader" })).toHaveCount(
    0,
  );
  expect(new URL(page.url()).searchParams.toString()).toBe(filters.toString());
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scroll);
  await expect(target).toBeFocused();
  await expect(page.locator(".post-card:visible")).toHaveCount(10);

  await target.click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await page.evaluate(() => window.scrollTo(0, 300));
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(300);
  await page.goBack();
  await expect(page.getByRole("main", { name: "Article reader" })).toHaveCount(
    0,
  );
  expect(new URL(page.url()).searchParams.toString()).toBe(filters.toString());
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scroll);
  await expect(target).toBeFocused();
  await expect(page.locator(".post-card:visible")).toHaveCount(10);
  await page.goForward();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(300);
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(target).toBeFocused();
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scroll);
});

test("opens source-specific deep links and returns to their filtered timeline", async ({
  page,
}) => {
  const state = await mockReader(page);
  await page.goto("/reader/?q=housing&article=shared-id&article_source=x.com");
  const reader = page.getByRole("main", { name: "Article reader" });
  await expect(
    reader.getByRole("heading", { name: socialPost.title }),
  ).toBeVisible();
  await expect(
    reader.getByText("A short eyewitness housing dispatch."),
  ).toBeVisible();
  expect(
    state.requests.some(
      (url) =>
        url.pathname.endsWith("/post") &&
        url.searchParams.get("domain") === "x.com",
    ),
  ).toBe(true);
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page.reload();
  await expect(
    reader.getByRole("heading", { name: socialPost.title }),
  ).toBeVisible();
  await reader
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(story(page)).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Search stories" }),
  ).toHaveValue("housing");
  expect(new URL(page.url()).search).toBe("?q=housing");
});

test("keeps a failed article on its page with a working way back", async ({
  page,
}) => {
  await mockReader(page);
  await page.goto("/reader/?article=missing&article_source=example.com");
  await expect(page.getByRole("alert")).toContainText("Story not found.");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(story(page)).toBeVisible();
});

test("keeps navigation and article controls focused on reading", async ({
  page,
}) => {
  await mockReader(page);
  await page.goto("/reader/");
  await expect(story(page)).toBeVisible();
  const sidebar = page.locator(".sidebar");
  await expect(sidebar.locator(".brand img")).toHaveCount(0);
  await expect(
    sidebar.getByRole("link", { name: "DANK reader home" }),
  ).toHaveText("DANK");
  await expect(page.locator('link[rel="icon"]')).toHaveAttribute(
    "href",
    "/reader/favicon.svg",
  );
  await expect(page.locator("main .page-heading h1")).toHaveCount(0);
  await expect(page.locator(".topbar h1")).toHaveText("All stories");
  await expect(
    sidebar.getByRole("button", { name: "All stories", exact: true }),
  ).toBeVisible();
  await expect(
    sidebar.getByRole("button", { name: "Sources", exact: true }),
  ).toBeVisible();
  await expect(
    sidebar.getByRole("button", { name: "Settings", exact: true }),
  ).toBeVisible();
  await expect(
    sidebar.getByText("Your workspace", { exact: true }),
  ).toHaveCount(0);
  await expect(page.getByText("A quieter corner of the internet")).toHaveCount(
    0,
  );
  await expect(page.getByText("Your personal reader")).toHaveCount(0);
  await expect(page.getByText("YOUR FEEDS", { exact: true })).toHaveCount(0);
  await expect(
    page.getByText("A library with your point of view."),
  ).toHaveCount(0);
  await expect(
    page.getByText("Feeds & bookmarks stay in this browser."),
  ).toHaveCount(0);
  await expect(page.getByRole("button", { name: /^Saved/ })).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: /Create a feed|Make your first feed/ }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: /^(Save|Unsave) / }),
  ).toHaveCount(0);

  await page.getByRole("button", { name: /^Filters/ }).click();
  await expect(page.getByRole("button", { name: /Save.*feed/i })).toHaveCount(
    0,
  );
  await story(page).click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: /^(Save|Unsave) / }),
  ).toHaveCount(0);
});

const legacyLibrary = JSON.stringify(
  {
    version: 1,
    feeds: [
      {
        id: "legacy-politics",
        name: "Politics",
        filters: parseFilters(
          new URLSearchParams("domain=example.com&q=housing"),
        ),
      },
    ],
    bookmarks: [article],
    read: [JSON.stringify([article.domain, article.id])],
    futureData: { preserved: true },
  },
  null,
  2,
);

for (const view of ["saved", "legacy-politics"]) {
  test(`opens the old ${view} view as All stories and preserves filters and local data`, async ({
    page,
  }) => {
    await mockReader(page);
    await page.addInitScript(
      ({ key, value }) => {
        if (localStorage.getItem(key) === null)
          localStorage.setItem(key, value);
      },
      { key: legacyStorageKey, value: legacyLibrary },
    );
    await page.goto(`/reader/?view=${view}&q=housing&domain=x.com&sort=oldest`);
    await expect(
      page.getByRole("heading", { name: "All stories", exact: true }),
    ).toBeVisible();
    await expect(story(page, socialPost.title)).toBeVisible();
    await expect(story(page)).toHaveCount(0);
    await expect(
      page.getByRole("textbox", { name: "Search stories" }),
    ).toHaveValue("housing");
    await expect(
      page.getByRole("combobox", { name: "Order stories" }),
    ).toHaveValue("oldest");
    await expect(
      page.getByRole("button", { name: "Politics", exact: true }),
    ).toHaveCount(0);
    await story(page, socialPost.title).click();
    await expect(
      page.getByRole("main", { name: "Article reader" }),
    ).toBeVisible();
    await page.reload();
    await expect(
      page.getByRole("main", { name: "Article reader" }),
    ).toBeVisible();
    await page
      .getByRole("button", { name: "Back to stories", exact: true })
      .click();
    await expect(
      page.getByRole("heading", { name: "All stories", exact: true }),
    ).toBeVisible();
    const params = new URL(page.url()).searchParams;
    expect(params.get("q")).toBe("housing");
    expect(params.getAll("domain")).toEqual(["x.com"]);
    expect(params.get("sort")).toBe("oldest");
    expect(
      await page.evaluate((key) => localStorage.getItem(key), legacyStorageKey),
    ).toBe(legacyLibrary);
  });
}

test("preserves old library data while recording read stories by source", async ({
  page,
}) => {
  await mockReader(page);
  await page.addInitScript(
    ({ key, value }) => {
      if (localStorage.getItem(key) === null) localStorage.setItem(key, value);
    },
    { key: legacyStorageKey, value: legacyLibrary },
  );
  await page.goto("/reader/");
  const articleCard = page.locator(".post-card").filter({ has: story(page) });
  const socialCard = page
    .locator(".post-card")
    .filter({ has: story(page, socialPost.title) });
  await expect(articleCard.getByText("Read", { exact: true })).toBeVisible();
  await expect(socialCard.getByText("Read", { exact: true })).toHaveCount(0);
  await story(page, socialPost.title).click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await page.reload();
  await expect(articleCard.getByText("Read", { exact: true })).toBeVisible();
  await expect(socialCard.getByText("Read", { exact: true })).toBeVisible();
  expect(
    await page.evaluate((key) => localStorage.getItem(key), legacyStorageKey),
  ).toBe(legacyLibrary);
  expect(
    await page.evaluate((key) => localStorage.getItem(key), readStorageKey),
  ).not.toBeNull();
});

test("keeps corrupted legacy storage untouched while reading remains usable", async ({
  page,
}) => {
  await mockReader(page);
  await page.addInitScript((key) => {
    if (localStorage.getItem(key) === null)
      localStorage.setItem(key, "{broken JSON");
  }, legacyStorageKey);
  await page.goto("/reader/");
  await expect(story(page)).toBeVisible();
  await story(page).click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await page.reload();
  await expect(
    page
      .locator(".post-card")
      .filter({ has: story(page) })
      .getByText("Read", { exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate((key) => localStorage.getItem(key), legacyStorageKey),
  ).toBe("{broken JSON");
});

test("shows API failures and recovers when the user retries", async ({
  page,
}) => {
  const state = await mockReader(page);
  state.failPosts = true;
  await page.goto("/reader/");
  await expect(page.getByRole("alert")).toContainText(
    "Collection temporarily unavailable.",
  );
  state.failPosts = false;
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await expect(story(page)).toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("sanitizes article markup while preserving safe content and keyboard navigation", async ({
  page,
}) => {
  await mockReader(page);
  await page.goto("/reader/");
  const trigger = story(page);
  await trigger.click();
  const reader = page.getByRole("main", { name: "Article reader" });
  await expect(reader).toBeVisible();
  await expect(
    reader.getByText("Collected article body with a safe link."),
  ).toBeVisible();
  await expect(
    reader.locator(
      "script, iframe, form, input, .article-body button, .article-body [style], .article-body [id], [onerror]",
    ),
  ).toHaveCount(0);
  await expect(reader.getByText("Unsafe link")).not.toHaveAttribute(
    "href",
    /javascript:/,
  );
  await expect(
    reader.getByRole("link", { name: "Supporting story" }),
  ).toHaveAttribute("rel", "noopener noreferrer");
  expect(
    await page.evaluate(() => Reflect.get(window, "readerAttack")),
  ).toBeUndefined();
  await expect(
    reader.getByRole("button", { name: "Back to stories", exact: true }),
  ).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(reader).toBeVisible();
  await reader
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(reader).toHaveCount(0);
  await expect(trigger).toBeFocused();
});

test("shows inline figures and credits at desktop and mobile widths", async ({
  page,
}) => {
  const state = await mockReader(page);
  state.posts[0] = {
    ...article,
    html: '<p>Before the illustration.</p><figure><img src="/assets/diagram.svg" alt="Article diagram"><figcaption>Screenshot credit: Example Lab</figcaption></figure><p>After the illustration.</p>',
  };
  await page.route("**/assets/diagram.svg", (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="960" height="548"><rect width="960" height="548" fill="#eee"/><text x="30" y="70" fill="#111" font-size="32">Article diagram</text></svg>',
    }),
  );
  await page.goto("/reader/?article=shared-id&article_source=example.com");
  for (const width of [1280, 390]) {
    await page.setViewportSize({ width, height: 900 });
    const figure = page.locator(".article-body figure");
    const image = figure.getByRole("img", { name: "Article diagram" });
    await image.scrollIntoViewIfNeeded();
    await expect(image).toBeVisible();
    await expect(figure.locator("figcaption")).toHaveText(
      "Screenshot credit: Example Lab",
    );
    expect(
      await image.evaluate((element: HTMLImageElement) => element.naturalWidth),
    ).toBe(960);
    await expect(figure.locator("a")).toHaveAttribute(
      "href",
      /\/assets\/diagram.svg$/,
    );
    await expect(figure).toHaveCSS("margin-left", "0px");
    await expectNoHorizontalOverflow(page);
  }
});

test("supports mobile navigation and article reading without horizontal overflow", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockReader(page);
  await page.goto("/reader/");
  await expect(story(page)).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await page
    .getByRole("button", { name: "Open navigation", exact: true })
    .click();
  const navigation = page.getByRole("navigation", { name: "Main navigation" });
  await expect(navigation).toBeVisible();
  await navigation
    .getByRole("button", { name: "Sources", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Sources", exact: true }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Open navigation", exact: true })
    .click();
  await navigation
    .getByRole("button", { name: "All stories", exact: true })
    .click();
  await story(page).click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  await expectNoHorizontalOverflow(page);
  expect(
    await page
      .getByRole("main", { name: "Article reader" })
      .evaluate((element) => element.scrollWidth <= element.clientWidth),
  ).toBe(true);
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expectNoHorizontalOverflow(page);
});

test("has no serious or critical accessibility violations in the feed and reader", async ({
  page,
}) => {
  await mockReader(page);
  await page.goto("/reader/");
  await expect(story(page)).toBeVisible();
  const feed = await new AxeBuilder({ page }).analyze();
  expect(
    feed.violations
      .filter((item) => ["serious", "critical"].includes(item.impact ?? ""))
      .map((item) => ({
        id: item.id,
        targets: item.nodes.map((node) => node.target),
      })),
  ).toEqual([]);
  await story(page, socialPost.title).click();
  await expect(
    page.getByRole("main", { name: "Article reader" }),
  ).toBeVisible();
  const reader = await new AxeBuilder({ page }).analyze();
  expect(
    reader.violations
      .filter((item) => ["serious", "critical"].includes(item.impact ?? ""))
      .map((item) => ({
        id: item.id,
        targets: item.nodes.map((node) => node.target),
      })),
  ).toEqual([]);
});

test("uses one combined search and retains source filters and ordering", async ({
  page,
}) => {
  const state = await mockReader(page);
  await page.goto("/reader/");
  await expect(story(page, socialPost.title)).toBeVisible();
  await expect(page.getByRole("group", { name: "Search method" })).toHaveCount(
    0,
  );
  await expect(
    page.getByRole("button", { name: /^(Words|Meaning)$/ }),
  ).toHaveCount(0);
  await expect(page.locator("#search-mode-help")).toHaveCount(0);
  await page.getByRole("textbox", { name: "Search stories" }).fill("housing");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await expect(
    page.getByRole("combobox", { name: "Order stories" }),
  ).toHaveValue("relevance");
  await expect
    .poll(() =>
      state.requests.some(
        (url) =>
          url.searchParams.get("mode") === "combined" &&
          url.searchParams.get("sort") === "relevance",
      ),
    )
    .toBe(true);
  expect(new URL(page.url()).searchParams.has("mode")).toBe(false);
  await page
    .getByRole("combobox", { name: "Order stories" })
    .selectOption("newest");
  await page.getByRole("button", { name: /^Filters/ }).click();
  await page.getByRole("button", { name: "politics", exact: true }).click();
  await expect(story(page, socialPost.title)).toHaveCount(0);
  await expect(story(page)).toBeVisible();
  const query = new URL(page.url()).searchParams;
  expect(query.get("q")).toBe("housing");
  expect(query.getAll("tag")).toEqual(["politics"]);
  expect(query.getAll("domain")).toEqual([]);
  expect(parseFilters(query).sort).toBe("newest");
  await page.reload();
  await expect(
    page.getByRole("combobox", { name: "Order stories" }),
  ).toHaveValue("newest");
  await expect(story(page)).toBeVisible();
  await expect(story(page, socialPost.title)).toHaveCount(0);
  await page.getByRole("button", { name: "Clear search", exact: true }).click();
  await expect(
    page.getByRole("textbox", { name: "Search stories" }),
  ).toHaveValue("");
  expect(new URL(page.url()).searchParams.getAll("tag")).toEqual(["politics"]);
  await expect
    .poll(() =>
      state.requests
        .filter((url) => url.pathname.endsWith("/posts"))
        .at(-1)
        ?.searchParams.has("mode"),
    )
    .toBe(false);
});

for (const mode of ["words", "meaning"]) {
  test(`old ${mode} search links use combined search`, async ({ page }) => {
    const state = await mockReader(page);
    await page.goto(`/reader/?q=housing&mode=${mode}&sort=relevance`);
    await expect(story(page)).toBeVisible();
    await expect(
      page.getByRole("textbox", { name: "Search stories" }),
    ).toHaveValue("housing");
    await expect(
      page.getByRole("group", { name: "Search method" }),
    ).toHaveCount(0);
    expect(
      state.requests
        .find((url) => url.pathname.endsWith("/posts"))
        ?.searchParams.get("mode"),
    ).toBe("combined");
    await page.getByRole("button", { name: "Search", exact: true }).click();
    expect(new URL(page.url()).searchParams.has("mode")).toBe(false);
  });
}

test("dismisses mobile navigation and switches pages without leaving an overlay", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockReader(page);
  await page.goto("/reader/");
  await expect(story(page)).toBeVisible();
  const navigation = page.getByRole("dialog", {
    name: "Navigation",
    exact: true,
  });
  const openNavigation = page.getByRole("button", {
    name: "Open navigation",
    exact: true,
  });

  await openNavigation.click();
  await expect(navigation).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(navigation).toHaveCount(0);
  await expect(openNavigation).toBeFocused();
  await expect(page.locator("body")).not.toHaveCSS("overflow", "hidden");

  await openNavigation.click();
  await navigation
    .getByRole("button", { name: "Settings", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Settings", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.locator("body")).not.toHaveCSS("overflow", "hidden");
  await expectNoHorizontalOverflow(page);

  await openNavigation.click();
  await navigation
    .getByRole("button", { name: "All stories", exact: true })
    .click();
  await expect(story(page)).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.locator("body")).not.toHaveCSS("overflow", "hidden");
  await expectNoHorizontalOverflow(page);
});
