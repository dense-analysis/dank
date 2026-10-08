import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import { parseFilters } from "../src/lib/navigation";
import type { Post, Source } from "../src/lib/types";

const storageKey = "dank-reader:library:v1";
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
        html: "<p>A collected report ready to read.</p>",
      })),
    ],
    failPosts: false,
    requests: [],
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
    await route.fulfill({ json: { posts, next_cursor: null, limited: false } });
  });

  return state;
}

function story(page: Page, title = article.title) {
  return page.getByRole("button", { name: `Read ${title}`, exact: true });
}

async function expectNoHorizontalOverflow(page: Page) {
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
}

test("retains all filters and scroll position after reader close and browser Back", async ({
  page,
}) => {
  const state = await mockReader(page);
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

  const target = story(page, "Housing report 5");
  await target.scrollIntoViewIfNeeded();
  const scroll = await page.evaluate(() => window.scrollY);
  expect(scroll).toBeGreaterThan(100);
  await target.click();
  await expect(
    page.getByRole("dialog", { name: "Article reader" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  expect(new URL(page.url()).searchParams.toString()).toBe(filters.toString());
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scroll);

  await target.click();
  await page.goBack();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  expect(new URL(page.url()).searchParams.toString()).toBe(filters.toString());
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scroll);
});

test("persists bookmarks and keeps identical article ids from different sources distinct", async ({
  page,
}) => {
  await mockReader(page);
  await page.goto("/reader/");
  await page
    .getByRole("button", { name: `Save ${article.title}`, exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: `Save ${socialPost.title}`, exact: true }),
  ).toHaveAttribute("aria-pressed", "false");
  await page.reload();
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: /^Saved/ })
    .click();
  await expect(story(page)).toBeVisible();
  await expect(story(page, socialPost.title)).toHaveCount(0);
  await page
    .getByRole("button", { name: `Unsave ${article.title}`, exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Keep a good story for later." }),
  ).toBeVisible();
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "Keep a good story for later." }),
  ).toBeVisible();
});

test("restores a named source feed and includes newly collected matching stories", async ({
  page,
}) => {
  const state = await mockReader(page);
  await page.goto("/reader/");
  await expect(story(page)).toBeVisible();
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Create a feed", exact: true })
    .first()
    .click();
  const dialog = page.getByRole("dialog", { name: "Create a feed" });
  await dialog.getByRole("textbox", { name: "Feed name" }).fill("Politics");
  await dialog.getByRole("checkbox", { name: /Example News/ }).check();
  await dialog.getByRole("textbox", { name: /Content rule/ }).fill("housing");
  await dialog
    .getByRole("button", { name: "Create feed", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Politics", exact: true }),
  ).toBeVisible();
  await expect(story(page, socialPost.title)).toHaveCount(0);
  const fresh = {
    ...article,
    id: "fresh",
    title: "Housing update just collected",
  };
  state.posts.unshift(fresh);

  await page.reload();
  await expect(
    page.getByRole("heading", { name: "Politics", exact: true }),
  ).toBeVisible();
  await expect(story(page, fresh.title)).toBeVisible();
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "All stories", exact: true })
    .click();
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Politics", exact: true })
    .click();
  expect(new URL(page.url()).searchParams.getAll("domain")).toEqual([
    "example.com",
  ]);
  expect(new URL(page.url()).searchParams.get("q")).toBe("housing");
  await expect(story(page, fresh.title)).toBeVisible();
  await expect(story(page, socialPost.title)).toHaveCount(0);
});

test("recovers from corrupted browser storage without losing the reading interface", async ({
  page,
}) => {
  await mockReader(page);
  await page.addInitScript(
    (key) => localStorage.setItem(key, "{broken JSON"),
    storageKey,
  );
  await page.goto("/reader/");
  await expect(story(page)).toBeVisible();
  await page
    .getByRole("button", { name: `Save ${article.title}`, exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: `Unsave ${article.title}`, exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      (key) => JSON.parse(localStorage.getItem(key) ?? "{}").version,
      storageKey,
    ),
  ).toBe(1);
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
  const dialog = page.getByRole("dialog", { name: "Article reader" });
  await expect(dialog).toBeVisible();
  await expect(
    dialog.getByText("Collected article body with a safe link."),
  ).toBeVisible();
  await expect(
    dialog.locator(
      "script, iframe, form, input, .article-body button, .article-body [style], .article-body [id], [onerror]",
    ),
  ).toHaveCount(0);
  await expect(dialog.getByText("Unsafe link")).not.toHaveAttribute(
    "href",
    /javascript:/,
  );
  await expect(
    dialog.getByRole("link", { name: "Supporting story" }),
  ).toHaveAttribute("rel", "noopener noreferrer");
  expect(
    await page.evaluate(() => Reflect.get(window, "readerAttack")),
  ).toBeUndefined();
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(trigger).toBeFocused();
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
    page.getByRole("heading", { name: "Good reading starts here." }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Open navigation", exact: true })
    .click();
  await navigation
    .getByRole("button", { name: "All stories", exact: true })
    .click();
  await story(page).click();
  await expect(
    page.getByRole("dialog", { name: "Article reader" }),
  ).toBeVisible();
  await expectNoHorizontalOverflow(page);
  expect(
    await page
      .getByRole("dialog")
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
  await expect(page.getByRole("dialog")).toBeVisible();
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

test("keeps source tags distinct from Words and Meaning search", async ({
  page,
}) => {
  const state = await mockReader(page);
  await page.goto("/reader/");
  await expect(story(page, socialPost.title)).toBeVisible();
  await page.getByRole("textbox", { name: "Search stories" }).fill("housing");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await page.getByRole("button", { name: "Meaning", exact: true }).click();
  await expect(
    page.getByRole("combobox", { name: "Order stories" }),
  ).toHaveValue("relevance");
  await page
    .getByRole("combobox", { name: "Order stories" })
    .selectOption("newest");
  await page.getByRole("button", { name: /^Filters/ }).click();
  await page.getByRole("button", { name: "politics", exact: true }).click();
  await expect(story(page, socialPost.title)).toHaveCount(0);
  await expect(story(page)).toBeVisible();
  await expect(
    page.getByText(
      "Tags select sources. Search finds words or meaning in their stories.",
    ),
  ).toBeVisible();
  const query = new URL(page.url()).searchParams;
  expect(query.get("q")).toBe("housing");
  expect(query.get("mode")).toBe("meaning");
  expect(query.getAll("tag")).toEqual(["politics"]);
  expect(query.getAll("domain")).toEqual([]);
  expect(parseFilters(query).sort).toBe("newest");
  expect(
    state.requests.some(
      (url) =>
        url.searchParams.get("mode") === "meaning" &&
        url.searchParams.get("sort") === "relevance",
    ),
  ).toBe(true);
  await page.getByRole("button", { name: "Words", exact: true }).click();
  expect(parseFilters(new URL(page.url()).searchParams).mode).toBe("words");
  expect(new URL(page.url()).searchParams.getAll("tag")).toEqual(["politics"]);
});
