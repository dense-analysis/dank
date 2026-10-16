import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post, Source } from "../src/lib/types";

function post(id: string, community: string, flair: string): Post {
  return {
    id,
    domain: "reddit.com",
    url: `https://www.reddit.com/r/${community}/comments/${id}/`,
    title: `${community} ${flair} story`,
    author: "model_builder",
    excerpt: "Local models and community research.",
    html: "<p>Local models and community research.</p>",
    created_at: "2026-10-15T10:00:00Z",
    source: "reddit",
    thumbnail: null,
    media: [],
    reddit: {
      community,
      score: 43,
      comments: 10,
      flair,
      icon_url: `/assets/${community}.svg`,
    },
  };
}
const posts = [
  post("new-model", "LocalLLaMA", "New Model"),
  post("local-discussion", "LocalLLaMA", "Discussion"),
  post("accelerate-discussion", "accelerate", "Discussion"),
  post("research", "accelerate", "Research"),
];
const sources: Source[] = [
  {
    domain: "reddit.com",
    name: "Reddit",
    kind: "reddit",
    count: 4,
    tags: ["technology"],
    flairs: [
      { name: "Discussion", count: 2 },
      { name: "New Model", count: 1 },
      { name: "Research", count: 1 },
    ],
    communities: [
      {
        name: "LocalLLaMA",
        count: 2,
        icon_url: "/assets/LocalLLaMA.svg",
        flairs: [
          { name: "New Model", count: 1 },
          { name: "Discussion", count: 1 },
        ],
      },
      {
        name: "accelerate",
        count: 2,
        icon_url: "/assets/accelerate.svg",
        flairs: [
          { name: "Research", count: 1 },
          { name: "Discussion", count: 1 },
        ],
      },
      { name: "Empty", count: 0, icon_url: null, flairs: [] },
    ],
  },
];

async function mockDetails(
  page: Pick<Page, "route">,
  brokenIcons = false,
  items: Post[] = posts,
) {
  const requests: URL[] = [];
  await page.route("**/assets/*.svg", (route) =>
    route.fulfill(
      brokenIcons
        ? { status: 404 }
        : {
            contentType: "image/svg+xml",
            body: '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"><rect width="32" height="32" fill="#75456b"/><circle cx="16" cy="16" r="9" fill="#f2e6cd"/></svg>',
          },
    ),
  );
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    requests.push(url);

    if (url.pathname.endsWith("/sources")) {
      await route.fulfill({ json: { sources, total_posts: items.length } });
      return;
    }

    if (url.pathname.endsWith("/post")) {
      await route.fulfill({
        json: items.find((item) => item.id === params.get("id")),
      });
      return;
    }

    const selected = items.filter(
      (item) =>
        params.get("kind") === "reddit" &&
        (!params.get("community") ||
          item.reddit?.community.toLowerCase() ===
            params.get("community")?.toLowerCase()) &&
        (!params.getAll("flair").length ||
          params.getAll("flair").includes(item.reddit?.flair ?? "")) &&
        (!params.get("q") ||
          item.title
            .toLowerCase()
            .includes(params.get("q")?.toLowerCase() ?? "")) &&
        (!params.get("author") || item.author === params.get("author")) &&
        (!params.get("after") ||
          item.created_at.slice(0, 10) >= (params.get("after") ?? "")) &&
        (!params.get("before") ||
          item.created_at.slice(0, 10) <= (params.get("before") ?? "")),
    );
    await route.fulfill({
      json: { posts: selected, next_cursor: null, limited: false },
    });
  });

  return requests;
}

test("shows cached community icons in navigation, mixed feeds and article headers with graceful fallback", async ({
  page,
}) => {
  await mockDetails(page);
  await page.goto("/reader/?view=reddit");
  const nav = page.getByRole("navigation", { name: "Main navigation" });
  const local = nav.getByRole("button", { name: /r\/LocalLLaMA/ });
  await expect(local.locator("img")).toHaveAttribute(
    "src",
    /\/assets\/LocalLLaMA.svg$/,
  );
  await expect(local.locator("img")).toHaveAttribute("alt", "");
  await expect(
    nav.getByRole("button", { name: /r\/Empty/ }).locator(".community-avatar"),
  ).toHaveText("E");
  const card = page.locator(".post-card").first();
  await expect(card.locator(".community-avatar img")).toHaveJSProperty(
    "naturalWidth",
    32,
  );
  await card
    .getByRole("link", { name: `Read ${posts[0].title}`, exact: true })
    .click();
  const article = page.getByRole("main", { name: "Article reader" });
  await expect(article.locator(".community-avatar img")).toHaveJSProperty(
    "naturalWidth",
    32,
  );

  await mockDetails(page, true);
  await page.reload();
  await expect(article.locator(".community-avatar")).toHaveText("L");
  await expect(article.locator(".community-avatar img")).toHaveCount(0);
  await expect(local.locator(".community-avatar")).toHaveText("L");
  await expect(
    nav
      .getByRole("button", { name: /r\/accelerate/ })
      .locator(".community-avatar"),
  ).toHaveText("A");
});

test("isolates community flairs, combines shared global labels and supports OR selection", async ({
  page,
}, testInfo) => {
  const requests = await mockDetails(page);
  await page.goto("/reader/?view=reddit&community=LocalLLaMA");
  await page.getByRole("button", { name: "Filters", exact: true }).click();
  const flairs = page.getByRole("group", { name: "Flair", exact: true });
  await expect(
    flairs.getByRole("button", { name: "New Model", exact: true }),
  ).toBeVisible();
  await expect(
    flairs.getByRole("button", { name: "Research", exact: true }),
  ).toHaveCount(0);
  await flairs.getByRole("button", { name: "New Model", exact: true }).click();
  await expect(page.locator(".post-card")).toHaveCount(1);
  await flairs.getByRole("button", { name: "Discussion", exact: true }).click();
  await expect(page.locator(".post-card")).toHaveCount(2);
  expect(new URL(page.url()).searchParams.getAll("flair")).toEqual([
    "New Model",
    "Discussion",
  ]);
  await expect
    .poll(() =>
      requests.some((url) => url.searchParams.getAll("flair").length === 2),
    )
    .toBe(true);
  await expect(page.getByRole("group", { name: "Source tags" })).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("community-flairs-desktop.png"),
    fullPage: true,
  });

  const nav = page.getByRole("navigation", { name: "Main navigation" });
  await nav.getByRole("button", { name: /r\/accelerate/ }).click();
  await expect(
    flairs.getByRole("button", { name: "Research", exact: true }),
  ).toBeVisible();
  await expect(
    flairs.getByRole("button", { name: "New Model", exact: true }),
  ).toHaveCount(0);
  expect(new URL(page.url()).searchParams.getAll("flair")).toEqual([]);
  await nav.getByRole("button", { name: "Reddit", exact: true }).click();
  await flairs.getByRole("button", { name: "Discussion", exact: true }).click();
  await expect(
    flairs.getByRole("button", { name: "Discussion", exact: true }),
  ).toHaveAttribute("title", "2 collected posts");
  await expect(page.locator(".post-card")).toHaveCount(2);
  await expect(
    page.getByRole("heading", { name: posts[1].title, exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: posts[2].title, exact: true }),
  ).toBeVisible();
  await page
    .getByRole("button", {
      name: "Remove flair filter: Discussion",
      exact: true,
    })
    .click();
  await expect(page.locator(".post-card")).toHaveCount(4);
  await flairs.getByRole("button", { name: "New Model", exact: true }).click();
  await page
    .getByRole("button", { name: "Reset filters", exact: true })
    .click();
  await expect(page.locator(".post-card")).toHaveCount(4);
  expect(new URL(page.url()).searchParams.has("flair")).toBe(false);
  await flairs.getByRole("button", { name: "New Model", exact: true }).click();
  await nav.getByRole("button", { name: "All stories", exact: true }).click();
  expect(new URL(page.url()).searchParams.has("flair")).toBe(false);
  await expect(flairs).toHaveCount(0);
});

test("retains flair with search, exact author, dates, article reload and browser navigation", async ({
  page,
}) => {
  const requests = await mockDetails(page);
  await page.goto(
    "/reader/?view=reddit&community=LocalLLaMA&flair=New+Model&after=2026-10-15&before=2026-10-15",
  );
  await page
    .getByRole("textbox", { name: "Search r/LocalLLaMA" })
    .fill("New Model");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await page
    .getByRole("link", { name: "Stories by model_builder", exact: true })
    .click();
  const timelineUrl = page.url();
  const story = page.getByRole("link", {
    name: `Read ${posts[0].title}`,
    exact: true,
  });
  await expect(story).toHaveAttribute("href", /flair=New\+Model/);
  await story.click();
  const article = page.getByRole("main", { name: "Article reader" });
  await expect(article).toBeVisible();
  await page.goBack();
  await expect(page).toHaveURL(timelineUrl);
  await expect(story).toBeVisible();
  await page.goForward();
  await expect(article).toBeVisible();
  await page.reload();
  await article
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page).toHaveURL(timelineUrl);
  await expect(
    page.getByRole("button", { name: "Remove flair filter: New Model" }),
  ).toBeVisible();
  expect(
    requests.some((url) => {
      const params = url.searchParams;
      return (
        params.get("kind") === "reddit" &&
        params.get("community") === "LocalLLaMA" &&
        params.get("flair") === "New Model" &&
        params.get("q") === "New Model" &&
        params.get("author_match") === "exact" &&
        params.get("author") === "model_builder" &&
        params.get("after") === "2026-10-15" &&
        params.get("before") === "2026-10-15"
      );
    }),
  ).toBe(true);
  await page.reload();
  await expect(
    page.getByRole("button", { name: "Remove flair filter: New Model" }),
  ).toBeVisible();
});

test("keeps icons and flair controls usable on mobile and accessible", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockDetails(page);
  await page.goto("/reader/?view=reddit&community=LocalLLaMA");
  await page
    .getByRole("button", { name: "Open navigation", exact: true })
    .click();
  const dialog = page.getByRole("dialog", { name: "Navigation", exact: true });
  await expect(
    dialog.getByRole("button", { name: /r\/LocalLLaMA/ }).locator("img"),
  ).toHaveJSProperty("naturalWidth", 32);
  await page.screenshot({
    path: testInfo.outputPath("community-icons-mobile.png"),
  });
  await dialog.getByRole("button", { name: /r\/accelerate/ }).click();
  await expect(dialog).toHaveCount(0);
  await page.getByRole("button", { name: "Filters", exact: true }).click();
  await page
    .getByRole("group", { name: "Flair", exact: true })
    .getByRole("button", { name: "Research", exact: true })
    .click();
  await expect(page.locator(".post-card")).toHaveCount(1);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: testInfo.outputPath("community-flairs-mobile.png"),
    fullPage: true,
  });
  const audit = await new AxeBuilder({ page }).analyze();
  expect(
    audit.violations.filter((item) =>
      ["serious", "critical"].includes(item.impact ?? ""),
    ),
  ).toEqual([]);
  await page
    .getByRole("button", { name: "Remove flair filter: Research" })
    .click();
  await expect(page.locator(".post-card")).toHaveCount(2);
});

for (const width of [1440, 390]) {
  test(`post flair opens its own community feed at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    const requests = await mockDetails(page);
    const initial =
      "/reader/?view=reddit&q=Discussion&author=model_builder&after=2026-10-15";
    await page.goto(initial);
    await expect(page.locator(".post-card")).toHaveCount(2);
    const badge = page.getByRole("link", {
      name: "Posts from r/accelerate with flair Discussion",
      exact: true,
    });
    const destination =
      "/reader/?view=reddit&community=accelerate&flair=Discussion";
    await expect(badge).toHaveAttribute("href", destination);
    await badge.click();
    await expect(page).toHaveURL(destination);
    await expect(page.locator(".post-card")).toHaveCount(1);
    await expect(
      page.getByRole("heading", { name: posts[2].title, exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: /^Filters/ }),
    ).toHaveAttribute("aria-expanded", "false");
    expect(
      requests.some(
        (url) =>
          url.searchParams.get("community") === "accelerate" &&
          url.searchParams.get("flair") === "Discussion" &&
          !url.searchParams.has("q") &&
          !url.searchParams.has("author") &&
          !url.searchParams.has("after"),
      ),
    ).toBe(true);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.goBack();
    await expect(page).toHaveURL(initial);
    await expect(page.locator(".post-card")).toHaveCount(2);
  });
}

test("article flair supports keyboard and new-tab navigation with the same community filter", async ({
  page,
  context,
}) => {
  await mockDetails(context);
  await page.goto(
    "/reader/?view=reddit&community=LocalLLaMA&flair=New+Model&flair=Discussion",
  );
  await page
    .getByRole("link", { name: `Read ${posts[0].title}`, exact: true })
    .click();
  const article = page.getByRole("main", { name: "Article reader" });
  const articleUrl = page.url();
  const badge = article.getByRole("link", {
    name: "Posts from r/LocalLLaMA with flair New Model",
    exact: true,
  });
  const destination =
    "/reader/?view=reddit&community=LocalLLaMA&flair=New+Model";
  await expect(badge).toHaveAttribute("href", destination);
  const opened = context.waitForEvent("page");
  await badge.click({ button: "middle" });
  const tab = await opened;
  await expect(tab).toHaveURL(destination);
  await expect(tab.locator(".post-card")).toHaveCount(1);
  await expect(page).toHaveURL(articleUrl);
  await tab.close();
  await badge.focus();
  await badge.press("Enter");
  await expect(page).toHaveURL(destination);
  await expect(article).toHaveCount(0);
  await expect(page.locator(".post-card")).toHaveCount(1);
  await expect(
    page.getByRole("button", {
      name: "Remove flair filter: New Model",
      exact: true,
    }),
  ).toBeVisible();
  await page.goBack();
  await expect(page).toHaveURL(articleUrl);
  await expect(article).toBeVisible();
});

for (const width of [1440, 816, 390]) {
  test(`keeps community context visible without repeating it on cards at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 897 });
    const items = Array.from({ length: 24 }, (_, index) => {
      const item = posts[index % 2];
      if (!item.reddit) throw new Error("Expected a Reddit fixture");

      return {
        ...item,
        id: `context-${index}`,
        title: `Community story ${index}`,
        reddit: { ...item.reddit, score: index === 0 ? 1 : 43 },
      };
    });
    await mockDetails(page, false, items);
    await page.goto("/reader/?view=reddit&community=LocalLLaMA");
    const header = page.locator(".topbar");
    await expect(
      header.getByRole("heading", { name: "r/LocalLLaMA", exact: true }),
    ).toBeVisible();
    await expect(header.locator(".community-avatar img")).toHaveJSProperty(
      "naturalWidth",
      32,
    );
    const search = page.getByRole("textbox", {
      name: "Search r/LocalLLaMA",
      exact: true,
    });
    await expect(search).toHaveAttribute("placeholder", "Search r/LocalLLaMA");
    const cards = page.locator(".post-card");
    await expect(cards).toHaveCount(24);
    await expect(
      cards.locator(".community-avatar, .post-meta .source-link, .source-kind"),
    ).toHaveCount(0);
    await expect(cards.first().locator("time")).toBeVisible();
    await expect(
      cards
        .first()
        .getByRole("link", { name: "Stories by model_builder", exact: true }),
    ).toBeVisible();
    const score = cards.first().locator(".reddit-score");
    await expect(score).toHaveText("Score: 1");
    await expect(score.locator("svg")).toHaveCount(1);
    await expect(cards.getByText(/points/)).toHaveCount(0);
    await page.evaluate(() => window.scrollTo(0, 1000));
    await expect
      .poll(() => page.evaluate(() => window.scrollY))
      .toBeGreaterThan(900);
    const box = await header.boundingBox();
    expect(box?.y).toBeCloseTo(0, 0);
    expect(box?.height).toBeLessThanOrEqual(60);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  });
}

test("updates search scope across communities, all Reddit and stories", async ({
  page,
}) => {
  await mockDetails(page);
  await page.goto("/reader/?view=reddit&community=LocalLLaMA");
  await expect(
    page.getByRole("textbox", { name: "Search r/LocalLLaMA", exact: true }),
  ).toBeVisible();
  const nav = page.getByRole("navigation", { name: "Main navigation" });
  await nav.getByRole("button", { name: /r\/accelerate/ }).click();
  await expect(
    page.getByRole("textbox", { name: "Search r/accelerate", exact: true }),
  ).toBeVisible();
  await nav.getByRole("button", { name: "Reddit", exact: true }).click();
  await expect(
    page.getByRole("textbox", { name: "Search Reddit", exact: true }),
  ).toBeVisible();
  await expect(page.locator(".post-card .community-avatar")).toHaveCount(4);
  await expect(page.locator(".post-card .post-meta .source-link")).toHaveCount(
    4,
  );
  await nav.getByRole("button", { name: "All stories", exact: true }).click();
  await expect(
    page.getByRole("textbox", { name: "Search stories", exact: true }),
  ).toBeVisible();
});

test("keeps article controls below the community header and restores the reading anchor", async ({
  page,
}) => {
  const longPost = {
    ...posts[0],
    html: Array.from(
      { length: 70 },
      (_, i) =>
        `<p>Paragraph ${i}. ${"A detailed local model discussion. ".repeat(8)}</p>`,
    ).join(""),
  };
  await mockDetails(page, false, [longPost]);
  await page.goto("/reader/?view=reddit&community=LocalLLaMA");
  await page
    .getByRole("link", { name: `Read ${longPost.title}`, exact: true })
    .click();
  const paragraphs = page.locator(".article-body p");
  await expect(paragraphs).toHaveCount(70);
  await page.evaluate(() => document.fonts.ready);
  await paragraphs
    .nth(12)
    .evaluate((element) =>
      window.scrollTo(
        0,
        window.scrollY + element.getBoundingClientRect().top - 180,
      ),
    );
  const header = await page.locator(".topbar").boundingBox();
  const toolbar = await page.locator(".reader-toolbar").boundingBox();
  if (!header || !toolbar) throw new Error("Expected visible reader headers");

  expect(header.y).toBeCloseTo(0, 0);
  expect(toolbar.y).toBeCloseTo(header.height, 0);
  const top = await paragraphs
    .nth(12)
    .evaluate((element) => element.getBoundingClientRect().top);
  await expect
    .poll(() =>
      page.evaluate(() => localStorage.getItem("dank-reader:positions:v1")),
    )
    .toContain('"block":');
  await page.reload();
  await expect(paragraphs).toHaveCount(70);
  await expect
    .poll(async () => {
      const restoredTop = await paragraphs
        .nth(12)
        .evaluate((element) => element.getBoundingClientRect().top);

      return Math.abs(restoredTop - top);
    })
    .toBeLessThan(5);
});
