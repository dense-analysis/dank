import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { HackerNewsPost, Post } from "../src/lib/types";

function story(index: number): Post {
  const day = index < 4 ? "2026-10-17" : "2026-10-16";
  return {
    id: String(index + 1),
    domain: "news.ycombinator.com",
    source: "hacker_news",
    url: `https://news.ycombinator.com/item?id=${index + 1}`,
    title: `Building better software ${index + 1}`,
    author: "builder",
    excerpt: `https://example.com/software-${index + 1}`,
    html: `<p><a href="https://example.com/software-${index + 1}">https://example.com/software-${index + 1}</a></p>`,
    created_at: `${day}T12:00:00Z`,
    thumbnail: null,
    media: [],
    hacker_news: {
      submitted_at: "2026-10-15T11:25:00Z",
      points: 200 - index,
      comments: 20,
      day,
      rank: index + 1,
      discussion_url: `https://news.ycombinator.com/item?id=${index + 1}`,
      external_url: `https://example.com/software-${index + 1}`,
    },
  };
}
const stories = Array.from({ length: 8 }, (_, index) => story(index));
const capture = {
  status: "disabled" as const,
  captured: 0,
  total: 20,
  reason: null,
};
async function fixtures(page: Page, detail: Partial<HackerNewsPost> = {}) {
  const requests: URL[] = [];
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    requests.push(url);
    if (url.pathname.includes("/youtube/")) {
      await route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 0 },
      });
      return;
    }
    if (url.pathname.endsWith("/sources")) {
      await route.fulfill({
        json: {
          sources: [
            {
              kind: "hacker_news",
              domain: "news.ycombinator.com",
              name: "Hacker News",
              tags: [],
              count: 8,
            },
          ],
          total_posts: 8,
        },
      });
      return;
    }
    if (url.pathname.endsWith("/post")) {
      const post =
        stories.find((item) => item.id === url.searchParams.get("id")) ??
        stories[0];
      await route.fulfill({
        json: {
          ...post,
          hacker_news: {
            ...post.hacker_news,
            comment_capture: capture,
            comments_items: [],
            ...detail,
          },
        },
      });
      return;
    }
    const offset = Number(url.searchParams.get("cursor") ?? "0");
    const kind = url.searchParams.get("kind");
    const selection = url.searchParams.get("hn_filter");
    const available =
      kind === "stories"
        ? []
        : selection === "top10"
          ? stories.slice(0, 1)
          : selection === "top20"
            ? stories.slice(0, 2)
            : stories;
    await route.fulfill({
      json: {
        posts: available.slice(offset, offset + 3),
        next_cursor: offset + 3 < available.length ? String(offset + 3) : null,
        limited: false,
      },
    });
  });
  return requests;
}

for (const width of [390, 1280]) {
  test(`Hacker News navigation, filters and date scrolling work at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 844 });
    const requests = await fixtures(page);
    await page.goto("/reader/");
    if (width < 600)
      await page.getByRole("button", { name: "Open navigation" }).click();
    const navigation = page.getByRole("navigation", {
      name: "Main navigation",
    });
    const entry = navigation.getByRole("button", {
      name: "Hacker News",
      exact: true,
    });
    await expect(entry.locator("svg")).toHaveAttribute(
      "stroke",
      "currentColor",
    );
    await expect(entry.locator('[fill="#ff6600"]')).toHaveCount(0);
    await entry.click();
    await expect(page).toHaveURL(/view=hacker_news/);
    const selection = page.getByRole("group", {
      name: "Hacker News daily selection",
    });
    await expect(
      selection.getByRole("button", { name: "Top 50%", exact: true }),
    ).toHaveAttribute("aria-pressed", "true");
    await expect(
      page.getByText(
        "Filters apply to collected stories, using hckr news daily score cutoffs.",
      ),
    ).toBeVisible();
    await expect(
      page.getByRole("textbox", { name: "Search Hacker News" }),
    ).toBeVisible();
    await page.locator(".hn-card").last().scrollIntoViewIfNeeded();
    await expect
      .poll(async () => page.locator(".hn-card").count())
      .toBeGreaterThan(3);
    await expect(
      page.locator('.hn-day time[datetime="2026-10-16"]'),
    ).toBeVisible();
    await expect(
      page.locator('.hn-day time[datetime="2026-10-17"]'),
    ).toHaveCount(1);
    expect(
      requests.some(
        (url) =>
          url.searchParams.get("kind") === "hacker_news" &&
          url.searchParams.get("hn_filter") === "top50" &&
          url.searchParams.has("cursor"),
      ),
    ).toBe(true);
    for (const [label, value, count] of [
      ["Top 10", "top10", 1],
      ["Top 20", "top20", 2],
    ] as const) {
      await selection.getByRole("button", { name: label, exact: true }).click();
      await expect(page).toHaveURL(new RegExp(`hn_filter=${value}`));
      await expect(page.locator(".hn-card")).toHaveCount(count);
    }
    await selection.getByRole("button", { name: "All", exact: true }).click();
    await expect(page).toHaveURL(/hn_filter=all/);
    await expect(page.locator(".hn-card").first()).toBeVisible();
    await expect(page.locator(".hn-card .post-open p")).toHaveCount(0);
    const firstCard = page.locator(".hn-card").first();
    await expect(
      firstCard.locator('time[datetime="2026-10-15T11:25:00Z"]'),
    ).toBeVisible();
    await expect(firstCard.locator(".post-footer-byline")).toHaveText(
      "builder",
    );
    const metricColors = await firstCard
      .locator(".hn-metrics > .social-metric")
      .evaluateAll((metrics) =>
        metrics.map((metric) => getComputedStyle(metric).color),
      );
    expect(new Set(metricColors).size).toBe(1);
    await page.screenshot({
      path: testInfo.outputPath(`hacker-news-${width}.png`),
      fullPage: true,
    });
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect(
      (await new AxeBuilder({ page }).analyze()).violations.filter((item) =>
        ["serious", "critical"].includes(item.impact ?? ""),
      ),
    ).toEqual([]);
    await page
      .getByRole("link", { name: `Read ${stories[0].title}`, exact: true })
      .click();
    await expect(
      page.getByRole("heading", { name: "Comments not captured" }),
    ).toBeVisible();
    await expect(page.locator(".hn-detail > .article-body")).toHaveCount(0);
    await expect(
      page.getByRole("link", { name: "Read original article", exact: true }),
    ).toHaveAttribute("href", "https://example.com/software-1");
    await expect(
      page.getByRole("link", {
        name: "View discussion on Hacker News",
        exact: true,
      }),
    ).toHaveAttribute("href", stories[0].url);
    await page
      .getByRole("button", { name: "Back to stories", exact: true })
      .click();
    await expect(
      selection.getByRole("button", { name: "All", exact: true }),
    ).toHaveAttribute("aria-pressed", "true");
    await expect(page.locator(".hn-card").first()).not.toHaveClass(/is-read/);
  });
}

test("saved comments preserve nesting, expose partial capture, and sanitize untrusted HTML", async ({
  page,
}) => {
  await fixtures(page, {
    comment_capture: {
      status: "partial",
      captured: 2,
      total: 20,
      reason:
        "Comments retained from an earlier detailed scrape. comment_limit",
    },
    comments_items: [
      {
        id: "101",
        parent_id: "1",
        author: "reader",
        html: '<p>First reply <a href="https://example.com/reference">Reference</a></p><script>window.hnAttack=true</script><img src="/missing.png" onerror="window.hnAttack=true">',
        depth: 0,
        created_at: "2026-10-17T12:05:00Z",
      },
      {
        id: "102",
        parent_id: "101",
        author: "builder",
        html: '<p>Nested answer <a href="javascript:alert(1)">Unsafe</a></p><iframe src="https://untrusted.example"></iframe>',
        depth: 1,
        created_at: null,
      },
    ],
  });
  await page.goto(
    "/reader/?view=hacker_news&hn_filter=top20&article=1&article_source=news.ycombinator.com",
  );
  await expect(
    page.getByRole("heading", { name: "Partial discussion" }),
  ).toBeVisible();
  await expect(
    page.getByText(
      "Comments retained from an earlier detailed scrape. The configured comment limit was reached.",
    ),
  ).toBeVisible();
  await expect(page.getByText(/2 saved of 20 reported/)).toBeVisible();
  await expect(page.locator(".hn-comment")).toHaveCount(2);
  await expect(page.locator(".hn-comment").last()).toHaveAttribute(
    "style",
    /--comment-depth: 1/,
  );
  await expect(page.getByText("Reply at level 1")).toHaveCount(1);
  await expect(
    page.locator(
      '.hn-comments script, .hn-comments iframe, .hn-comments [onerror], .hn-comments a[href^="javascript:"]',
    ),
  ).toHaveCount(0);
  expect(
    await page.evaluate(() => Reflect.get(window, "hnAttack")),
  ).toBeUndefined();
  await expect(
    page.getByRole("link", { name: "Reference", exact: true }),
  ).toHaveAttribute("href", "https://example.com/reference");
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page.locator(".hn-card").first()).toHaveClass(/is-read/);
});

test("failed capture is explicit and API failure never becomes an empty feed", async ({
  page,
}) => {
  await fixtures(page, { comment_capture: { ...capture, status: "failed" } });
  await page.goto(
    "/reader/?view=hacker_news&article=1&article_source=news.ycombinator.com",
  );
  await expect(
    page.getByRole("heading", { name: "Comment collection failed" }),
  ).toBeVisible();
  await page.route("**/api/reader/posts?**", (route) =>
    route.fulfill({
      status: 503,
      json: { detail: "Hacker News data unavailable." },
    }),
  );
  await page.goto("/reader/?view=hacker_news");
  await expect(page.getByText("Hacker News data unavailable.")).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "No collected posts found" }),
  ).toHaveCount(0);
});

test("author filters preserve the Hacker News daily selection", async ({
  page,
}) => {
  await fixtures(page);
  await page.goto("/reader/?view=hacker_news&hn_filter=top20");
  await page.getByRole("link", { name: "Stories by builder" }).first().click();
  await expect(page).toHaveURL(/view=hacker_news/);
  await expect(page).toHaveURL(/hn_filter=top20/);
  await expect(page).toHaveURL(/author=builder/);
});

test("complete discussions show every captured comment beyond 500", async ({
  page,
}) => {
  await fixtures(page, {
    comment_capture: {
      status: "captured",
      captured: 650,
      total: 650,
      reason: null,
    },
    comments_items: Array.from({ length: 650 }, (_, index) => ({
      id: String(1000 + index),
      parent_id: index ? String(999 + index) : "1",
      author: "reader",
      html: `<p>Saved comment ${index + 1}</p>`,
      depth: Math.min(index, 32),
      created_at: null,
    })),
  });
  await page.goto(
    "/reader/?view=hacker_news&article=1&article_source=news.ycombinator.com",
  );
  await expect(page.locator(".hn-comment")).toHaveCount(650);
  await expect(
    page.getByText("Saved comment 650", { exact: true }),
  ).toHaveCount(1);
  await expect(page.getByText(/650 saved of 650 reported/)).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Partial discussion" }),
  ).toHaveCount(0);
});
