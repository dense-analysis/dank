import { fileURLToPath } from "node:url";
import AxeBuilder from "@axe-core/playwright";
import { type BrowserContext, expect, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

function makePost(id: string, html: string): Post {
  return {
    id,
    domain: "x.com",
    source: "x",
    url: `https://x.com/researcher/status/${id}`,
    author: "A Researcher",
    title: `Captured post ${id}`,
    excerpt: "A search excerpt must not replace the captured post.",
    html,
    created_at: "2026-10-16T12:00:00Z",
    thumbnail: null,
    media: [],
    x: {
      handle: "researcher",
      name: "A Researcher",
      icon_url: null,
      links: [],
      media: [],
      replies_status: "not_collected",
    },
  };
}

const mediumParagraphs = Array.from(
  { length: 6 },
  (_, index) =>
    "Section " +
    (index + 1) +
    ": This explanation keeps the reasoning together, including the context needed to understand the next step.",
);
const medium = makePost(
  "medium",
  '<p>Read <a href="https://example.com/research">the study</a> first.<br>Keep this line break.</p>' +
    mediumParagraphs.map((text) => `<p>${text}</p>`).join(""),
);
const articleParagraphs = Array.from(
  { length: 25 },
  (_, index) => `Paragraph ${index + 1}: ${"explanation ".repeat(30).trim()}`,
);
const ending = "The final conclusion is available in the full reader.";
const article = makePost(
  "article",
  articleParagraphs.map((text) => `<p>${text}</p>`).join("") +
    "<p>" +
    ending +
    "</p>",
);

async function mockPosts(context: BrowserContext, posts: Post[]) {
  await context.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());

    if (url.pathname.endsWith("/sources")) {
      await route.fulfill({
        json: {
          sources: [
            {
              domain: "x.com",
              name: "X",
              kind: "x",
              count: posts.length,
              tags: [],
              accounts: [
                {
                  handle: "researcher",
                  name: "A Researcher",
                  icon_url: null,
                  count: posts.length,
                },
              ],
            },
          ],
          total_posts: posts.length,
        },
      });
      return;
    }

    if (url.pathname.endsWith("/post")) {
      await route.fulfill({
        json: posts.find((post) => post.id === url.searchParams.get("id")),
      });
      return;
    }

    await route.fulfill({
      json: { posts, next_cursor: null, limited: false },
    });
  });
}

test("feed boundaries distinguish short, medium and article-length text", async ({
  page,
  context,
}) => {
  const posts = [
    makePost("at-preview", `<p>${"a".repeat(300)}</p>`),
    makePost("over-preview", `<p>${"b".repeat(301)}</p>`),
    makePost("below-article", `<p>${"c".repeat(2999)}</p>`),
    makePost("at-article", `<p>${"d".repeat(3000)}</p>`),
    makePost("below-words", `<p>${"a ".repeat(499).trim()}</p>`),
    makePost("at-words", `<p>${"a ".repeat(500).trim()}</p>`),
  ];
  await mockPosts(context, posts);
  await page.goto("/reader/?view=x");
  const cards = page.locator(".post-card");
  await expect(cards).toHaveCount(posts.length);
  await expect(
    cards.nth(0).getByRole("button", { name: "Show more", exact: true }),
  ).toHaveCount(0);
  await expect(cards.nth(0).locator(".article-body")).toHaveText(
    "a".repeat(300),
  );

  for (const index of [1, 2, 4]) {
    const card = cards.nth(index);
    await expect(
      card.getByRole("link", { name: /Read full post/ }),
    ).toHaveCount(0);
    await card.getByRole("button", { name: "Show more", exact: true }).click();
    await expect(card.locator(".article-body")).toHaveText(
      index === 1
        ? "b".repeat(301)
        : index === 2
          ? "c".repeat(2999)
          : "a ".repeat(499).trim(),
    );
  }

  for (const index of [3, 5]) {
    await expect(
      cards.nth(index).getByRole("link", { name: /Read full post/ }),
    ).toBeVisible();
  }
});

test("ordinary long posts expand completely with paragraphs, source links and search highlights", async ({
  page,
  context,
}) => {
  await mockPosts(context, [medium]);
  await page.goto("/reader/?view=x&q=explanation");
  const originalUrl = page.url();
  const card = page.locator(".post-card");
  const study = card.getByRole("link", { name: "the study", exact: true });
  await expect(study).toHaveAttribute("href", "https://example.com/research");
  await expect(study).toHaveAttribute("target", "_blank");
  await expect(study).toHaveAttribute("rel", "noopener noreferrer");
  await expect(card.locator(".article-body p").first()).toContainText(
    "first.\nKeep this line break.",
  );
  const toggle = card.getByRole("button", { name: "Show more", exact: true });
  await toggle.focus();
  await toggle.press("Enter");
  await expect(page).toHaveURL(originalUrl);
  const collapse = card.getByRole("button", {
    name: "Show less",
    exact: true,
  });
  await expect(collapse).toBeFocused();
  await expect(collapse).toHaveAttribute("aria-expanded", "true");
  const controlledId = await collapse.getAttribute("aria-controls");
  expect(controlledId).toBeTruthy();
  expect(
    await page.evaluate(
      (id) => !!id && document.getElementById(id) !== null,
      controlledId,
    ),
  ).toBe(true);

  for (const paragraph of mediumParagraphs) {
    await expect(card.getByText(paragraph, { exact: true })).toBeVisible();
  }

  await expect(card.locator(".article-body mark").first()).toHaveText(
    "explanation",
  );
  await expect(card.locator("a a")).toHaveCount(0);
  await expect(card.getByRole("link", { name: /Read full post/ })).toHaveCount(
    0,
  );
  await collapse.press("Enter");
  await expect(toggle).toBeFocused();
  await expect(toggle).toHaveAttribute("aria-expanded", "false");
  await expect(card.locator(".article-body")).not.toContainText(
    mediumParagraphs.at(-1) ?? "",
  );
});

for (const view of ["", "view=x", "view=x&account=researcher"]) {
  test(`article-length posts retain a cap in ${view || "All"}`, async ({
    page,
    context,
  }) => {
    await mockPosts(context, [article]);
    await page.goto(`/reader/?${view}`);
    const card = page.locator(".post-card");
    const fullPost = card.getByRole("link", { name: /Read full post/ });
    await expect(fullPost).toBeVisible();
    await expect(card).toContainText(/\d+ min read/);
    const collapsed = (await card.locator(".article-body").innerText()).length;
    expect(collapsed).toBeLessThanOrEqual(330);
    await card.getByRole("button", { name: "Show more", exact: true }).click();
    const expanded = (await card.locator(".article-body").innerText()).length;
    expect(expanded).toBeGreaterThan(collapsed);
    expect(expanded).toBeLessThanOrEqual(1250);
    await expect(card.locator(".article-body")).not.toContainText(ending);
    await expect(fullPost).toBeVisible();
    await expect(card.locator("a a")).toHaveCount(0);
  });
}

test("the full-post link supports new tabs and keyboard navigation and Back retains expansion, focus and scroll", async ({
  page,
  context,
}) => {
  const preceding = Array.from({ length: 4 }, (_, index) =>
    makePost(`preceding-${index}`, "<p>A short preceding post.</p>"),
  );
  await mockPosts(context, [...preceding, article]);
  await page.goto(
    "/reader/?view=x&account=researcher&q=explanation&sort=oldest",
  );
  const originalUrl = page.url();
  const card = page.locator(".post-card").last();
  await card.getByRole("button", { name: "Show more", exact: true }).click();
  const fullPost = card.getByRole("link", { name: /Read full post/ });
  const [tab] = await Promise.all([
    context.waitForEvent("page"),
    fullPost.click({ button: "middle" }),
  ]);
  const tabReader = tab.getByRole("main", { name: "X post reader" });
  await expect(tabReader.getByText(ending, { exact: true })).toBeVisible();
  expect(Object.fromEntries(new URL(tab.url()).searchParams)).toMatchObject({
    view: "x",
    account: "researcher",
    q: "explanation",
    sort: "oldest",
    article: article.id,
    article_source: "x.com",
  });
  await tab.close();
  await expect(page).toHaveURL(originalUrl);
  await fullPost.scrollIntoViewIfNeeded();
  await fullPost.focus();
  const scroll = await page.evaluate(() => window.scrollY);
  expect(scroll).toBeGreaterThan(100);
  await fullPost.press("Enter");
  const reader = page.getByRole("main", { name: "X post reader" });
  await expect(reader.getByText(ending, { exact: true })).toBeVisible();
  await expect(reader.getByRole("button", { name: "Show more" })).toHaveCount(
    0,
  );
  await expect(
    reader.getByRole("link", { name: /Read full post/ }),
  ).toHaveCount(0);
  await reader.getByRole("button", { name: "Back to stories" }).click();
  await expect(page).toHaveURL(originalUrl);
  await expect(
    card.getByRole("button", { name: "Show less", exact: true }),
  ).toHaveAttribute("aria-expanded", "true");
  await expect(fullPost).toBeFocused();
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scroll);
});

test("mobile expanded article previews remain bounded and accessible", async ({
  page,
  context,
}) => {
  await page.setViewportSize({ width: 320, height: 800 });
  await mockPosts(context, [article]);
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  await card.getByRole("button", { name: "Show more", exact: true }).click();
  await expect(
    card.getByRole("link", { name: /Read full post/ }),
  ).toBeVisible();
  expect((await card.locator(".article-body").innerText()).length).toBeLessThan(
    1250,
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  const accessibility = await new AxeBuilder({ page })
    .include(".post-card")
    .analyze();
  expect(accessibility.violations).toEqual([]);
});

test("expanding preserves attachment state and suppresses raw URLs represented by previews", async ({
  page,
  context,
}) => {
  const previewUrl = "https://example.com/linked-study";
  const videoUrl = "https://x.com/researcher/status/123/video/1";
  const post: Post = {
    ...medium,
    html:
      '<p><a href="' +
      previewUrl +
      '">' +
      previewUrl +
      '</a></p><p><a href="' +
      videoUrl +
      '">' +
      videoUrl +
      "</a></p>" +
      medium.html,
    x: {
      ...medium.x,
      handle: "researcher",
      name: "A Researcher",
      links: [
        {
          url: previewUrl,
          title: "The linked study",
          description: "A separate preview remains available.",
          image_url: null,
        },
      ],
      media: [
        {
          type: "video",
          url: "/assets/long-post-video.mp4",
          poster_url: null,
          alt: "A captured explanation",
          remote: false,
          source_url: videoUrl,
        },
      ],
    },
  };
  await mockPosts(context, [post]);
  await context.route("**/assets/long-post-video.mp4", (route) =>
    route.fulfill({
      contentType: "video/mp4",
      path: fileURLToPath(
        new URL("./fixtures/reddit-video/archived.mp4", import.meta.url),
      ),
    }),
  );
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  const video = card.locator("video");
  await video.scrollIntoViewIfNeeded();
  await video.evaluate(async (element) => {
    const player = element as HTMLVideoElement;
    player.muted = true;
    await player.play();
  });
  await expect
    .poll(() =>
      video.evaluate((element) => (element as HTMLVideoElement).currentTime),
    )
    .toBeGreaterThan(0.1);
  const savedTime = await video.evaluate((element) => {
    const player = element as HTMLVideoElement;
    player.pause();

    return player.currentTime;
  });
  const originalVideo = await video.elementHandle();

  for (const label of ["Show more", "Show less"]) {
    await card.getByRole("button", { name: label, exact: true }).click();
    await expect(card.locator(".article-body")).not.toContainText(previewUrl);
    await expect(card.locator(".article-body")).not.toContainText(videoUrl);
    await expect(
      card.getByRole("link", { name: "Open link: The linked study" }),
    ).toHaveAttribute("href", previewUrl);
    expect(
      await video.evaluate(
        (element, original) => element === original,
        originalVideo,
      ),
    ).toBe(true);
    await expect(video).toHaveJSProperty("currentTime", savedTime);
  }

  await originalVideo?.dispose();
});

test("long conversation replies fully expand when no internal reader destination exists", async ({
  page,
  context,
}) => {
  const focused = makePost(
    "conversation-parent",
    "<p>A short parent post.</p>",
  );
  const post: Post = {
    ...focused,
    x: {
      ...focused.x,
      handle: "researcher",
      name: "A Researcher",
      replies_status: "captured",
      replies_captured_count: 1,
      conversation: {
        status: "captured",
        captured_at: "2026-10-17T00:00:00Z",
        has_more: false,
        reason: null,
        reported_replies: 1,
        limit: 50,
        context: [],
        replies: [
          {
            ...article,
            parent_id: focused.id,
            depth: 0,
            is_author: true,
            missing_parent: false,
          },
        ],
      },
    },
  };
  await mockPosts(context, [post]);
  await page.goto(
    "/reader/?view=x&article=conversation-parent&article_source=x.com",
  );
  const reader = page.getByRole("main", { name: "X post reader" });
  const reply = reader.locator(".x-conversation-entry");
  await expect(reply).toHaveCount(1);
  await expect(reply.getByText(ending, { exact: true })).toHaveCount(0);
  await expect(reply.getByRole("link", { name: /Read full post/ })).toHaveCount(
    0,
  );
  const originalUrl = page.url();
  await reply.getByRole("button", { name: "Show more", exact: true }).click();
  await expect(page).toHaveURL(originalUrl);
  await expect(reply.getByText(ending, { exact: true })).toBeVisible();
  await expect(reply.locator(".article-body p")).toHaveCount(
    articleParagraphs.length + 1,
  );
  await expect(reply.getByRole("link", { name: /Read full post/ })).toHaveCount(
    0,
  );
  await reply.getByRole("button", { name: "Show less", exact: true }).click();
  await expect(reply.getByText(ending, { exact: true })).toHaveCount(0);
  const header = await reply.locator(".x-post-header").boundingBox();
  const toolbar = await page.locator(".reader-toolbar").boundingBox();
  expect(header?.y).toBeGreaterThanOrEqual(
    (toolbar?.y ?? 0) + (toolbar?.height ?? 0),
  );
  expect((header?.y ?? 0) + (header?.height ?? 0)).toBeLessThan(720);
  await expect(
    reply.getByRole("button", { name: "Show more", exact: true }),
  ).toBeFocused();
});

for (const [width, query] of [
  [1440, ""],
  [1440, "view=x&account=researcher"],
  [390, "view=x&account=researcher"],
] as const) {
  test(`Show less returns to the same post below the header at ${width}px in ${query || "All"}`, async ({
    page,
    context,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    const filler = (id: string) =>
      makePost(id, `<p>${"A short surrounding post. ".repeat(5)}</p>`);
    await mockPosts(context, [
      ...Array.from({ length: 3 }, (_, index) => filler(`before-${index}`)),
      article,
      ...Array.from({ length: 6 }, (_, index) => filler(`after-${index}`)),
    ]);
    await page.goto(`/reader/?${query}`);
    const originalUrl = page.url();
    const card = page.locator(".post-card").filter({
      has: page.getByRole("link", {
        name: `Read ${article.title}`,
        exact: true,
      }),
    });

    for (const input of ["pointer", "keyboard"]) {
      await card
        .getByRole("button", { name: "Show more", exact: true })
        .click();
      const collapse = card.getByRole("button", {
        name: "Show less",
        exact: true,
      });
      await collapse.evaluate((button) =>
        window.scrollBy(0, button.getBoundingClientRect().top - 160),
      );
      expect(
        (await card.locator(".x-post-header").boundingBox())?.y,
      ).toBeLessThan(0);

      if (input === "keyboard") {
        await collapse.focus();
        await collapse.press("Enter");
      } else {
        await collapse.click();
      }

      const expand = card.getByRole("button", {
        name: "Show more",
        exact: true,
      });
      await expect(expand).toHaveAttribute("aria-expanded", "false");
      await expect(expand).toBeFocused();
      await expect(page).toHaveURL(originalUrl);
      const edge = query ? 56 : 0;
      await expect
        .poll(async () =>
          Math.abs(((await card.boundingBox())?.y ?? -1000) - edge),
        )
        .toBeLessThanOrEqual(2);
      const header = await card.locator(".x-post-header").boundingBox();
      expect(header?.y).toBeGreaterThanOrEqual(edge);
      expect((header?.y ?? 0) + (header?.height ?? 0)).toBeLessThan(900);
    }
  });
}
