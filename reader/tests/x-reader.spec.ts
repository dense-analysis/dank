import { fileURLToPath } from "node:url";
import AxeBuilder from "@axe-core/playwright";
import {
  type BrowserContext,
  expect,
  type Locator,
  test,
} from "@playwright/test";
import type { Post } from "../src/lib/types";

const postText =
  "Some reflections on AI in math. I found it helpful to simply list out the various parts of what a mathematician does, why math is valuable, and where the funding comes from. From there, what happens if AI gets more capable?";
const videoTitle = "There's never been a better time to go into math";
const videoUrl = "https://www.youtube.com/watch?v=math-video";
const profile = {
  handle: "3blue1brown",
  name: "Grant Sanderson",
  icon_url: "https://pbs.twimg.com/profile_images/123/grant.jpg",
  links: [
    {
      url: videoUrl,
      title: videoTitle,
      description: "Sharing a few thoughts on the future of math.",
      image_url: "https://pbs.twimg.com/card_img/123/math.jpg",
    },
  ],
  media: [],
  reply_count: 24,
  repost_count: 136,
  like_count: 931,
  view_count: 54000,
  replies_status: "not_collected" as const,
  captured_at: "2026-10-17T00:00:00Z",
};
const grant: Post = {
  id: "2111135480116850944",
  domain: "x.com",
  url: "https://x.com/3blue1brown/status/2111135480116850944",
  author: "Grant Sanderson",
  title: postText,
  excerpt: postText,
  html: `<p>${postText}</p>`,
  source: "x",
  created_at: "2026-10-16T17:41:00Z",
  x: profile,
  media: [],
  thumbnail: null,
};
const paragraphs = Array.from(
  { length: 12 },
  (_, index) =>
    `Paragraph ${index + 1}: A longer note keeps its full explanation, with enough context to understand the idea without losing the end of the post.`,
);
const longProfile = {
  ...profile,
  links: [],
  reply_count: null,
  repost_count: null,
  like_count: null,
  view_count: null,
};
const longNote: Post = {
  ...grant,
  id: "long-note",
  title: "A shortened legacy title that should not become a headline",
  excerpt: "A shortened search excerpt should not replace the full note.",
  html:
    paragraphs.map((paragraph) => `<p>${paragraph}</p>`).join("") +
    '<p>Discuss with <a href="https://x.com/colleague">@colleague</a> about <a href="https://x.com/hashtag/math">#math</a>. See <a href="https://example.com/research">the complete research</a>.</p>',
  x: longProfile,
};
const attachments = {
  ...profile,
  links: [],
  reply_count: 0,
  repost_count: 0,
  like_count: 0,
  view_count: 0,
  media: [
    {
      type: "image" as const,
      url: "/assets/x-photo.svg",
      poster_url: null,
      alt: "A mathematical sketch",
      remote: false,
    },
    {
      type: "video" as const,
      url: "/assets/x-video.mp4",
      poster_url: "/assets/x-photo.svg",
      alt: "A mathematical animation",
      remote: false,
    },
    {
      type: "gif" as const,
      url: "https://video.twimg.com/animated.mp4",
      poster_url: "/assets/x-photo.svg",
      alt: "A repeating mathematical animation",
      remote: true,
    },
  ],
};
const mediaPost: Post = {
  ...grant,
  id: "media-only",
  title: "Media-only post",
  excerpt: "",
  html: "",
  x: attachments,
};

async function mockX(context: BrowserContext, posts = [grant]) {
  const image = {
    contentType: "image/svg+xml",
    body: '<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180"><rect width="320" height="180" fill="#538573"/></svg>',
  };
  await context.route("https://pbs.twimg.com/**", (route) =>
    route.fulfill(image),
  );
  await context.route("**/assets/x-photo.svg", (route) => route.fulfill(image));
  await context.route("**/assets/x-video.mp4", (route) =>
    route.fulfill({
      contentType: "video/mp4",
      path: fileURLToPath(
        new URL("./fixtures/reddit-video/archived.mp4", import.meta.url),
      ),
    }),
  );
  await context.route("https://video.twimg.com/**", (route) =>
    route.fulfill({
      contentType: "video/mp4",
      path: fileURLToPath(
        new URL("./fixtures/reddit-video/archived.mp4", import.meta.url),
      ),
    }),
  );
  await context.route("https://www.youtube.com/**", (route) =>
    route.fulfill({
      contentType: "text/html",
      body: "Captured video destination",
    }),
  );
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
              accounts: [{ ...profile, count: posts.length }],
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

function openPost(scope: Locator, post = grant) {
  return scope.getByRole("link", { name: `Read ${post.title}`, exact: true });
}

async function expectIdentity(scope: Locator) {
  const identity = scope.getByRole("link", {
    name: "Posts from @3blue1brown",
    exact: true,
  });
  await expect(identity).toContainText("Grant Sanderson");
  await expect(identity).toContainText("@3blue1brown");
  await expect(identity).toHaveAttribute(
    "href",
    "/reader/?view=x&account=3blue1brown",
  );
  await expect(identity.locator("img")).toHaveJSProperty("naturalWidth", 320);
  await expect(scope.getByText(/min read|YOU’VE REACHED THE END/)).toHaveCount(
    0,
  );
}

for (const width of [1440, 390, 320]) {
  test(`X cards and opened posts show identity, one body and linked media at ${width}px`, async ({
    page,
    context,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    await mockX(context);

    for (const query of ["", "view=x", "view=x&account=3blue1brown"]) {
      await page.goto(`/reader/?${query}`);
      const card = page.locator(".post-card");
      await expectIdentity(card);
      await expect(card.getByText(postText, { exact: true })).toHaveCount(1);
      await expect(card.locator("h2")).toHaveCount(0);
      await expect(card.getByText("Open post", { exact: true })).toHaveCount(0);
      const engagement = card.getByRole("group", {
        name: "Engagement when collected",
      });
      await expect(
        engagement.getByRole("link", {
          name: "24 comments reported when collected",
          exact: true,
        }),
      ).toBeVisible();
      await expect(engagement).toContainText("136");
      await expect(engagement).toContainText("931");
      await expect(engagement).toContainText(/54K/i);
      expect(
        await card
          .locator(".article-body")
          .evaluate((element) => Number(getComputedStyle(element).fontWeight)),
      ).toBeGreaterThan(400);

      if (width === 1440) {
        const name = await card.locator(".account-name").boundingBox();
        const timestamp = await card
          .locator(".x-post-header time")
          .boundingBox();
        expect(name).not.toBeNull();
        expect(timestamp).not.toBeNull();
        expect(
          Math.abs((name?.y ?? 0) - (timestamp?.y ?? 0)),
        ).toBeLessThanOrEqual(3);
      }
      const preview = card.getByRole("link", {
        name: `Open link: ${videoTitle}`,
      });
      await expect(preview).toHaveAttribute("href", videoUrl);
      await expect(preview.locator("img")).toHaveJSProperty(
        "naturalWidth",
        320,
      );
      await expect(preview).toHaveAttribute("target", "_blank");
      await card.getByText(postText, { exact: true }).click();
      const reader = page.getByRole("main", { name: "X post reader" });
      await expectIdentity(reader);
      await expect(reader.getByText(postText, { exact: true })).toHaveCount(1);
      await expect(
        reader.getByRole("heading", {
          name: "Post by Grant Sanderson",
          level: 1,
        }),
      ).toHaveCount(1);
      await expect(reader.getByRole("heading", { name: postText })).toHaveCount(
        0,
      );
      await expect(
        reader.getByRole("link", { name: `Open link: ${videoTitle}` }),
      ).toHaveAttribute("href", videoUrl);
      const replies = reader.getByRole("region", { name: "Reply collection" });
      await expect(
        replies.getByRole("heading", { name: "Replies not collected" }),
      ).toBeVisible();
      await expect(replies).toContainText(
        "24 replies reported when collected.",
      );
      await expect(
        replies.getByRole("link", { name: "View conversation on X" }),
      ).toHaveAttribute("href", grant.url);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);
      await reader
        .getByRole("button", { name: "Back to stories", exact: true })
        .click();
      await expect(card.getByText(postText, { exact: true })).toBeFocused();
      await expect(card.getByText("Read", { exact: true })).toBeVisible();
    }

    await openPost(page.locator(".post-card")).click();
    await page.screenshot({
      path: testInfo.outputPath(`x-reader-${width}.png`),
      fullPage: true,
    });
    expect(
      (await new AxeBuilder({ page }).analyze()).violations.filter((item) =>
        ["serious", "critical"].includes(item.impact ?? ""),
      ),
    ).toEqual([]);
  });
}

test("preview image failure retains the working destination and keyboard target", async ({
  page,
  context,
}) => {
  await mockX(context);
  await context.route("https://pbs.twimg.com/card_img/**", (route) =>
    route.fulfill({ status: 404 }),
  );
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  const preview = card.getByRole("link", { name: `Open link: ${videoTitle}` });
  await expect(preview).toBeVisible();
  await expect(preview).toContainText(videoTitle);
  await preview.focus();
  await expect(preview).toBeFocused();
  const originalUrl = page.url();
  const [tab] = await Promise.all([
    context.waitForEvent("page"),
    preview.press("Enter"),
  ]);
  await tab.waitForURL(videoUrl);
  expect(await tab.evaluate(() => window.opener === null)).toBe(true);
  await tab.close();
  await expect(page).toHaveURL(originalUrl);
  await expect(card).toBeVisible();
  await openPost(card).focus();
  await openPost(card).press("Enter");
  await expect(page.getByRole("main", { name: "X post reader" })).toBeVisible();
});

test("long notes retain paragraphs and separate safe inline links from opening the post", async ({
  page,
  context,
}) => {
  await mockX(context, [longNote]);
  await page.goto("/reader/?view=x&q=explanation");
  const card = page.locator(".post-card");
  await expect(card.getByText(longNote.title, { exact: true })).toHaveCount(0);
  await expect(card.getByText(longNote.excerpt, { exact: true })).toHaveCount(
    0,
  );
  await card.getByRole("button", { name: "Show more", exact: true }).click();
  await expect(
    card.getByText(paragraphs.at(-1) ?? "", { exact: true }),
  ).toBeVisible();
  const research = card.getByRole("link", {
    name: "the complete research",
    exact: true,
  });
  await expect(research).toHaveAttribute(
    "href",
    "https://example.com/research",
  );
  await expect(research).toHaveAttribute("target", "_blank");
  await expect(research).toHaveAttribute("rel", "noopener noreferrer");
  await expect(card.locator("a a")).toHaveCount(0);
  await openPost(card, longNote).click();
  const reader = page.getByRole("main", { name: "X post reader" });

  for (const paragraph of paragraphs) {
    await expect(reader.getByText(paragraph, { exact: true })).toHaveCount(1);
  }

  await expect(
    reader.getByRole("link", { name: "@colleague", exact: true }),
  ).toHaveAttribute("href", "https://x.com/colleague");
  await expect(
    reader.getByRole("link", { name: "#math", exact: true }),
  ).toHaveAttribute("href", "https://x.com/hashtag/math");
  await expect(reader.getByRole("button", { name: "Show more" })).toHaveCount(
    0,
  );
  await expect(
    reader.getByRole("region", { name: "Reply collection" }),
  ).not.toContainText(/\d+ replies reported/);
});

test("media-only posts remain readable and never interpret zero reported replies as collected", async ({
  page,
  context,
}) => {
  await mockX(context, [mediaPost]);
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  await expect(card.getByText("Not collected", { exact: true })).toHaveCount(0);
  const image = card.getByRole("img", {
    name: "A mathematical sketch",
    exact: true,
  });
  await expect(image).toHaveJSProperty("naturalWidth", 320);
  await expect(image.locator("..")).toHaveAttribute(
    "href",
    /\/assets\/x-photo.svg$/,
  );
  await openPost(card, mediaPost).click();
  const reader = page.getByRole("main", { name: "X post reader" });
  await expect(
    reader.getByRole("img", { name: "A mathematical sketch", exact: true }),
  ).toHaveAttribute("src", /\/assets\/x-photo.svg$/);
  const videos = reader.locator("video");
  await expect(videos).toHaveCount(2);
  await expect(videos.nth(0)).toHaveAttribute("src", /\/assets\/x-video.mp4$/);
  await expect(videos.nth(1)).toHaveAttribute(
    "src",
    "https://video.twimg.com/animated.mp4",
  );

  for (const video of await videos.all()) {
    await expect(video).toHaveJSProperty("controls", true);
    await expect(video).toHaveJSProperty("autoplay", false);
    await expect(video).toHaveAttribute("preload", "none");
    await expect(video).toHaveAttribute("poster", /\/assets\/x-photo.svg$/);
    await video.evaluate(async (element) => {
      const player = element as HTMLVideoElement;
      player.muted = true;
      await player.play();
    });
    await expect
      .poll(() =>
        video.evaluate((element) => (element as HTMLVideoElement).currentTime),
      )
      .toBeGreaterThan(0);
    await video.evaluate((element) => (element as HTMLVideoElement).pause());
  }

  const replies = reader.getByRole("region", { name: "Reply collection" });
  await expect(
    replies.getByRole("heading", { name: "Replies not collected" }),
  ).toBeVisible();
  await expect(replies).toContainText("0 replies reported when collected.");
  await expect(
    reader.getByText(/Full text not collected|No replies|No comments/),
  ).toHaveCount(0);
  await reader
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(card.getByText("Read", { exact: true })).toBeVisible();
});

test("post links retain search context in a new tab and Back restores the card and scroll", async ({
  page,
  context,
}) => {
  const repeated = Array.from({ length: 6 }, (_, index) => ({
    ...grant,
    id: `post-${index}`,
    title: `Math dispatch ${index + 1}`,
  }));
  await mockX(context, repeated);
  await page.goto(
    "/reader/?view=x&account=3blue1brown&q=math&sort=oldest&after=2026-10-01",
  );
  const originalUrl = page.url();
  const target = page.getByRole("link", {
    name: `Read ${repeated[5].title}`,
    exact: true,
  });
  await target.scrollIntoViewIfNeeded();
  const scroll = await page.evaluate(() => window.scrollY);
  expect(scroll).toBeGreaterThan(100);
  const [tab] = await Promise.all([
    context.waitForEvent("page"),
    target.click({ button: "middle" }),
  ]);
  await expect(tab.getByRole("main", { name: "X post reader" })).toBeVisible();
  expect(Object.fromEntries(new URL(tab.url()).searchParams)).toMatchObject({
    view: "x",
    account: "3blue1brown",
    q: "math",
    sort: "oldest",
    after: "2026-10-01",
    article: "post-5",
    article_source: "x.com",
  });
  await tab.close();
  await expect(page).toHaveURL(originalUrl);
  await target.click();
  await expect(page.getByRole("main", { name: "X post reader" })).toBeVisible();
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page).toHaveURL(originalUrl);
  await expect(target).toBeFocused();
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scroll);
});

const shortParagraphs = [
  "A first observation from the collected post opens the full discussion.",
  "A second paragraph stays independently clickable without repeating a headline.",
];
const textPost: Post = {
  ...grant,
  id: "text-interactions",
  title: "Collected post with separate paragraphs and links",
  html:
    shortParagraphs.map((paragraph) => `<p>${paragraph}</p>`).join("") +
    '<p>Read <a href="https://example.com/research">the complete research</a> with <a href="https://x.com/colleague">@colleague</a>.</p>',
  x: { ...profile, links: [] },
};

for (const width of [1440, 390, 320]) {
  test(`ordinary post paragraphs open the reader and Back restores focus at ${width}px`, async ({
    page,
    context,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await mockX(context, [textPost]);
    await page.goto("/reader/?view=x&account=3blue1brown&q=collected");
    const originalUrl = page.url();

    for (const paragraph of shortParagraphs) {
      const card = page.locator(".post-card");
      await card.getByText(paragraph, { exact: true }).click();
      await expect(
        page.getByRole("main", { name: "X post reader" }),
      ).toBeVisible();
      await page
        .getByRole("button", { name: "Back to stories", exact: true })
        .click();
      await expect(page).toHaveURL(originalUrl);
      await expect(card.getByText(paragraph, { exact: true })).toBeFocused();
    }

    await page
      .locator(".post-card")
      .getByRole("link", {
        name: "24 comments reported when collected",
        exact: true,
      })
      .click();
    await expect(
      page.getByRole("region", { name: "Reply collection" }),
    ).toContainText("24 replies reported when collected.");
  });
}

test("selecting post text leaves the feed open and preserves the selected words", async ({
  page,
  context,
}) => {
  await mockX(context, [textPost]);
  await page.goto("/reader/?view=x");
  const originalUrl = page.url();
  const paragraph = page
    .locator(".post-card")
    .getByText(shortParagraphs[0], { exact: true });
  const points = await paragraph.evaluate((element) => {
    const node = element.firstChild;

    if (!node || node.nodeType !== Node.TEXT_NODE)
      throw new Error("Expected a text run");

    const range = document.createRange();
    const point = (offset: number) => {
      range.setStart(node, offset);
      range.setEnd(node, offset + 1);
      const rect = range.getBoundingClientRect();

      return {
        x: Math.round(rect.x + rect.width / 2),
        y: Math.round(rect.y + rect.height / 2),
      };
    };

    return { start: point(2), end: point(38) };
  });
  await page.mouse.move(points.start.x, points.start.y);
  await page.mouse.down();
  await page.mouse.move(points.end.x, points.end.y, { steps: 12 });
  await page.mouse.up();
  await expect
    .poll(() =>
      page.evaluate(() => window.getSelection()?.toString().length ?? 0),
    )
    .toBeGreaterThan(20);
  await expect(page).toHaveURL(originalUrl);
  await expect(page.getByRole("main", { name: "X post reader" })).toHaveCount(
    0,
  );
});

for (const gesture of ["middle", "modifier"] as const) {
  test(`${gesture} clicking post text opens a new tab with the current feed context`, async ({
    page,
    context,
  }) => {
    await mockX(context, [textPost]);
    await page.goto(
      "/reader/?view=x&account=3blue1brown&q=collected&sort=oldest",
    );
    const originalUrl = page.url();
    const text = page
      .locator(".post-card")
      .getByText(shortParagraphs[1], { exact: true });
    const [tab] = await Promise.all([
      context.waitForEvent("page"),
      text.click(
        gesture === "middle"
          ? { button: "middle" }
          : { modifiers: ["ControlOrMeta"] },
      ),
    ]);
    await expect(
      tab.getByRole("main", { name: "X post reader" }),
    ).toBeVisible();
    expect(Object.fromEntries(new URL(tab.url()).searchParams)).toMatchObject({
      view: "x",
      account: "3blue1brown",
      q: "collected",
      sort: "oldest",
      article: textPost.id,
      article_source: "x.com",
    });
    await tab.close();
    await expect(page).toHaveURL(originalUrl);
  });
}

test("inline destinations and mentions remain independent from opening the card", async ({
  page,
  context,
}) => {
  await mockX(context, [textPost]);
  await context.route("https://example.com/**", (route) =>
    route.fulfill({ body: "Research destination" }),
  );
  await context.route("https://x.com/colleague", (route) =>
    route.fulfill({ body: "Colleague destination" }),
  );
  await page.goto("/reader/?view=x");
  const originalUrl = page.url();
  const card = page.locator(".post-card");
  await expect(card.locator("a a")).toHaveCount(0);

  for (const [name, url] of [
    ["the complete research", "https://example.com/research"],
    ["@colleague", "https://x.com/colleague"],
  ]) {
    const [tab] = await Promise.all([
      context.waitForEvent("page"),
      card.getByRole("link", { name, exact: true }).click(),
    ]);
    await tab.waitForURL(url);
    expect(await tab.evaluate(() => window.opener === null)).toBe(true);
    await tab.close();
    await expect(page).toHaveURL(originalUrl);
    await expect(page.getByRole("main", { name: "X post reader" })).toHaveCount(
      0,
    );
  }
});

test("playing a feed video does not open the post", async ({
  page,
  context,
}) => {
  await mockX(context, [mediaPost]);
  await page.goto("/reader/?view=x");
  const originalUrl = page.url();
  const video = page.locator(".post-card video").first();
  await video.scrollIntoViewIfNeeded();
  await video.evaluate((element) => {
    (element as HTMLVideoElement).muted = true;
  });
  const box = await video.boundingBox();
  expect(box).not.toBeNull();
  await video.hover();
  await video.click({ position: { x: 24, y: (box?.height ?? 0) - 48 } });
  await expect
    .poll(() =>
      video.evaluate((element) => (element as HTMLVideoElement).currentTime),
    )
    .toBeGreaterThan(0);
  await expect(page).toHaveURL(originalUrl);
  await expect(page.getByRole("main", { name: "X post reader" })).toHaveCount(
    0,
  );
});

test("engagement distinguishes uncaptured counts from captured zeroes", async ({
  page,
  context,
}) => {
  await mockX(context, [longNote, mediaPost]);
  await page.goto("/reader/?view=x");
  const unknownCard = page.locator(".post-card").filter({
    has: page.getByRole("link", {
      name: `Read ${longNote.title}`,
      exact: true,
    }),
  });
  const unknown = unknownCard.getByRole("group", {
    name: "Engagement when collected",
  });
  await expect(
    unknown.getByRole("link", { name: "Comments not captured", exact: true }),
  ).toBeVisible();

  for (const metric of ["Reposts", "Likes", "Views"]) {
    await expect(
      unknown.getByTitle(`${metric} not captured`, { exact: true }),
    ).toBeVisible();
  }

  const zeroCard = page.locator(".post-card").filter({
    has: page.getByRole("link", {
      name: `Read ${mediaPost.title}`,
      exact: true,
    }),
  });
  const zero = zeroCard.getByRole("group", {
    name: "Engagement when collected",
  });
  await expect(
    zero.getByRole("link", {
      name: "0 comments reported when collected",
      exact: true,
    }),
  ).toBeVisible();

  for (const metric of ["reposts", "likes", "views"]) {
    await expect(
      zero.getByTitle(`0 ${metric} reported when collected`, { exact: true }),
    ).toBeVisible();
  }
});

test("saved conversation status has its own row below engagement on a narrow read card", async ({
  page,
  context,
}) => {
  await page.setViewportSize({ width: 320, height: 900 });
  const captured: Post = {
    ...grant,
    x: { ...profile, replies_status: "partial", replies_captured_count: 8 },
  };
  await mockX(context, [captured]);
  await page.addInitScript(
    (id) =>
      localStorage.setItem(
        "dank-reader:read:v1",
        JSON.stringify([JSON.stringify(["x.com", id])]),
      ),
    grant.id,
  );
  await page.goto("/reader/?view=x");
  const card = page.locator(".post-card");
  const group = card.getByRole("group", { name: "Engagement when collected" });
  const summary = card.getByText("8 saved · partial", { exact: true });
  await expect(summary).toBeVisible();
  const metricsBox = await group.boundingBox();
  const summaryBox = await summary.boundingBox();
  expect(metricsBox).not.toBeNull();
  expect(summaryBox).not.toBeNull();
  expect(summaryBox?.y ?? 0).toBeGreaterThanOrEqual(
    (metricsBox?.y ?? 0) + (metricsBox?.height ?? 0),
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
