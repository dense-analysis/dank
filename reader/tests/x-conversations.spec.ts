import AxeBuilder from "@axe-core/playwright";
import { type BrowserContext, expect, type Page, test } from "@playwright/test";
import type { Post, XConversation, XConversationNode } from "../src/lib/types";

const capturedAt = "2026-10-17T00:10:00Z";
const attemptedAt = "2026-10-17T01:10:00Z";
const focusText = "The second part of a thread about mathematical discovery.";
const parentText = "First, a little context about the original question.";
const replyText = "A useful question, with a source worth reading.";
const continuationText = "The author adds an explanation to the conversation.";
const videoUrl = "https://www.youtube.com/watch?v=conversation-math";

function node(
  id: string,
  body: string,
  overrides: Partial<XConversationNode> = {},
): XConversationNode {
  return {
    id,
    domain: "x.com",
    url: `https://x.com/mathematician/status/${id}`,
    title: body,
    excerpt: body,
    html: `<p>${body}</p>`,
    author: "A Mathematician",
    created_at: "2026-10-16T17:41:00Z",
    source: "x",
    thumbnail: null,
    media: [],
    x: {
      handle: "mathematician",
      name: "A Mathematician",
      icon_url: "https://pbs.twimg.com/profile_images/math.jpg",
      links: [],
      media: [],
    },
    parent_id: null,
    depth: 0,
    is_author: false,
    missing_parent: false,
    ...overrides,
  };
}

const parent = node("parent", parentText, { is_author: true });
const reply = node("reply", replyText, {
  parent_id: "focus",
  author: "Curious Reader",
  html: `<p>${replyText}</p><p><a href="https://example.com/research">Read the research</a><a href="javascript:window.badReply=true">Unsafe link</a></p><script>window.badReply=true</script><iframe src="https://unsafe.example"></iframe>`,
  x: {
    handle: "curious_reader",
    name: "Curious Reader",
    icon_url: "https://pbs.twimg.com/profile_images/reader.jpg",
    links: [
      {
        url: videoUrl,
        title: "A reply with a linked explanation",
        description: "A captured reply retains its source.",
        image_url: "https://pbs.twimg.com/card_img/reply.jpg",
      },
    ],
    media: [
      {
        type: "image",
        url: "https://pbs.twimg.com/media/reply.jpg",
        poster_url: null,
        alt: "A diagram attached to the reply",
        remote: true,
      },
    ],
  },
});
const continuation = node("continuation", continuationText, {
  parent_id: "reply",
  depth: 1,
  is_author: true,
});
const conversation: XConversation = {
  status: "captured",
  captured_at: capturedAt,
  has_more: false,
  reason: null,
  reported_replies: 2,
  limit: 50,
  context: [parent],
  replies: [reply, continuation],
};

function post(capture: XConversation | null = conversation): Post {
  const focused = node("focus", focusText, {
    parent_id: "parent",
    is_author: true,
  });

  return {
    ...focused,
    x: {
      ...focused.x,
      handle: "mathematician",
      name: "A Mathematician",
      reply_count: capture?.reported_replies ?? 0,
      captured_at: capturedAt,
      conversation: capture ?? undefined,
    },
  };
}

async function mockConversation(context: BrowserContext, detail = post()) {
  const current = { detail, pause: undefined as Promise<void> | undefined };
  await context.route("https://pbs.twimg.com/**", (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180"><rect width="320" height="180" fill="#538573"/></svg>',
    }),
  );
  await context.route("https://www.youtube.com/**", (route) =>
    route.fulfill({ contentType: "text/html", body: "Reply video source" }),
  );
  await context.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());

    if (url.pathname.endsWith("/post")) {
      await current.pause;
      await route.fulfill({ json: current.detail });
      return;
    }

    const detail = current.detail;
    const { conversation: capture, ...profile } = detail.x ?? {};
    const summary = {
      ...detail,
      x: {
        ...profile,
        replies_status: capture?.status ?? "not_collected",
        replies_captured_count: capture?.replies.length ?? 0,
      },
    };

    await route.fulfill({
      json: url.pathname.endsWith("/sources")
        ? {
            sources: [
              {
                domain: "x.com",
                name: "X",
                kind: "x",
                tags: [],
                count: 1,
                accounts: [{ ...profile, count: 1 }],
              },
            ],
            total_posts: 1,
          }
        : { posts: [summary], next_cursor: null, limited: false },
    });
  });

  return current;
}

async function openConversation(page: Page) {
  await page.goto("/reader/?view=x&account=mathematician");
  await page
    .getByRole("link", { name: `Read ${focusText}`, exact: true })
    .click();
  await expect(page.getByRole("main", { name: "X post reader" })).toBeVisible();
}

test("opens parent context and sanitized replies with author identity and working attachments", async ({
  page,
  context,
}) => {
  await mockConversation(context);
  await openConversation(page);
  const reader = page.getByRole("main", { name: "X post reader" });
  const ancestors = reader.getByRole("region", { name: "Thread context" });
  const replies = reader.getByRole("region", {
    name: "Conversation",
    exact: true,
  });
  await expect(ancestors.getByText(parentText, { exact: true })).toBeVisible();
  await expect(
    replies.getByRole("heading", { name: "Captured replies" }),
  ).toBeVisible();

  for (const text of [parentText, focusText, replyText, continuationText]) {
    await expect(reader.getByText(text, { exact: true })).toHaveCount(1);
  }

  const positions = await Promise.all(
    [parentText, focusText, replyText, continuationText].map(async (text) => {
      const bounds = await reader
        .getByText(text, { exact: true })
        .boundingBox();
      expect(bounds).not.toBeNull();

      return bounds?.y ?? 0;
    }),
  );
  expect(positions).toEqual([...positions].sort((a, b) => a - b));
  const identity = replies.getByRole("link", {
    name: "Posts from @curious_reader",
    exact: true,
  });
  await expect(identity).toContainText("Curious Reader");
  await expect(identity).toContainText("@curious_reader");
  await expect(identity.locator("img")).toHaveJSProperty("naturalWidth", 320);
  await expect(replies.getByText("Author", { exact: true })).toHaveCount(1);
  await expect(
    replies.locator("script, iframe, a[href^='javascript:']"),
  ).toHaveCount(0);
  expect(
    await page.evaluate(() => Reflect.get(window, "badReply")),
  ).toBeUndefined();
  await expect(
    replies.getByRole("link", { name: "Read the research", exact: true }),
  ).toHaveAttribute("href", "https://example.com/research");
  const image = replies.getByRole("img", {
    name: "A diagram attached to the reply",
  });
  await image.scrollIntoViewIfNeeded();
  await expect(image).toHaveJSProperty("naturalWidth", 320);
  await expect(image.locator("..")).toHaveAttribute(
    "href",
    "https://pbs.twimg.com/media/reply.jpg",
  );
  const preview = replies.getByRole("link", {
    name: "Open link: A reply with a linked explanation",
  });
  await expect(preview).toHaveAttribute("href", videoUrl);
  const [tab] = await Promise.all([
    context.waitForEvent("page"),
    preview.click(),
  ]);
  await tab.waitForURL(videoUrl);
  expect(await tab.evaluate(() => window.opener === null)).toBe(true);
  await tab.close();
  await expect(replies.locator(`time[datetime="${capturedAt}"]`)).toHaveCount(
    1,
  );
});

test("labels limited and interrupted captures as partial and explains missing reply context", async ({
  page,
  context,
}) => {
  const orphan = node(
    "orphan",
    "An archived reply whose parent was not captured.",
    { parent_id: "missing", missing_parent: true },
  );
  const capture: XConversation = {
    ...conversation,
    status: "partial",
    has_more: true,
    reported_replies: 24,
    limit: 3,
    reason: "limit",
    replies: [...conversation.replies, orphan],
  };
  const current = await mockConversation(context, post(capture));
  await openConversation(page);
  const replies = page.getByRole("region", {
    name: "Conversation",
    exact: true,
  });
  await expect(
    replies.getByRole("heading", { name: "Partial conversation" }),
  ).toBeVisible();
  await expect(replies).toContainText("3 replies saved");
  await expect(replies).toContainText(/24 (?:replies )?reported/);
  await expect(replies).toContainText(/limit/i);
  await expect(
    replies.getByText("Parent post not captured", { exact: true }),
  ).toBeVisible();
  await expect(
    replies.getByRole("link", { name: "View conversation on X" }),
  ).toHaveAttribute("href", post().url);
  current.detail = post({ ...capture, reason: "timeout" });
  await page.reload();
  await expect(
    replies.getByRole("heading", { name: "Partial conversation" }),
  ).toBeVisible();
  await expect(replies).toContainText(/timed out|interrupted/i);
  await expect(replies.getByText(replyText, { exact: true })).toBeVisible();
  await expect(
    replies.getByText(/all replies captured|complete conversation/i),
  ).toHaveCount(0);
});

test("distinguishes captured empty conversations from missing captures and retains an archive after refresh failure", async ({
  page,
  context,
}) => {
  const empty = {
    ...conversation,
    context: [],
    replies: [],
    reported_replies: 0,
  };
  const current = await mockConversation(context, post(empty));
  await openConversation(page);
  const emptyMessage = page.getByText(
    "No replies were returned in this captured conversation.",
    { exact: true },
  );
  await expect(emptyMessage).toBeVisible();

  for (const status of [null, "not_collected", "disabled", "failed"] as const) {
    current.detail = post(
      status
        ? {
            ...empty,
            status,
            captured_at: null,
            reason: status === "failed" ? "login_required" : null,
          }
        : null,
    );
    await page.reload();
    await expect(
      page.getByRole("heading", {
        name: /Replies not collected|Conversation unavailable|Conversation capture disabled/,
      }),
    ).toBeVisible();
    await expect(emptyMessage).toHaveCount(0);
    await expect(
      page.getByRole("link", { name: "View conversation on X" }),
    ).toHaveAttribute("href", post().url);
  }

  current.detail = post({
    ...conversation,
    latest_attempt_status: "failed",
    latest_attempt_at: attemptedAt,
  });
  await page.reload();
  const replies = page.getByRole("region", {
    name: "Conversation",
    exact: true,
  });
  await expect(replies.getByText(replyText, { exact: true })).toBeVisible();
  await expect(replies).toContainText(
    /latest.*(?:failed|couldn.t|could not)|couldn.t refresh/i,
  );
  await expect(replies.locator(`time[datetime="${capturedAt}"]`)).toHaveCount(
    1,
  );
  await expect(replies.locator(`time[datetime="${attemptedAt}"]`)).toHaveCount(
    1,
  );
});

test("loads conversation detail separately from feed summaries and preserves search through reload and Back", async ({
  page,
  context,
}) => {
  const current = await mockConversation(context);
  const feedUrl =
    "/reader/?view=x&account=mathematician&q=discovery&sort=oldest&after=2026-10-01";
  await page.goto(feedUrl);
  await expect(page.getByText(replyText, { exact: true })).toHaveCount(0);
  let release = () => {};
  current.pause = new Promise<void>((resolve) => {
    release = resolve;
  });
  const open = page.getByRole("link", {
    name: `Read ${focusText}`,
    exact: true,
  });
  await open.click();
  await expect(
    page.getByText("Loading conversation…", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Replies not collected" }),
  ).toHaveCount(0);
  release();
  await expect(page.getByText(replyText, { exact: true })).toBeVisible();
  const directUrl = page.url();
  expect(Object.fromEntries(new URL(directUrl).searchParams)).toMatchObject({
    view: "x",
    account: "mathematician",
    q: "discovery",
    sort: "oldest",
    after: "2026-10-01",
    article: "focus",
    article_source: "x.com",
  });
  await page.reload();
  await expect(page.getByText(replyText, { exact: true })).toBeVisible();
  await page
    .getByRole("button", { name: "Back to stories", exact: true })
    .click();
  await expect(page).toHaveURL(new URL(feedUrl, page.url()).href);
  await expect(open).toBeVisible();
  await expect(page.getByText(replyText, { exact: true })).toHaveCount(0);
  await page.goto(directUrl);
  await expect(page.getByText(parentText, { exact: true })).toBeVisible();
  await expect(page.getByText(replyText, { exact: true })).toBeVisible();
});

for (const width of [390, 320]) {
  test(`keeps deeply nested replies readable and accessible at ${width}px`, async ({
    page,
    context,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    const deepText =
      "A deep reply retains enough width for comfortable reading.";
    await mockConversation(
      context,
      post({
        ...conversation,
        status: "partial",
        has_more: true,
        reason: "missing_context",
        reported_replies: 24,
        replies: [
          ...conversation.replies,
          node("deep", deepText, { parent_id: "continuation", depth: 20 }),
        ],
      }),
    );
    await openConversation(page);
    const replies = page.getByRole("region", {
      name: "Conversation",
      exact: true,
    });
    await expect(
      replies.getByRole("heading", { name: "Partial conversation" }),
    ).toBeVisible();
    const deep = replies.getByText(deepText, { exact: true });
    await deep.scrollIntoViewIfNeeded();
    const bounds = await deep.boundingBox();
    expect(bounds).not.toBeNull();
    expect(bounds?.width).toBeGreaterThan(width / 2);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: testInfo.outputPath(`x-conversation-${width}.png`),
      fullPage: true,
    });
    expect(
      (await new AxeBuilder({ page }).analyze()).violations.filter((item) =>
        ["serious", "critical"].includes(item.impact ?? ""),
      ),
    ).toEqual([]);
  });
}
