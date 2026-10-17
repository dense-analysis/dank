import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { PostConnection } from "../src/lib/postConnections";
import type { Post, PostReference } from "../src/lib/types";

function destination(kind: "blog" | "x" | "reddit"): Post {
  const domain = kind === "blog" ? "example.com" : `${kind}.com`;
  return {
    id: "original",
    domain,
    url:
      kind === "x"
        ? "https://x.com/writer/status/123456789"
        : `https://${domain}/article`,
    source: kind === "blog" ? "rss" : kind,
    title: "Original source title",
    author: "writer",
    excerpt: "The captured original article explains the research.",
    html: "<p>The captured original article explains the research.</p>",
    created_at: "2026-10-15T10:00:00Z",
    media: [],
    thumbnail: null,
    ...(kind === "reddit"
      ? {
          reddit: {
            community: "research",
            score: 15,
            comments: 3,
            flair: null,
          },
        }
      : {}),
  };
}
function reference(post: Post): PostReference {
  const { domain, id, url, title, source } = post;
  return { domain, id, url, title, source };
}
async function fixtures(
  page: Page,
  kind: "blog" | "x" | "reddit",
  overrides: Partial<Post> = {},
) {
  const original = { ...destination(kind), ...overrides };
  const story: Post = {
    ...original,
    id: "1",
    domain: "news.ycombinator.com",
    source: "hacker_news",
    url: "https://news.ycombinator.com/item?id=1",
    title: "Hacker News discussion title",
    author: "submitter",
    html: `<p>${original.url}</p>`,
    excerpt: original.url,
    reddit: undefined,
    linked_post: reference(original),
    hacker_news: {
      submitted_at: "2026-10-16T09:00:00Z",
      day: "2026-10-17",
      rank: 1,
      points: 126,
      comments: 1,
      discussion_url: "https://news.ycombinator.com/item?id=1",
      external_url: original.url,
      comment_capture: {
        status: "captured",
        captured: 1,
        total: 1,
        reason: null,
      },
      comments_items: [
        {
          id: "10",
          parent_id: "1",
          author: "reader",
          html: "<p>A captured Hacker News reply.</p>",
          depth: 0,
          created_at: null,
        },
      ],
    },
  };
  original.links = [
    reference(story),
    {
      ...reference(story),
      id: "2",
      title: "Another collected discussion",
      url: "https://news.ycombinator.com/item?id=2",
    },
  ];
  original.links_count = 2;
  const incoming: PostConnection[] = original.links.map((origin) => ({
    id: `incoming-${origin.id}`,
    direction: "incoming",
    origin,
    target: {
      url: original.url,
      post: reference(original),
      youtube_item_id: null,
    },
    evidence: {
      domain: origin.domain,
      post_id: origin.id,
      kind: "post",
      reference_id: "",
      reference_url: origin.url,
      author: story.author,
      matched_url: original.url,
      excerpt: `Posted the original source: ${original.title}`,
      observed_at: story.created_at,
    },
  }));
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.includes("/youtube/")) {
      await route.fulfill({
        json: { feeds: [], channels: [], total: 0, linked_count: 0 },
      });
    } else if (url.pathname.endsWith("/sources")) {
      await route.fulfill({
        json: {
          sources: [
            {
              domain: story.domain,
              name: "Hacker News",
              count: 2,
              kind: "hacker_news",
              tags: [],
            },
            {
              domain: original.domain,
              name: original.domain,
              count: 1,
              kind:
                original.source === "linked_article" ? "rss" : original.source,
              tags: [],
            },
          ],
          total_posts: 3,
        },
      });
    } else if (url.pathname.endsWith("/connections")) {
      const discussion = url.searchParams.get("domain") === story.domain;
      await route.fulfill({
        json: {
          root: reference(discussion ? story : original),
          incoming: discussion ? [] : incoming,
          outgoing: discussion
            ? [{ ...incoming[0], direction: "outgoing" }]
            : [],
          incoming_count: discussion ? 0 : incoming.length,
          outgoing_count: discussion ? 1 : 0,
          limit: 100,
          coverage: "saved_evidence",
        },
      });
    } else if (url.pathname.endsWith("/post")) {
      await route.fulfill({
        json:
          url.searchParams.get("domain") === story.domain ? story : original,
      });
    } else {
      await route.fulfill({
        json: {
          posts:
            url.searchParams.get("kind") === "hacker_news"
              ? [story]
              : [original],
          next_cursor: null,
          limited: false,
        },
      });
    }
  });
  return { original, story };
}

for (const kind of ["blog", "x", "reddit"] as const) {
  for (const width of [390, 1280]) {
    test(`saved ${kind} destination and incoming discussions work at ${width}px`, async ({
      page,
    }, testInfo) => {
      await page.setViewportSize({ width, height: 844 });
      const { original, story } = await fixtures(page, kind);
      await page.goto("/reader/?view=hacker_news&hn_filter=top20");
      const title = page.getByRole("link", {
        name: `Read ${story.title}`,
        exact: true,
      });
      await expect(title).toHaveAttribute(
        "href",
        new RegExp(`article_source=${original.domain}`),
      );
      await title.click();
      await expect(page).toHaveURL(/view=hacker_news&hn_filter=top20/);
      await expect(page).toHaveURL(
        new RegExp(`article_source=${original.domain}`),
      );
      const article = page.getByRole("main", {
        name: kind === "x" ? "X post reader" : "Article reader",
        exact: true,
      });
      await expect(
        article.getByText(original.excerpt, { exact: true }),
      ).toBeVisible();
      await page.goBack();
      await expect(page.locator(".hn-card")).toHaveClass(/is-read/);
      await title.click();
      await expect(
        article.getByText(original.excerpt, { exact: true }),
      ).toBeVisible();
      const connections = article.locator(".post-connections");
      const toggle = connections.getByRole("button", {
        name: "Connections",
        exact: true,
      });
      await toggle.focus();
      await page.keyboard.press("Enter");
      await expect(toggle).toHaveAttribute("aria-expanded", "true");
      const incoming = connections.getByRole("region", {
        name: "Links to this post",
      });
      await expect(incoming.locator(".post-connection-evidence")).toHaveCount(
        2,
      );
      const backlink = incoming
        .locator(".post-connection-evidence")
        .filter({
          has: page.getByRole("heading", { name: story.title, exact: true }),
        })
        .getByRole("link", { name: "Open link in DANK", exact: true });
      await expect(backlink).toHaveAttribute(
        "href",
        /article=1&article_source=news.ycombinator.com/,
      );
      await expect(backlink).toHaveAttribute("href", /reference_kind=post/);
      await expect(backlink).toHaveAttribute("href", /connections_from=/);
      await backlink.click();
      await expect(page.locator('[data-reference-match="true"]')).toHaveText(
        "Read original article",
      );
      await expect(page.locator('[data-reference-match="true"]')).toBeFocused();
      await expect(
        page.getByText("A captured Hacker News reply."),
      ).toBeVisible();
      await page
        .getByRole("link", { name: "Read collected article", exact: true })
        .click();
      await expect(page).toHaveURL(
        new RegExp(`article_source=${original.domain}`),
      );
      await page.goBack();
      await expect(
        page.getByText("A captured Hacker News reply."),
      ).toBeVisible();
      await page
        .getByRole("button", { name: "Back to connections", exact: true })
        .click();
      await expect(page).toHaveURL(
        new RegExp(`article_source=${original.domain}`),
      );
      await expect(incoming).toBeVisible();
      await expect(toggle).toHaveAttribute("aria-expanded", "true");
      await page
        .getByRole("button", { name: "Back to stories", exact: true })
        .click();
      await expect(page).toHaveURL(/view=hacker_news&hn_filter=top20$/);
      await page
        .getByRole("link", {
          name: `Discussion on Hacker News: ${story.title}`,
          exact: true,
        })
        .click();
      await expect(page).toHaveURL(
        /article=1&article_source=news.ycombinator.com/,
      );
      await expect(
        page.getByText("A captured Hacker News reply."),
      ).toBeVisible();
      expect(new URL(page.url()).searchParams.has("reference_kind")).toBe(
        false,
      );
      expect(new URL(page.url()).searchParams.has("connections_from")).toBe(
        false,
      );
      await expect(page.locator('[data-reference-match="true"]')).toHaveCount(
        0,
      );
      await page.goto("/reader/");
      await expect(page.locator(".post-card")).toHaveCount(1);
      await expect(page.locator(".hn-card")).toHaveCount(0);
      await page.locator(".post-card .post-links summary").click();
      await expect(page.locator(".post-card .post-links a")).toHaveCount(2);
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
      await page.screenshot({
        path: testInfo.outputPath(`linked-${kind}-${width}.png`),
        fullPage: true,
      });
    });
  }
}

for (const publishedAt of [null, "2026-10-14T12:30:00Z", undefined]) {
  test(`linked article publication date is explicit when ${String(publishedAt)}`, async ({
    page,
  }) => {
    const { original } = await fixtures(page, "blog", {
      source: "linked_article",
      published_at: publishedAt,
    });
    await page.goto("/reader/");
    const card = page.locator(".post-card");
    await expect(card).toBeVisible();
    if (publishedAt === null) {
      await expect(
        card.getByText("Publication date unavailable", { exact: true }),
      ).toBeVisible();
      await expect(card.locator(".post-meta time")).toHaveCount(0);
    } else {
      await expect(card.locator(".post-meta time")).toHaveAttribute(
        "datetime",
        publishedAt ?? original.created_at,
      );
    }
    await card
      .getByRole("link", { name: `Read ${original.title}`, exact: true })
      .click();
    const metadata = page.locator(".article-inner > .post-meta");
    if (publishedAt === null) {
      await expect(
        metadata.getByText("Publication date unavailable", { exact: true }),
      ).toBeVisible();
      await expect(metadata.locator("time")).toHaveCount(0);
    } else {
      await expect(metadata.locator("time")).toHaveAttribute(
        "datetime",
        publishedAt ?? original.created_at,
      );
    }
  });
}
