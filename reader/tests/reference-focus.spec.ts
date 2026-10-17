import { expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";
import type {
  YouTubeItem,
  YouTubeReference,
} from "../src/lib/youtubeCollection";

const videoId = "NnV_cWeoo5Q";
const videoUrl = `https://www.youtube.com/watch?v=${videoId}`;
const saved = "2026-10-17T12:00:00Z";
const matchingHtml = `<p>Other video: <a href="https://youtu.be/aaaaaaaaaaa">also relevant</a>. We have <a href="${videoUrl}&t=1468">seen it happen in real time</a>.</p>`;
const filler = Array.from(
  { length: 14 },
  (_, i) =>
    `<p>Discussion context ${i}. ${"The source context stays intact. ".repeat(5)}</p>`,
).join("");

function article(
  source: "reddit" | "hacker_news" | "rss" | "x" = "reddit",
): Post {
  const base: Post = {
    domain:
      source === "reddit"
        ? "reddit.com"
        : source === "hacker_news"
          ? "news.ycombinator.com"
          : source === "x"
            ? "x.com"
            : "blog.example",
    id: "source-post",
    url: "https://example.com/post",
    title: "A collected discussion",
    author: "Writer",
    source,
    html: filler,
    excerpt: "Discussion",
    created_at: saved,
    thumbnail: null,
    media: [],
  };

  if (source === "reddit")
    base.reddit = {
      community: "gamedev",
      score: 30,
      comments: 2,
      flair: null,
      comment_capture: {
        status: "captured",
        sort: "best",
        limit: 2,
        total_available: 2,
        has_more: false,
        comments: [
          {
            id: "t1_other",
            parent_id: "source-post",
            author: "Other",
            body: "",
            body_html: matchingHtml,
            score: 1,
            created_utc: null,
            permalink: "https://reddit.com/other",
            is_op: false,
            depth: 0,
          },
          {
            id: "t1_target",
            parent_id: "source-post",
            author: "James20k",
            body: "",
            body_html: matchingHtml,
            score: 2,
            created_utc: null,
            permalink: "https://reddit.com/target",
            is_op: false,
            depth: 1,
          },
        ],
      },
    };
  if (source === "hacker_news")
    base.hacker_news = {
      points: 20,
      comments: 2,
      discussion_url: base.url,
      external_url: null,
      day: "2026-10-17",
      rank: 1,
      comment_capture: {
        status: "captured",
        captured: 2,
        total: 2,
        reason: null,
      },
      comments_items: [
        {
          id: "50133640",
          parent_id: base.id,
          author: "Other",
          html: matchingHtml,
          created_at: saved,
          depth: 0,
        },
        {
          id: "50133642",
          parent_id: base.id,
          author: "zdw",
          html: matchingHtml,
          created_at: saved,
          depth: 1,
        },
      ],
    };
  if (source === "rss")
    base.html += `<div id="comment-other">${matchingHtml}</div><div id="comment-target">${matchingHtml}</div>`;
  if (source === "x") {
    base.html = `<p>${videoUrl}</p>${filler}`;
    base.x = {
      handle: "reporter",
      name: "Reporter",
      links: [
        {
          url: videoUrl,
          title: "Security talk",
          description: "A source video",
          image_url: null,
        },
      ],
    };
  }

  return base;
}
function reference(post: Post): YouTubeReference {
  return {
    domain: post.domain,
    post_id: post.id,
    url: post.url,
    title: post.title,
    source: post.source,
    reader_url: "/reader/",
    kind: post.source === "x" ? "post" : "comment",
    reference_id:
      post.source === "reddit"
        ? "t1_target"
        : post.source === "hacker_news"
          ? "50133642"
          : post.source === "x"
            ? ""
            : "comment-target",
    reference_url: "https://example.com/original-comment",
    author: post.source === "reddit" ? "James20k" : "reader",
    matched_url: `${videoUrl}&t=1468`,
    excerpt: "We have seen it happen in real time.",
    observed_at: saved,
  };
}
async function fixtures(page: Page, post = article(), linkedPost?: Post) {
  const ref = reference(post);
  const video: YouTubeItem = {
    id: `video:${videoId}`,
    kind: "video",
    video_id: videoId,
    url: videoUrl,
    title: "Security in the LLM age",
    text: "",
    channel: { id: "", name: "", url: "" },
    thumbnail_url: null,
    duration_text: "",
    views_text: "",
    published_text: "",
    observed_at: saved,
    detail_capture: { status: "disabled" },
    discoveries: [],
    references: [ref],
    reference_count: 1,
  };
  await page.route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/youtube/sources"))
      return route.fulfill({
        json: { feeds: [], channels: [], total: 1, linked_count: 1 },
      });
    if (url.pathname.endsWith("/youtube"))
      return route.fulfill({
        json: { items: [video], next_cursor: null, total: 1 },
      });
    if (url.pathname.endsWith("/sources"))
      return route.fulfill({ json: { sources: [], total_posts: 1 } });
    if (url.pathname.endsWith("/post"))
      return route.fulfill({
        json:
          linkedPost &&
          url.searchParams.get("id") === linkedPost.id &&
          url.searchParams.get("domain") === linkedPost.domain
            ? linkedPost
            : post,
      });
    return route.fulfill({
      json: { posts: [], next_cursor: null, limited: false },
    });
  });
  return ref;
}
const detailUrl = `/reader/?view=youtube&feed=linked&q=security&youtube_item=video%3A${videoId}`;

test("collected originals and incoming links clear the previous video target", async ({
  page,
}) => {
  const original = {
    ...article("rss"),
    id: "collected-original",
    title: "Collected original",
    html: "<p>The original article remains independent of the discussion.</p>",
  };
  const story = article("hacker_news");
  const { id, domain, url, title, source } = original;
  story.linked_post = { id, domain, url, title, source };
  story.links = [story.linked_post];
  story.links_count = 1;
  if (story.hacker_news) story.hacker_news.external_url = original.url;
  await fixtures(page, story, original);
  await page.goto(detailUrl);
  await page
    .getByRole("link", { name: "Open comment: reader", exact: true })
    .click();
  await expect(page.locator('[data-reference-match="true"]')).toHaveText(
    "seen it happen in real time",
  );
  const collected = page.getByRole("link", {
    name: "Read collected article",
    exact: true,
  });
  const destinations = [
    await collected.getAttribute("href"),
    ...(await page
      .locator(".post-links a")
      .evaluateAll((links) =>
        links.map((link) => (link as HTMLAnchorElement).href),
      )),
  ];
  for (const destination of destinations) {
    const params = new URL(destination ?? "", "http://localhost").searchParams;
    expect([...params.keys()].some((key) => key.startsWith("reference_"))).toBe(
      false,
    );
  }
  await collected.click();
  await expect(
    page.getByRole("heading", { name: "Collected original", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("complementary", { name: "Video reference" }),
  ).toHaveCount(0);
  await expect(page.locator('[data-reference-match="true"]')).toHaveCount(0);
  await page.goBack();
  await expect(page.locator('[data-reference-match="true"]')).toHaveText(
    "seen it happen in real time",
  );
  expect(new URL(page.url()).searchParams.get("reference_id")).toBe("50133642");
});

for (const width of [1210, 390]) {
  test(`graph view keeps position and opens exact DANK evidence at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 958 });
    const reference = await fixtures(page);
    await page.goto(detailUrl);
    const graphButton = page.getByRole("button", {
      name: "Graph view",
      exact: true,
    });
    await graphButton.scrollIntoViewIfNeeded();
    const before = await page.evaluate(() => window.scrollY);
    await graphButton.click();
    await expect(page.locator(".relationship-graph")).toHaveAttribute(
      "data-status",
      "ready",
    );
    expect(
      Math.abs((await page.evaluate(() => window.scrollY)) - before),
    ).toBeLessThan(60);
    expect(new URL(page.url()).searchParams.get("references")).toBe("graph");
    await page
      .getByLabel("Explore nodes")
      .selectOption(
        JSON.stringify([
          "comment",
          reference.domain,
          reference.post_id,
          reference.reference_id,
        ]),
      );
    const details = page.getByRole("region", { name: "Selected connection" });
    await details.getByRole("link", { name: "Open comment in reader" }).click();
    const match = page.locator('[data-reference-match="true"]');
    await expect(match).toHaveText("seen it happen in real time");
    await expect(match).toBeFocused();
    await expect(match).toBeInViewport();
    await page.goBack();
    await expect(graphButton).toHaveAttribute("aria-pressed", "true");
    await expect(page.locator(".relationship-graph")).toHaveAttribute(
      "data-status",
      "ready",
    );
    await page.reload();
    await expect(graphButton).toHaveAttribute("aria-pressed", "true");
    await page.getByRole("button", { name: "List view", exact: true }).click();
    await expect(page.locator(".relationship-graph")).toHaveCount(0);
    await expect(
      page.getByRole("link", { name: "Open comment: James20k", exact: true }),
    ).toBeVisible();
  });
}

for (const width of [1440, 390]) {
  test(`reference opens and highlights the exact saved Reddit link at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.addInitScript(() =>
      localStorage.setItem(
        "dank-reader:preferences:v1",
        JSON.stringify({ showRedditComments: false }),
      ),
    );
    await fixtures(page);
    await page.goto(detailUrl);
    const open = page.getByRole("link", {
      name: "Open comment: James20k",
      exact: true,
    });
    await expect(open).toHaveAttribute("href", /reference_id=t1_target/);
    await expect(
      page.getByRole("link", { name: "Original comment: James20k" }),
    ).toHaveAttribute("target", "_blank");
    await open.click();
    const match = page.locator('[data-reference-match="true"]');
    await expect(match).toHaveCount(1);
    await expect(match).toHaveText("seen it happen in real time");
    await expect(match).toBeFocused();
    await expect(match).toBeInViewport();
    await expect(
      page.locator('[data-reference-comment="t1_other"] .reference-match'),
    ).toHaveCount(0);
    expect(
      await page.evaluate(
        () =>
          JSON.parse(localStorage.getItem("dank-reader:preferences:v1") || "{}")
            .showRedditComments,
      ),
    ).toBe(false);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    const directUrl = page.url();
    await page.goBack();
    await expect(page).toHaveURL(
      new RegExp(`youtube_item=video%3A${videoId}$`),
    );
    await expect(open).toBeInViewport();
    await page.goForward();
    await expect(match).toBeInViewport();
    await expect(page).toHaveURL(directUrl);
    await page.reload();
    await expect(match).toBeFocused();
    await expect(match).toBeInViewport();
    await page
      .getByRole("button", { name: "Back to stories", exact: true })
      .click();
    expect(new URL(page.url()).searchParams.has("reference_id")).toBe(false);
    expect(new URL(page.url()).searchParams.get("q")).toBe("security");
  });
}
for (const source of ["hacker_news", "rss"] as const) {
  test(`${source} focuses only the captured comment identity`, async ({
    page,
  }) => {
    await fixtures(page, article(source));
    await page.goto(detailUrl);
    await page
      .getByRole("link", { name: "Open comment: reader", exact: true })
      .click();
    const match = page.locator('[data-reference-match="true"]');
    await expect(match).toHaveCount(1);
    await expect(match).toHaveText("seen it happen in real time");
    await expect(match).toBeFocused();
    await expect(match).toBeInViewport();
  });
}
test("missing historical comment does not highlight another comment and exposes archived evidence", async ({
  page,
}) => {
  const post = article();
  if (post.reddit?.comment_capture) post.reddit.comment_capture.comments.pop();
  await fixtures(page, post);
  await page.goto(detailUrl);
  await page
    .getByRole("link", { name: "Open comment: James20k", exact: true })
    .click();
  await expect(page.locator('[data-reference-match="true"]')).toHaveCount(0);
  const notice = page.getByRole("complementary", { name: "Video reference" });
  await expect(notice).toContainText("not available in the current saved post");
  await expect(notice).toContainText("We have seen it happen in real time.");
  await expect(notice).toBeFocused();
  await expect(
    notice.getByRole("link", { name: "Original comment", exact: true }),
  ).toHaveAttribute("href", "https://example.com/original-comment");
});
test("untrusted content cannot forge a reference match or execute markup", async ({
  page,
}) => {
  const post = article();
  const comment = post.reddit?.comment_capture?.comments[1];
  if (comment)
    comment.body_html = `<p data-reference-match="true">Spoofed</p><a href="https://youtube.com.evil.test/watch?v=${videoId}">lookalike</a><img src="bad" onerror="window.compromised=true"><script>window.compromised=true</script>`;
  await fixtures(page, post);
  await page.goto(detailUrl);
  await page
    .getByRole("link", { name: "Open comment: James20k", exact: true })
    .click();
  await expect(page.locator('[data-reference-match="true"]')).toHaveCount(0);
  await expect(
    page.getByRole("complementary", { name: "Video reference" }),
  ).toContainText("not available");
  expect(
    await page.evaluate(() => Reflect.get(window, "compromised")),
  ).toBeUndefined();
});

test("a video preview retains an exact visible post match without changing media preferences", async ({
  page,
}) => {
  const post = article("x");
  await fixtures(page, post);
  await page.goto(detailUrl);
  await page
    .getByRole("link", { name: "Open post: reader", exact: true })
    .click();
  await expect(
    page.locator('.x-focused-post .article-body [data-reference-match="true"]'),
  ).toHaveText(videoUrl);
  await expect(
    page.locator('.x-youtube-preview [data-reference-match="true"]'),
  ).toHaveText("Security talk");
  await expect(
    page.locator('.x-focused-post .article-body [data-reference-match="true"]'),
  ).toBeFocused();
});

test("a directly linked HN video highlights the original-article destination", async ({
  page,
}) => {
  const post = article("hacker_news");
  if (post.hacker_news) post.hacker_news.external_url = videoUrl;
  const ref = await fixtures(page, post);
  const params = new URLSearchParams({
    view: "youtube",
    feed: "linked",
    youtube_item: `video:${videoId}`,
    article: post.id,
    article_source: post.domain,
    reference_kind: "post",
    reference_video: videoId,
    reference_saved: ref.observed_at,
  });
  await page.goto(`/reader/?${params}`);
  const match = page.locator('[data-reference-match="true"]');
  await expect(match).toHaveCount(1);
  await expect(match).toHaveText("Read original article");
  await expect(match).toBeFocused();
});

test("plain-text captured comments preserve line breaks and highlight only the exact URL", async ({
  page,
}) => {
  const post = article();
  const comment = post.reddit?.comment_capture?.comments[1];
  if (comment) {
    comment.body_html = "";
    comment.body = `First paragraph\n\nSee ${videoUrl}&t=1468\n<not markup>`;
  }
  await fixtures(page, post);
  await page.goto(detailUrl);
  await page
    .getByRole("link", { name: "Open comment: James20k", exact: true })
    .click();
  const body = page.locator(
    '[data-reference-comment="t1_target"] .article-body',
  );
  await expect(body.locator("br")).toHaveCount(3);
  await expect(body.locator('[data-reference-match="true"]')).toHaveText(
    `${videoUrl}&t=1468`,
  );
  await expect(body).toContainText("<not markup>");
});
