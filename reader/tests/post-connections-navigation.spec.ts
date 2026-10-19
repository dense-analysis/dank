import { expect, type Page, test } from "@playwright/test";
import type { Core } from "cytoscape";
import {
  connectionDiscussion,
  connectionRoot,
  connectionsFixture,
  incomingConnection,
  outgoingConnection,
  videoConnection,
} from "../src/lib/postConnections.fixture";
import type { Post, PostReference } from "../src/lib/types";
import type { YouTubeItem } from "../src/lib/youtubeCollection";

const saved = "2026-10-17T12:00:00Z";
const rootQuery = new URLSearchParams({
  view: "stories",
  q: "kernel",
  sort: "oldest",
  article: connectionRoot.id,
  article_source: connectionRoot.domain,
});
const rootUrl = `/reader/?${rootQuery}`;
const commentNode = JSON.stringify([
  "comment",
  connectionDiscussion.domain,
  connectionDiscussion.id,
  incomingConnection.evidence.reference_id,
]);

function postFrom(reference: PostReference, html: string): Post {
  return {
    ...reference,
    html,
    author: "Writer",
    excerpt: reference.title,
    created_at: saved,
    thumbnail: null,
    media: [],
  };
}

async function fixtures(page: Page, missingComment = false) {
  const data = structuredClone(connectionsFixture);
  const original = postFrom(
    connectionRoot,
    `<p>A collected kernel article with <a href="${outgoingConnection.evidence.matched_url}">the original research</a> and <a href="${videoConnection.evidence.matched_url}">the conference talk</a>.</p>`,
  );
  const neighbor = postFrom(
    outgoingConnection.target.post as PostReference,
    "<p>The collected research source.</p>",
  );
  const source = postFrom(connectionDiscussion, "<p>The saved discussion.</p>");
  source.reddit = {
    community: "kernel",
    score: 12,
    comments: 2,
    flair: null,
    comment_capture: {
      status: "captured",
      sort: "best",
      limit: 20,
      total_available: 2,
      has_more: false,
      comments: [
        {
          id: "unrelated-comment",
          parent_id: source.id,
          author: "OtherReader",
          body: "",
          body_html: `<p><a href="${incomingConnection.evidence.matched_url}">Same destination in another comment</a></p>`,
          score: 1,
          created_utc: null,
          permalink: `${source.url}unrelated-comment/`,
          is_op: false,
          depth: 0,
        },
        ...(missingComment
          ? []
          : [
              {
                id: incomingConnection.evidence.reference_id,
                parent_id: source.id,
                author: incomingConnection.evidence.author,
                body: "",
                body_html: `<p>This source explains <a href="${incomingConnection.evidence.matched_url}">why kernel security matters</a>.</p>`,
                score: 4,
                created_utc: null,
                permalink: incomingConnection.evidence.reference_url,
                is_op: false,
                depth: 1,
              },
            ]),
      ],
    },
  };
  const video: YouTubeItem = {
    id: videoConnection.target.youtube_item_id ?? "",
    kind: "video",
    video_id: "NnV_cWeoo5Q",
    url: videoConnection.target.url,
    title: videoConnection.target.title ?? "",
    text: "",
    channel: { id: "", name: "Kernel Recipes", url: "" },
    thumbnail_url: null,
    duration_text: "",
    views_text: "",
    published_text: "",
    observed_at: saved,
    detail_capture: { status: "disabled" },
    discoveries: [],
    references: [],
    reference_count: 0,
  };
  const posts = [original, source, neighbor];
  const requests: string[] = [];
  await page.context().route("**/api/reader/**", async (route) => {
    const url = new URL(route.request().url());
    requests.push(url.pathname);
    if (url.pathname.endsWith("/youtube/sources"))
      return route.fulfill({
        json: { feeds: [], channels: [], total: 1, linked_count: 1 },
      });
    if (url.pathname.endsWith("/youtube"))
      return route.fulfill({
        json: { items: [video], total: 1, next_cursor: null },
      });
    if (url.pathname.endsWith("/sources"))
      return route.fulfill({
        json: { sources: [], total_posts: posts.length },
      });
    if (url.pathname.endsWith("/post")) {
      const found = posts.find(
        (post) =>
          post.domain === url.searchParams.get("domain") &&
          post.id === url.searchParams.get("id"),
      );
      return route.fulfill(
        found
          ? { json: found }
          : { status: 404, json: { error: "Article not found" } },
      );
    }
    if (url.pathname.endsWith("/connections")) {
      const id = url.searchParams.get("id");
      if (id === connectionRoot.id) return route.fulfill({ json: data });
      const root = id === neighbor.id ? neighbor : source;
      return route.fulfill({
        json: {
          root,
          incoming: [],
          outgoing: [],
          incoming_count: 0,
          outgoing_count: 0,
          limit: 100,
          coverage: "saved_evidence",
        },
      });
    }
    if (url.pathname.endsWith("/connection"))
      return route.fulfill({ json: incomingConnection });
    return route.fulfill({
      json: { posts: [original], next_cursor: null, limited: false },
    });
  });
  return { requests, original, source, neighbor };
}

const graph = (page: Page) =>
  page.locator(".post-connections .relationship-graph");
const selected = (page: Page) =>
  page.getByRole("region", { name: "Selected connection" });
const renderer = (page: Page) =>
  page
    .locator(".post-connections .relationship-graph-canvas")
    .evaluateHandle(
      (element) =>
        (element as HTMLDivElement & { _cyreg: { cy: Core } })._cyreg.cy,
    );

for (const width of [1210, 390]) {
  test(`ordinary article connections keep evidence, direction and return state at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 958 });
    const { requests } = await fixtures(page);
    await page.goto(rootUrl);
    const connections = page.getByRole("region", {
      name: "Connections",
      exact: true,
    });
    await expect(
      connections.getByRole("button", { name: "Connections", exact: true }),
    ).toHaveAttribute("aria-expanded", "false");
    expect(
      requests.filter((path) => path.endsWith("/connections")),
    ).toHaveLength(0);
    await connections
      .getByRole("button", { name: "Connections", exact: true })
      .click();
    await expect(
      page.getByRole("region", { name: "Links to this post" }),
    ).toContainText("KernelReader");
    await expect(
      page.getByRole("region", { name: "Links from this post" }),
    ).toContainText("The original security research");
    await expect(connections).toContainText(
      "1 incoming · 3 outgoing references",
    );
    await connections
      .getByRole("button", { name: "Graph view", exact: true })
      .click();
    await expect(graph(page)).toHaveAttribute("data-status", "ready");
    await connections
      .getByRole("button", { name: "Incoming links", exact: true })
      .click();
    expect(new URL(page.url()).searchParams.get("connections_direction")).toBe(
      "incoming",
    );
    await page.getByLabel("Explore nodes").selectOption(commentNode);
    await expect(selected(page)).toContainText(
      "This source explains why kernel security matters.",
    );
    await page.evaluate(() => document.fonts.ready);
    const before = await renderer(page);
    await before.evaluate((cy) => {
      cy.zoom(1.1);
      cy.pan({ x: 90, y: 120 });
    });
    const position = await before.evaluate((cy) => ({
      zoom: cy.zoom(),
      pan: cy.pan(),
    }));
    const originUrl = page.url();
    const evidenceLink = selected(page).getByRole("link", {
      name: "Open comment in DANK",
      exact: true,
    });
    const href = new URL(
      (await evidenceLink.getAttribute("href")) ?? "",
      page.url(),
    );
    expect(href.searchParams.get("reference_url")).toBe(
      incomingConnection.evidence.matched_url,
    );
    expect(href.searchParams.get("connections_from")).toBe(
      new URL(originUrl).search.slice(1),
    );
    await evidenceLink.click();
    const match = page.locator('[data-reference-match="true"]');
    await expect(match).toHaveCount(1);
    await expect(match).toHaveText("why kernel security matters");
    await expect(match).toBeFocused();
    await expect(match).toBeInViewport();
    await expect(graph(page)).toHaveCount(0);
    await page
      .getByRole("button", { name: "Back to connections", exact: true })
      .first()
      .click();
    await expect(page).toHaveURL(originUrl);
    await expect(graph(page)).toHaveAttribute("data-status", "ready");
    await expect(page.getByLabel("Explore nodes")).toHaveValue(commentNode);
    const restored = await renderer(page);
    expect(
      await restored.evaluate((cy) => ({ zoom: cy.zoom(), pan: cy.pan() })),
    ).toEqual(position);
    await selected(page)
      .getByRole("link", { name: "Open comment in DANK", exact: true })
      .click();
    await expect(match).toBeFocused();
    await page.goBack();
    await expect(graph(page)).toHaveAttribute("data-status", "ready");
    await expect(page.getByLabel("Explore nodes")).toHaveValue(commentNode);
    await page.reload();
    await expect(
      connections.getByRole("button", { name: "Incoming links", exact: true }),
    ).toHaveAttribute("aria-pressed", "true");
    await connections
      .getByRole("button", { name: "Outgoing links", exact: true })
      .click();
    await expect(
      page
        .getByLabel("Explore nodes")
        .getByRole("option", { name: /KernelReader/ }),
    ).toHaveCount(0);
    await expect(selected(page)).toContainText("Read the original research");
    await connections
      .getByRole("button", { name: "List view", exact: true })
      .click();
    await expect(graph(page)).toHaveCount(0);
    await expect(
      page.getByRole("region", { name: "Links to this post" }),
    ).toBeVisible();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await before.dispose();
    await restored.dispose();
  });
}

test("outgoing evidence focuses the current article and normal exploration opens a neighbor graph", async ({
  page,
}) => {
  const { neighbor } = await fixtures(page);
  await page.goto(`${rootUrl}&connections=list`);
  const outgoing = page
    .getByRole("region", { name: "Links from this post" })
    .locator(".post-connection-evidence")
    .filter({
      has: page.getByRole("heading", { name: neighbor.title, exact: true }),
    });
  await outgoing
    .getByRole("link", { name: "Open link in DANK", exact: true })
    .click();
  await expect(page.locator('[data-reference-match="true"]')).toHaveText(
    "the original research",
  );
  await expect(page.locator('[data-reference-match="true"]')).toBeFocused();
  await expect(page.locator(".post-connections-toggle")).toHaveAttribute(
    "aria-expanded",
    "false",
  );
  await page
    .getByRole("button", { name: "Back to connections", exact: true })
    .first()
    .click();
  await outgoing
    .getByRole("link", { name: "Explore this post", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: neighbor.title, exact: true }),
  ).toBeVisible();
  expect(new URL(page.url()).searchParams.get("connections")).toBe("graph");
  expect(
    [...new URL(page.url()).searchParams.keys()].some((key) =>
      key.startsWith("reference_"),
    ),
  ).toBe(false);
  await expect(page.locator(".post-connections")).toContainText(
    "No verified links found",
  );
  await page.goBack();
  await expect(outgoing).toBeVisible();
});

test("metadata-only YouTube destinations open their existing DANK details", async ({
  page,
}) => {
  await fixtures(page);
  await page.goto(`${rootUrl}&connections=graph`);
  await expect(graph(page)).toHaveAttribute("data-status", "ready");
  await page
    .getByLabel("Explore nodes")
    .selectOption(
      JSON.stringify(["video", videoConnection.target.youtube_item_id]),
    );
  await selected(page)
    .getByRole("link", { name: "Explore video in DANK", exact: true })
    .first()
    .click();
  expect(new URL(page.url()).searchParams.get("youtube_item")).toBe(
    videoConnection.target.youtube_item_id,
  );
  expect(new URL(page.url()).searchParams.has("article")).toBe(false);
  await expect(page.locator(".yt-detail")).toContainText(
    videoConnection.target.title ?? "",
  );
  await page.goBack();
  await expect(graph(page)).toHaveAttribute("data-status", "ready");
  await expect(page.getByLabel("Explore nodes")).toHaveValue(
    JSON.stringify(["video", videoConnection.target.youtube_item_id]),
  );
});

test("modified evidence clicks retain return context in a separate reader tab and reload", async ({
  page,
}) => {
  await fixtures(page);
  await page.goto(`${rootUrl}&connections=list`);
  const incoming = page.getByRole("region", { name: "Links to this post" });
  const origin = page.url();
  const popupPromise = page.context().waitForEvent("page");
  await incoming
    .getByRole("link", { name: "Open comment in DANK", exact: true })
    .click({ modifiers: ["ControlOrMeta"] });
  const popup = await popupPromise;
  await expect(page).toHaveURL(origin);
  await expect(popup.locator('[data-reference-match="true"]')).toHaveText(
    "why kernel security matters",
  );
  await popup.reload();
  await expect(popup.locator('[data-reference-match="true"]')).toBeFocused();
  await popup
    .getByRole("button", { name: "Back to connections", exact: true })
    .first()
    .click();
  await expect(popup).toHaveURL(origin);
  await expect(
    popup.getByRole("region", { name: "Links to this post" }),
  ).toBeVisible();
  await popup.close();
});

test("missing historical comment keeps its archived evidence and returns to the source graph after reload", async ({
  page,
}) => {
  await fixtures(page, true);
  await page.goto(`${rootUrl}&connections=graph`);
  await expect(graph(page)).toHaveAttribute("data-status", "ready");
  await page.getByLabel("Explore nodes").selectOption(commentNode);
  const origin = page.url();
  await selected(page)
    .getByRole("link", { name: "Open comment in DANK", exact: true })
    .click();
  await expect(page.locator('[data-reference-match="true"]')).toHaveCount(0);
  const notice = page.getByRole("complementary", { name: "Link reference" });
  await expect(notice).toContainText(incomingConnection.evidence.excerpt);
  await expect(
    notice.getByRole("link", { name: "Original comment" }),
  ).toHaveAttribute("href", incomingConnection.evidence.reference_url);
  await page.reload();
  await expect(notice).toContainText(incomingConnection.evidence.excerpt);
  await expect(notice).toBeFocused();
  await page
    .getByRole("button", { name: "Back to connections", exact: true })
    .first()
    .click();
  await expect(page).toHaveURL(origin);
  await expect(graph(page)).toHaveAttribute("data-status", "ready");
});

test("exploring a previously read article waits for its graph and opens Connections instead of its saved paragraph", async ({
  page,
}) => {
  const { neighbor } = await fixtures(page);
  neighbor.html = Array.from(
    { length: 70 },
    (_, index) =>
      `<p>Research paragraph ${index}. ${"Detailed supporting research stays available in this article. ".repeat(8)}</p>`,
  ).join("");
  await page.addInitScript(({ domain, id }) => {
    localStorage.setItem(
      "dank-reader:positions:v1",
      JSON.stringify([
        [JSON.stringify([domain, id]), { scroll: 6000, block: 35, offset: 20 }],
      ]),
    );
  }, neighbor);
  let releaseConnections = () => {};
  const pendingConnections = new Promise<void>((resolve) => {
    releaseConnections = resolve;
  });
  await page.route("**/api/reader/connections?**", async (route) => {
    if (new URL(route.request().url()).searchParams.get("id") !== neighbor.id)
      return route.fallback();
    await pendingConnections;
    return route.fulfill({
      json: {
        root: neighbor,
        incoming: [{ ...outgoingConnection, direction: "incoming" }],
        outgoing: [],
        incoming_count: 1,
        outgoing_count: 0,
        limit: 100,
        coverage: "saved_evidence",
      },
    });
  });
  await page.goto(`${rootUrl}&connections=list`);
  const neighborCard = page
    .getByRole("region", { name: "Links from this post" })
    .locator(".post-connection-evidence")
    .filter({
      has: page.getByRole("heading", { name: neighbor.title, exact: true }),
    });
  await neighborCard
    .getByRole("link", { name: "Explore this post", exact: true })
    .click();
  await expect(
    page.locator(".post-connections").getByRole("status"),
  ).toHaveText("Loading connections…");
  await expect(page.locator(".article-body p").nth(35)).not.toBeInViewport();
  releaseConnections();
  await expect(graph(page)).toHaveAttribute("data-status", "ready");
  await expect(page.locator(".post-connections-toggle")).toBeInViewport();
  await expect
    .poll(() =>
      page.evaluate(
        () => !!document.activeElement?.closest(".post-connections"),
      ),
    )
    .toBe(true);
  await expect(page.locator(".article-body p").nth(35)).not.toBeInViewport();
});

test("a post with no collected body still opens and focuses its connections graph", async ({
  page,
}) => {
  const { original } = await fixtures(page);
  original.html = "";
  await page.goto(`${rootUrl}&connections=graph`);
  await expect(graph(page)).toHaveAttribute("data-status", "ready");
  await expect(page.locator(".post-connections-toggle")).toBeInViewport();
  await expect
    .poll(() =>
      page.evaluate(
        () => !!document.activeElement?.closest(".post-connections"),
      ),
    )
    .toBe(true);
  await expect(
    page.getByRole("heading", { name: "Full text not collected", exact: true }),
  ).toBeAttached();
  await page.getByLabel("Explore nodes").selectOption(commentNode);
  const origin = page.url();
  await selected(page)
    .getByRole("link", { name: "Open comment in DANK", exact: true })
    .click();
  await expect(page.locator('[data-reference-match="true"]')).toHaveText(
    "why kernel security matters",
  );
  await page
    .getByRole("button", { name: "Back to connections", exact: true })
    .first()
    .click();
  await expect(page).toHaveURL(origin);
  await expect(graph(page)).toHaveAttribute("data-status", "ready");
  await expect
    .poll(() =>
      page.evaluate(
        () => !!document.activeElement?.closest(".post-connections"),
      ),
    )
    .toBe(true);
});

test("publisher exploration retains Reddit community context while All requests stay broad", async ({
  page,
}) => {
  const { requests, neighbor } = await fixtures(page);
  const params = new URLSearchParams(rootQuery);
  params.set("view", "reddit");
  params.set("community", "kernel");
  params.set("connections", "list");
  await page.goto(`/reader/?${params}`);
  const outgoing = page
    .getByRole("region", { name: "Links from this post" })
    .locator(".post-connection-evidence")
    .filter({
      has: page.getByRole("heading", { name: neighbor.title, exact: true }),
    });
  await expect(outgoing).toBeVisible();
  expect(
    requests.some(
      (url) =>
        url.includes("/connections?") &&
        url.includes("source=reddit&community=kernel"),
    ),
  ).toBe(true);
  await outgoing
    .getByRole("link", { name: "Explore this post", exact: true })
    .click();
  await expect(page.locator(".post-connections")).toContainText(
    "No verified links found",
  );
  const location = new URL(page.url());
  expect(location.searchParams.get("view")).toBe("reddit");
  expect(location.searchParams.get("community")).toBe("kernel");
  expect(
    requests.some((url) =>
      url.includes(`id=${neighbor.id}&source=reddit&community=kernel`),
    ),
  ).toBe(true);
  params.set("view", "all");
  params.delete("community");
  await page.goto(`/reader/?${params}`);
  await expect(
    page.getByRole("region", { name: "Links to this post" }),
  ).toBeVisible();
  const latest = requests.filter((url) => url.includes("/connections?")).at(-1);
  expect(
    new URL(latest ?? "http://test.invalid").searchParams.has("source"),
  ).toBe(false);
});
