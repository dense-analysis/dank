import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import {
  capturedX,
  conversationNode,
  post,
  xAccount,
  xConversation,
  xPost,
  xSource,
} from "../src/lib/api.fixture";
import type { Post, XConversationNode } from "../src/lib/types";
import { apiPath, emptyResponses, fulfillEmpty } from "./apiMock";

const profile = { handle: "author", name: "Author", icon_url: null };
const root: Post = post({
  id: "100",
  domain: "x.com",
  url: "https://x.com/author/status/100",
  author: "Author",
  title: "Feel free to explore mathematics.",
  excerpt: "Feel free to explore mathematics.",
  html: "<p>Feel free to explore mathematics.</p>",
  source: "x",
  created_at: "2026-10-16T03:40:00Z",
  media: [],
  thumbnail: null,
  x: xPost({ ...profile, replies_status: "not_collected", reply_count: 191 }),
});
const reply: Post = {
  ...root,
  id: "101",
  url: "https://x.com/author/status/101",
  title: "Three notes.",
  excerpt: "Three notes.",
  html: "<p>Three notes.</p>",
  created_at: "2026-10-16T04:04:00Z",
  x: xPost({
    ...root.x,
    ...profile,
    reply_to: {
      id: "100",
      domain: "x.com",
      url: root.url,
      is_self: true,
      collected: true,
    },
  }),
};
function node(post: Post, parent: string | null): XConversationNode {
  return conversationNode({
    ...post,
    x: capturedX({ ...post.x, media: post.x?.media ?? [] }),
    parent_id: parent,
    depth: 0,
    is_author: true,
    missing_parent: false,
    collected: true,
  });
}
function detail(post: Post): Post {
  return {
    ...post,
    x: xPost({
      ...post.x,
      ...profile,
      conversation: xConversation({
        status: "not_collected",
        captured_at: null,
        has_more: false,
        reason: null,
        limit: 0,
        reported_replies: 191,
        local_posts_count: 1,
        context: post.id === "101" ? [node(root, null)] : [],
        replies: post.id === "100" ? [node(reply, "100")] : [],
      }),
    }),
  };
}

for (const mobile of [false, true]) {
  test(`saved replies connect with preserved feed selection (${mobile ? "mobile" : "desktop"})`, async ({
    page,
  }) => {
    if (mobile) await page.setViewportSize({ width: 390, height: 844 });
    await page.route("**/api/reader/**", (route) => {
      const params = new URL(route.request().url()).searchParams;

      switch (apiPath(route)) {
        case "/api/reader/sources":
          return route.fulfill({
            json: {
              ...emptyResponses["/api/reader/sources"],
              total_posts: 2,
              sources: [
                xSource({
                  domain: "x.com",
                  name: "X",
                  kind: "x",
                  tags: [],
                  count: 2,
                  accounts: [xAccount({ ...profile, count: 2 })],
                }),
              ],
            },
          });
        case "/api/reader/post":
          return route.fulfill({
            json: detail(params.get("id") === "101" ? reply : root),
          });
        case "/api/reader/posts":
          return route.fulfill({
            json: {
              ...emptyResponses["/api/reader/posts"],
              posts:
                params.get("x_view") === "replies" ? [reply, root] : [root],
            },
          });
        default:
          return fulfillEmpty(route);
      }
    });
    await page.goto("/?view=x&account=author");
    await expect(
      page.getByRole("button", { name: "Posts", exact: true }),
    ).toHaveAttribute("aria-pressed", "true");
    await expect(page.locator(".x-post-card")).toHaveCount(1);
    await expect(page.getByLabel("Order stories")).toHaveValue("newest");
    await page
      .getByRole("button", { name: "Posts & replies", exact: true })
      .click();
    await expect(page).toHaveURL(/x_view=replies/);
    await expect(page.locator(".x-post-card")).toHaveCount(2);
    await expect(page.locator(".x-post-card").first()).toContainText(
      "Three notes.",
    );
    await page
      .getByRole("link", { name: "Reply to own post", exact: true })
      .click();
    await expect(page).toHaveURL(/article=100/);
    await expect(
      page.getByRole("heading", { name: "Saved replies" }),
    ).toBeVisible();
    await expect(page.locator(".x-conversation-entry")).toHaveCount(1);
    await page
      .getByRole("link", { name: "Open saved post", exact: true })
      .click();
    await expect(page).toHaveURL(/article=101/);
    await expect(
      page.getByRole("region", { name: "Thread context" }),
    ).toContainText(root.title);
    await expect(page.locator(".x-focused-post")).toContainText(reply.title);
    await page.goBack();
    await expect(page).toHaveURL(/article=100/);
    await page.goBack();
    await expect(page).not.toHaveURL(/article=/);
    await expect(
      page.getByRole("button", { name: "Posts & replies" }),
    ).toHaveAttribute("aria-pressed", "true");
    await page.reload();
    await expect(page.locator(".x-post-card")).toHaveCount(2);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    const accessibility = await new AxeBuilder({ page })
      .include(".feed-tools")
      .analyze();
    expect(accessibility.violations).toEqual([]);
  });
}
