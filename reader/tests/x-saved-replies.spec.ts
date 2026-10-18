import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import type { Post, XConversationNode } from "../src/lib/types";

const profile = { handle: "author", name: "Author", icon_url: null };
const root: Post = {
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
  x: { ...profile, replies_status: "not_collected", reply_count: 191 },
};
const reply: Post = {
  ...root,
  id: "101",
  url: "https://x.com/author/status/101",
  title: "Three notes.",
  excerpt: "Three notes.",
  html: "<p>Three notes.</p>",
  created_at: "2026-10-16T04:04:00Z",
  x: {
    ...root.x,
    ...profile,
    reply_to: {
      id: "100",
      domain: "x.com",
      url: root.url,
      is_self: true,
      collected: true,
    },
  },
};
function node(post: Post, parent: string | null): XConversationNode {
  return {
    ...post,
    parent_id: parent,
    depth: 0,
    is_author: true,
    missing_parent: false,
    collected: true,
  };
}
function detail(post: Post): Post {
  return {
    ...post,
    x: {
      ...post.x,
      ...profile,
      conversation: {
        status: "not_collected",
        captured_at: null,
        has_more: false,
        reason: null,
        limit: 0,
        reported_replies: 191,
        local_posts_count: 1,
        context: post.id === "101" ? [node(root, null)] : [],
        replies: post.id === "100" ? [node(reply, "100")] : [],
      },
    },
  };
}

for (const mobile of [false, true]) {
  test(`saved replies connect with preserved feed selection (${mobile ? "mobile" : "desktop"})`, async ({
    page,
  }) => {
    if (mobile) await page.setViewportSize({ width: 390, height: 844 });
    await page.route("**/api/reader/**", async (route) => {
      const url = new URL(route.request().url());
      if (url.pathname.endsWith("/sources")) {
        await route.fulfill({
          json: {
            total_posts: 2,
            sources: [
              {
                domain: "x.com",
                name: "X",
                kind: "x",
                tags: [],
                count: 2,
                accounts: [{ ...profile, count: 2 }],
              },
            ],
          },
        });
      } else if (url.pathname.endsWith("/post")) {
        await route.fulfill({
          json: detail(url.searchParams.get("id") === "101" ? reply : root),
        });
      } else {
        await route.fulfill({
          json: {
            posts:
              url.searchParams.get("x_view") === "replies"
                ? [reply, root]
                : [root],
            next_cursor: null,
            limited: false,
          },
        });
      }
    });
    await page.goto("/reader/?view=x&account=author");
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
