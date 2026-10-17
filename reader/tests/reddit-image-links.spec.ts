import { expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const original = "https://i.redd.it/first.jpg";
const second = "https://i.redd.it/second.jpg";
const gallery = "https://www.reddit.com/gallery/abc";
const unrelated = "https://i.redd.it/other.jpg";

async function mockImages(
  page: Page,
  options: {
    noThumbnail?: boolean;
    failed?: "first" | "second";
    missingSecond?: boolean;
  } = {},
) {
  const firstImage = "/assets/reddit-first.svg";
  const secondImage = "/assets/reddit-second.svg";
  const post: Post = {
    id: "t3_abc",
    domain: "reddit.com",
    source: "reddit",
    url: "https://www.reddit.com/r/photos/comments/abc/gallery/",
    author: "Photographer",
    title: "A saved Reddit gallery",
    excerpt: `Context. ${original} ${gallery} ${second} ${unrelated}`,
    html: `<p>Context.</p><img src="${firstImage}" alt="First photograph">${options.missingSecond ? "" : `<img src="${secondImage}" alt="Second photograph">`}<p><a href="${original}">${original}</a></p><p><a href="${second}">${second}</a></p><p><a href="${gallery}">${gallery}</a></p><p>See <a href="${original}">the original photograph</a>.</p><p><a href="${unrelated}">${unrelated}</a></p><p>Plain: ${original}</p>`,
    created_at: "2026-10-17T00:00:00Z",
    thumbnail: options.noThumbnail ? null : firstImage,
    media: [],
    reddit: {
      community: "photos",
      score: 5,
      comments: 0,
      flair: null,
      images: [
        { url: firstImage, source_url: original },
        { url: secondImage, source_url: second },
      ],
    },
  };
  await page.route("**/assets/reddit-*.svg", (route) =>
    route.fulfill(
      options.failed &&
        route.request().url().endsWith(`reddit-${options.failed}.svg`)
        ? { status: 404 }
        : {
            contentType: "image/svg+xml",
            body: '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="300"><rect width="400" height="300" fill="#538573"/></svg>',
          },
    ),
  );
  await page.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());

    return route.fulfill({
      json: url.pathname.endsWith("/sources")
        ? {
            sources: [
              {
                domain: "reddit.com",
                name: "Reddit",
                kind: "reddit",
                tags: [],
                count: 1,
              },
            ],
            total_posts: 1,
          }
        : url.pathname.endsWith("/post")
          ? post
          : { posts: [post], next_cursor: null, limited: false },
    });
  });
}

for (const width of [1440, 390]) {
  test(`Reddit image previews replace their raw links in feeds and articles at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await mockImages(page);

    for (const query of ["", "view=reddit", "view=reddit&community=photos"]) {
      await page.goto(`/reader/?${query}`);
      const card = page.locator(".reddit-card");
      const summary = card.locator(".post-open p");
      await expect(card.locator(".thumbnail img")).toHaveJSProperty(
        "naturalWidth",
        400,
      );
      await expect(summary).toContainText("Context.");
      await expect(summary).not.toContainText(original);
      await expect(summary).not.toContainText(gallery);
      await expect(summary).toContainText(second);
      await expect(summary).toContainText(unrelated);
      await card
        .getByRole("link", {
          name: "Open image and story: A saved Reddit gallery",
        })
        .click();
      const reader = page.locator(".article-page");
      await reader.getByAltText("Second photograph").scrollIntoViewIfNeeded();
      await expect(reader.getByAltText("Second photograph")).toHaveJSProperty(
        "naturalWidth",
        400,
      );
      for (const link of [original, second, gallery]) {
        await expect(
          reader.getByRole("link", { name: link, exact: true }),
        ).toBeHidden();
      }
      const plain = reader
        .locator("p")
        .filter({ hasText: `Plain: ${original}` });
      await expect
        .poll(async () => (await plain.innerText()).trim())
        .toBe("Plain:");
      await expect(
        reader.getByRole("link", {
          name: "the original photograph",
          exact: true,
        }),
      ).toHaveAttribute("href", original);
      await expect(
        reader.getByRole("link", { name: unrelated, exact: true }),
      ).toBeVisible();
      await expect(
        reader.getByRole("link", {
          name: "Open full-size image: First photograph",
        }),
      ).toHaveAttribute("target", "_blank");
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
    }
  });
}

for (const noThumbnail of [false, true]) {
  test(`a ${noThumbnail ? "missing" : "failed"} thumbnail leaves the raw image and gallery references readable`, async ({
    page,
  }) => {
    await mockImages(page, {
      noThumbnail,
      failed: noThumbnail ? undefined : "first",
    });
    await page.goto("/reader/?view=reddit");
    const summary = page.locator(".reddit-card .post-open p");
    await expect(summary).toContainText(original);
    await expect(summary).toContainText(gallery);
  });
}

for (const missingSecond of [false, true]) {
  test(`a ${missingSecond ? "missing" : "failed"} gallery image keeps its source and gallery links available`, async ({
    page,
  }) => {
    await mockImages(page, {
      missingSecond,
      failed: missingSecond ? undefined : "second",
    });
    await page.goto(
      "/reader/?view=reddit&article=t3_abc&article_source=reddit.com",
    );
    const reader = page.locator(".article-page");
    await reader
      .getByRole("link", { name: gallery, exact: true })
      .scrollIntoViewIfNeeded();
    await expect(
      reader.getByRole("link", { name: gallery, exact: true }),
    ).toBeVisible();
    await expect(
      reader.getByRole("link", { name: second, exact: true }),
    ).toBeVisible();
    await expect(
      reader.getByRole("link", { name: original, exact: true }),
    ).toBeHidden();
  });
}

test("an image failure restores raw references without replacing other article images", async ({
  page,
}) => {
  await mockImages(page);
  await page.goto(
    "/reader/?view=reddit&article=t3_abc&article_source=reddit.com",
  );
  const reader = page.locator(".article-page");
  const first = reader.getByAltText("First photograph");
  const other = reader.getByAltText("Second photograph");
  await other.scrollIntoViewIfNeeded();
  await expect(other).toHaveJSProperty("naturalWidth", 400);
  await expect(
    reader.getByRole("link", { name: gallery, exact: true }),
  ).toBeHidden();
  await first.evaluate((image) => image.setAttribute("data-retained", "true"));
  await other.evaluate((image) => image.dispatchEvent(new Event("error")));
  await expect(
    reader.getByRole("link", { name: second, exact: true }),
  ).toBeVisible();
  await expect(
    reader.getByRole("link", { name: gallery, exact: true }),
  ).toBeVisible();
  await expect(first).toHaveAttribute("data-retained", "true");
  await reader.getByRole("button", { name: "Larger article text" }).click();
  await expect(first).toHaveAttribute("data-retained", "true");
});
