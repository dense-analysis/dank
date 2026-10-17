import { expect, type Locator, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const firstPhoto = "https://x.com/photographer/status/123/photo/1";
const secondPhoto = "https://twitter.com/another/status/456/photo/2";
const otherPhoto = "https://x.com/photographer/status/123/photo/3";

async function mockImages(
  page: Page,
  unavailable: "failed" | "unsupported" | null = null,
) {
  const post: Post = {
    id: "123",
    domain: "x.com",
    source: "x",
    url: "https://x.com/photographer/status/123",
    author: "Photographer",
    title: "A pair of photographs",
    excerpt: "A pair of photographs",
    html: `<p>A pair of photographs. <a href="${firstPhoto}">${firstPhoto}</a></p><p><a href="${secondPhoto}">x.com/another/status/456/photo/2?s=20</a></p><p>See <a href="${firstPhoto}">the original photograph</a>.</p><p><a href="${otherPhoto}">${otherPhoto}</a></p>`,
    created_at: "2026-10-17T00:00:00Z",
    thumbnail: null,
    media: [],
    x: {
      handle: "photographer",
      name: "Photographer",
      media: [
        {
          type: "image",
          url:
            unavailable === "unsupported"
              ? "https://untrusted.example/image.jpg"
              : "/assets/photo-one.svg",
          poster_url: null,
          source_url: firstPhoto,
          alt: "First photograph",
          remote: false,
        },
        {
          type: "image",
          url: "/assets/photo-two.svg",
          poster_url: null,
          source_url: secondPhoto,
          alt: "Second photograph",
          remote: false,
        },
      ],
    },
  };
  await page.route("**/assets/photo-*.svg", (route) =>
    route.fulfill(
      unavailable === "failed" &&
        route.request().url().endsWith("photo-one.svg")
        ? { status: 404 }
        : {
            contentType: "image/svg+xml",
            body: '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="300"><rect width="100%" height="100%" fill="#538573"/></svg>',
          },
    ),
  );
  await page.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());

    return route.fulfill({
      json: url.pathname.endsWith("/sources")
        ? {
            sources: [
              { domain: "x.com", name: "X", kind: "x", tags: [], count: 1 },
            ],
            total_posts: 1,
          }
        : url.pathname.endsWith("/post")
          ? post
          : { posts: [post], next_cursor: null, limited: false },
    });
  });
}

async function expectPhotoLinks(scope: Locator, firstVisible = false) {
  const raw = scope.getByRole("link", { name: firstPhoto, exact: true });

  if (firstVisible) await expect(raw).toHaveAttribute("href", firstPhoto);
  else await expect(raw).toHaveCount(0);

  await expect(
    scope.getByRole("link", {
      name: "x.com/another/status/456/photo/2?s=20",
      exact: true,
    }),
  ).toHaveCount(0);
  await expect(
    scope.getByRole("link", { name: "the original photograph", exact: true }),
  ).toHaveAttribute("href", firstPhoto);
  await expect(
    scope.getByRole("link", { name: otherPhoto, exact: true }),
  ).toHaveAttribute("href", otherPhoto);
}

for (const width of [1440, 390]) {
  test(`clickable photo previews replace only their raw references at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await mockImages(page);

    for (const query of [
      "",
      "view=x",
      "view=x&account=photographer",
      "view=x&article=123&article_source=x.com",
    ]) {
      await page.goto(`/reader/?${query}`);
      const scope = page.locator(
        query.includes("article=") ? ".article-page" : ".post-card",
      );
      await scope.getByAltText("First photograph").scrollIntoViewIfNeeded();
      await expect(scope.getByAltText("First photograph")).toHaveJSProperty(
        "naturalWidth",
        400,
      );
      await expectPhotoLinks(scope);
      await expect(
        scope.getByRole("link", { name: "Open full-size image 1" }),
      ).toHaveAttribute("href", /photo-one.svg$/);
      await expect(
        scope.getByRole("link", { name: "Open full-size image 1" }),
      ).toHaveAttribute("target", "_blank");
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
    }
  });
}

for (const unavailable of ["failed", "unsupported"] as const) {
  for (const detail of [false, true]) {
    test(`${unavailable} image restores only its own raw reference in the ${detail ? "reader" : "feed"}`, async ({
      page,
    }) => {
      await mockImages(page, unavailable);
      await page.goto(
        detail
          ? "/reader/?view=x&article=123&article_source=x.com"
          : "/reader/?view=x",
      );
      const scope = page.locator(detail ? ".article-page" : ".post-card");
      await scope.locator(".x-attachments").scrollIntoViewIfNeeded();

      await expect(
        scope.getByText("Image unavailable", { exact: true }),
      ).toBeVisible();
      await expectPhotoLinks(scope, true);
      await expect(
        scope.getByRole("link", { name: "Open full-size image 2" }),
      ).toHaveAttribute("href", /photo-two.svg$/);
    });
  }
}
