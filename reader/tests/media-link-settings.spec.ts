import { expect, type Locator, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const xImage = "https://x.com/author/status/123/photo/1";
const xVideo = "https://x.com/author/status/123/video/2";
const youtube = "https://youtu.be/dQw4w9WgXcQ";
const redditImage = "https://i.redd.it/first.jpg";
const redditVideo = "https://v.redd.it/clip";
const gallery = "https://www.reddit.com/gallery/abc";
const base: Post = {
  id: "123",
  domain: "x.com",
  source: "x",
  url: "https://x.com/author/status/123",
  author: "Author",
  title: "Media settings",
  excerpt: "Media settings",
  html: "",
  created_at: "2026-10-17T00:00:00Z",
  thumbnail: null,
  media: [],
};

async function mockMedia(page: Page) {
  let requests = 0;
  const photo: Post = {
    ...base,
    id: "t3_abc",
    domain: "reddit.com",
    source: "reddit",
    url: "https://www.reddit.com/r/photos/comments/abc/gallery/",
    title: "Reddit photograph",
    excerpt: `Photo. ${redditImage} ${gallery}`,
    thumbnail: "/assets/image.svg",
    html: `<img src="/assets/image.svg" alt="Photograph"><p><a href="${redditImage}">${redditImage}</a> <a href="${gallery}">${gallery}</a></p>`,
    reddit: {
      community: "photos",
      score: 1,
      comments: 0,
      flair: null,
      images: [{ url: "/assets/image.svg", source_url: redditImage }],
    },
  };
  const posts: Post[] = [
    {
      ...base,
      html: `<p>Look. <a href="${xImage}">${xImage}</a> <a href="${xVideo}">${xVideo}</a> <a href="${youtube}">${youtube}</a></p><p>See <a href="${xImage}">the image</a>.</p>`,
      x: {
        handle: "author",
        name: "Author",
        media: [
          {
            type: "image",
            url: "/assets/image.svg",
            source_url: xImage,
            poster_url: null,
            alt: "X image",
            remote: false,
          },
          {
            type: "video",
            url: "/assets/video.mp4",
            source_url: xVideo,
            poster_url: "/assets/image.svg",
            aspect_ratio: 4 / 3,
            alt: "X video",
            remote: false,
          },
        ],
        links: [
          {
            url: youtube,
            title: "An explanation",
            description: "Video",
            image_url: null,
          },
        ],
      },
    },
    photo,
    {
      ...photo,
      id: "t3_clip",
      title: "Reddit clip",
      excerpt: `Clip. ${redditVideo}`,
      html: `<img src="/assets/image.svg" alt="Photograph"><p><a href="${redditImage}">${redditImage}</a> <a href="${redditVideo}">${redditVideo}</a></p>`,
      reddit: {
        ...photo.reddit,
        community: "photos",
        score: 1,
        comments: 0,
        flair: null,
        video: {
          url: "/assets/video.mp4",
          source_url: redditVideo,
          poster_url: "/assets/image.svg",
          remote: false,
        },
      },
    },
  ];
  await page.route("**/assets/image.svg", (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="300"><rect width="400" height="300" fill="#538573"/></svg>',
    }),
  );
  await page.route("**/assets/video.mp4", (route) => {
    requests++;
    return route.abort();
  });
  await page.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());

    return route.fulfill({
      json: url.pathname.endsWith("/sources")
        ? {
            sources: [
              { domain: "x.com", name: "X", kind: "x", count: 1, tags: [] },
              {
                domain: "reddit.com",
                name: "Reddit",
                kind: "reddit",
                count: 2,
                tags: [],
              },
            ],
            total_posts: 3,
          }
        : url.pathname.endsWith("/post")
          ? posts.find((post) => post.id === url.searchParams.get("id"))
          : { posts, next_cursor: null, limited: false },
    });
  });

  return () => requests;
}

async function expectRaw(scope: Locator, url: string, hidden: boolean) {
  const link = scope.getByRole("link", { name: url, exact: true });

  if (hidden) await expect(link).toBeHidden();
  else await expect(link).toBeVisible();
}

for (const width of [1440, 390]) {
  test(`raw image and video link preferences apply independently to Reddit and X at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    const requests = await mockMedia(page);
    await page.goto("/reader/?view=settings");
    const images = page.getByRole("checkbox", {
      name: "Hide raw image links",
      exact: true,
    });
    const videos = page.getByRole("checkbox", {
      name: "Hide raw video links",
      exact: true,
    });
    await expect(images).toBeChecked();
    await expect(videos).toBeChecked();

    for (const [hideImages, hideVideos] of [
      [false, true],
      [true, false],
      [false, false],
      [true, true],
    ]) {
      await images.setChecked(hideImages);
      await videos.setChecked(hideVideos);
      await page.reload();
      await expect(images).toBeChecked({ checked: hideImages });
      await expect(videos).toBeChecked({ checked: hideVideos });
      await page.goto("/reader/");
      const xCard = page.locator(".post-card").first();
      await expectRaw(xCard, xImage, hideImages);
      await expectRaw(xCard, xVideo, hideVideos);
      await expectRaw(xCard, youtube, hideVideos);
      await expect(
        xCard.getByRole("button", { name: "Play video: An explanation" }),
      ).toBeVisible();
      const photoSummary = page
        .locator(".reddit-card")
        .filter({ hasText: "Reddit photograph" })
        .locator(".post-open p");
      const clipSummary = page
        .locator(".reddit-card")
        .filter({ hasText: "Reddit clip" })
        .locator(".post-open p");

      if (hideImages) {
        await expect(photoSummary).not.toContainText(redditImage);
        await expect(photoSummary).not.toContainText(gallery);
      } else {
        await expect(photoSummary).toContainText(redditImage);
        await expect(photoSummary).toContainText(gallery);
      }

      if (hideVideos) await expect(clipSummary).not.toContainText(redditVideo);
      else await expect(clipSummary).toContainText(redditVideo);

      await page.goto("/reader/?article=123&article_source=x.com");
      const reader = page.locator(".article-page");
      await expectRaw(reader, xImage, hideImages);
      await expectRaw(reader, xVideo, hideVideos);
      await expectRaw(reader, youtube, hideVideos);
      await expect(
        reader.getByRole("link", { name: "the image", exact: true }),
      ).toHaveAttribute("href", xImage);
      await expect(
        reader.getByRole("link", { name: "Open full-size image 1" }),
      ).toHaveAttribute("target", "_blank");
      await page.goto("/reader/?article=t3_clip&article_source=reddit.com");
      await reader.getByAltText("Photograph").scrollIntoViewIfNeeded();
      await expect(reader.getByAltText("Photograph")).toHaveJSProperty(
        "naturalWidth",
        400,
      );
      await expectRaw(reader, redditImage, hideImages);
      await expectRaw(reader, redditVideo, hideVideos);
      await expect(
        reader.getByLabel("Reddit video", { exact: true }),
      ).toBeVisible();
      await page.goto("/reader/?article=t3_abc&article_source=reddit.com");
      await expectRaw(reader, gallery, hideImages);
      await page.goto("/reader/?view=settings");
    }

    await images.uncheck();
    await videos.uncheck();
    await page.getByRole("button", { name: "Reset to defaults" }).click();
    await page.reload();
    await expect(images).toBeChecked();
    await expect(videos).toBeChecked();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect(requests()).toBe(0);
  });
}
