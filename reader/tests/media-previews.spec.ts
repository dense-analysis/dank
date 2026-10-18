import { expect, type Locator, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

async function mockMedia(page: Page, ratio = 9 / 16) {
  const image = "/assets/preview-image.svg";
  const poster = "/assets/preview-poster.svg";
  const posts: Post[] = [
    {
      id: "x-media",
      domain: "x.com",
      source: "x",
      url: "https://x.com/photographer/status/123",
      author: "Photographer",
      title: "An image and a video",
      excerpt: "An image and a video",
      html: "<p>An image and a video</p>",
      created_at: "2026-10-17T00:00:00Z",
      thumbnail: null,
      media: [],
      x: {
        handle: "photographer",
        name: "Photographer",
        reply_count: 191,
        repost_count: 232,
        like_count: 2700,
        view_count: 217500,
        media: [
          {
            type: "image",
            url: image,
            poster_url: null,
            alt: "Whole photograph",
            remote: false,
          },
          {
            type: "video",
            url: "/assets/preview-video.mp4",
            poster_url: poster,
            aspect_ratio: 9 / 16,
            alt: "Portrait video",
            remote: false,
          },
        ],
      },
    },
    {
      id: "reddit-image",
      domain: "reddit.com",
      source: "reddit",
      url: "https://www.reddit.com/r/photos/comments/image/",
      author: "Photographer",
      title: "A Reddit photograph",
      excerpt: "The whole photograph",
      html: `<p>The whole photograph</p><img src="${image}" alt="Whole photograph">`,
      created_at: "2026-10-17T00:00:00Z",
      thumbnail: image,
      media: [],
      reddit: { community: "photos", score: 1, comments: 0, flair: null },
    },
    {
      id: "reddit-video",
      domain: "reddit.com",
      source: "reddit",
      url: "https://www.reddit.com/r/photos/comments/video/",
      author: "Photographer",
      title: "A Reddit video",
      excerpt: "A portrait video",
      html: "<p>A portrait video</p>",
      created_at: "2026-10-17T00:00:00Z",
      thumbnail: null,
      media: [],
      reddit: {
        community: "photos",
        score: 1,
        comments: 0,
        flair: null,
        video: {
          url: "/assets/preview-video.mp4",
          poster_url: poster,
          remote: false,
        },
      },
    },
  ];
  const blog: Post = {
    ...posts[1],
    id: "blog",
    domain: "example.com",
    source: "rss",
    url: "https://example.com/story",
    title: "A blog article",
    html: "<p>Article text.</p>",
    excerpt: "Article text.",
    thumbnail: null,
    reddit: undefined,
  };
  posts.push(blog, {
    ...blog,
    id: "hn",
    domain: "news.ycombinator.com",
    source: "hacker_news",
    url: "https://news.ycombinator.com/item?id=1",
    title: "A Hacker News story",
    hacker_news: {
      submitted_at: blog.created_at,
      points: 200,
      comments: 20,
      day: "2026-10-17",
      rank: 1,
      discussion_url: "https://news.ycombinator.com/item?id=1",
      external_url: "https://example.com/original",
    },
  });
  let videoRequests = 0;
  await page.route("**/assets/preview-video.mp4", (route) => {
    videoRequests++;

    return route.abort();
  });
  await page.route("**/assets/preview-*.svg", (route) => {
    const aspect = route.request().url().endsWith("poster.svg")
      ? 9 / 16
      : ratio;
    return route.fulfill({
      contentType: "image/svg+xml",
      body: `<svg xmlns="http://www.w3.org/2000/svg" width="${1600 * aspect}" height="1600"><rect width="100%" height="100%" fill="#538573"/><circle cx="50%" cy="50%" r="100" fill="#dce8df"/></svg>`,
    });
  });
  await page.route("**/api/reader/**", (route) => {
    const url = new URL(route.request().url());

    return route.fulfill({
      json: url.pathname.endsWith("/sources")
        ? {
            sources: [
              {
                domain: "example.com",
                name: "Example",
                kind: "rss",
                count: 1,
                tags: [],
              },
              {
                domain: "news.ycombinator.com",
                name: "Hacker News",
                kind: "hacker_news",
                count: 1,
                tags: [],
              },
              { domain: "x.com", name: "X", kind: "x", count: 1, tags: [] },
              {
                domain: "reddit.com",
                name: "Reddit",
                kind: "reddit",
                count: 2,
                tags: [],
              },
            ],
            total_posts: posts.length,
          }
        : url.pathname.endsWith("/post")
          ? posts.find((post) => post.id === url.searchParams.get("id"))
          : {
              posts: posts.filter((post) => {
                const kind = url.searchParams.get("kind");
                return (
                  !kind ||
                  kind === "all" ||
                  post.source === (kind === "stories" ? "rss" : kind)
                );
              }),
              next_cursor: null,
              limited: false,
            },
    });
  });

  return () => videoRequests;
}

async function expectRatio(element: Locator, ratio: number) {
  await element.scrollIntoViewIfNeeded();
  expect(
    await element.evaluate((node) => node.getBoundingClientRect().height),
  ).toBeLessThanOrEqual(482);
  await expect
    .poll(async () => {
      return element.evaluate((node, expected) => {
        const box = node.getBoundingClientRect();
        const style = getComputedStyle(node);
        const width =
          box.width -
          parseFloat(style.borderLeftWidth) -
          parseFloat(style.borderRightWidth);
        const height =
          box.height -
          parseFloat(style.borderTopWidth) -
          parseFloat(style.borderBottomWidth);

        return Math.abs(width / height - expected);
      }, ratio);
    })
    .toBeLessThan(0.03);
}

for (const width of [1440, 390, 320]) {
  for (const ratio of [9 / 16, 1, 16 / 9]) {
    test(`image frames preserve ${ratio} proportions at ${width}px`, async ({
      page,
    }, testInfo) => {
      await page.setViewportSize({ width, height: 900 });
      const requests = await mockMedia(page, ratio);
      await page.goto("/reader/");
      const xImage = page.locator(".x-attachment img");
      await expectRatio(xImage, ratio);
      await expectRatio(page.locator(".reddit-card .thumbnail"), ratio);
      await expect(
        page.getByRole("link", { name: "Open full-size image 1" }),
      ).toHaveAttribute("target", "_blank");
      await expect(
        page.getByRole("link", { name: "Open full-size image 1" }),
      ).toHaveAttribute("href", /preview-image.svg$/);
      await page.goto(
        "/reader/?article=reddit-image&article_source=reddit.com",
      );
      await expectRatio(page.locator(".article-body img"), ratio);
      await expect(
        page.getByRole("link", {
          name: "Open full-size image: Whole photograph",
        }),
      ).toHaveAttribute("target", "_blank");
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
      expect(requests()).toBe(0);

      if (ratio === 9 / 16) {
        await page.screenshot({
          path: testInfo.outputPath(`fitted-image-${width}.png`),
          fullPage: true,
        });
      }
    });
  }
}

test("image and video fitting can be changed independently and persist through navigation, reload and reset", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  const requests = await mockMedia(page);
  await page.goto("/reader/?view=settings");
  const images = page.getByRole("checkbox", {
    name: "Fit image previews to their aspect ratio",
  });
  const videos = page.getByRole("checkbox", {
    name: "Fit video previews to their aspect ratio",
  });
  await expect(images).toBeChecked();
  await expect(videos).toBeChecked();

  for (const [fitImages, fitVideos] of [
    [false, true],
    [true, false],
    [false, false],
    [true, true],
  ]) {
    await images.setChecked(fitImages);
    await videos.setChecked(fitVideos);
    await page.reload();
    await expect(images).toBeChecked({ checked: fitImages });
    await expect(videos).toBeChecked({ checked: fitVideos });
    await page.getByRole("button", { name: "All", exact: true }).click();
    const xImage = page.locator(".x-attachment img");
    const xVideo = page.locator(".x-attachment video");
    const redditImage = page.locator(".reddit-card .thumbnail img");
    const redditVideo = page.locator(".reddit-card video");

    for (const [element, fitted] of [
      [xImage, fitImages],
      [redditImage, fitImages],
      [xVideo, fitVideos],
      [redditVideo, fitVideos],
    ] as const) {
      await element.scrollIntoViewIfNeeded();

      if (fitted) {
        await expectRatio(element, 9 / 16);
      } else {
        await expect
          .poll(async () => {
            const box = await element.boundingBox();

            return box ? box.width / box.height : 0;
          })
          .toBeGreaterThan(0.7);
        await expect(element).toHaveCSS("object-fit", "contain");
      }
    }

    await page.goto("/reader/?article=x-media&article_source=x.com");

    if (fitImages)
      await expectRatio(
        page.locator(".article-page .x-attachment img"),
        9 / 16,
      );

    if (fitVideos)
      await expectRatio(
        page.locator(".article-page .x-attachment video"),
        9 / 16,
      );

    await page.getByRole("button", { name: "Settings", exact: true }).click();
  }

  await images.uncheck();
  await videos.uncheck();
  await page.getByRole("button", { name: "Reset to defaults" }).click();
  await page.reload();
  await expect(images).toBeChecked();
  await expect(videos).toBeChecked();
  expect(requests()).toBe(0);
});

for (const width of [1440, 1100, 816, 653, 390, 320]) {
  test(`social media stays compact and footers adapt at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 1400 });
    await page.addInitScript(() => {
      localStorage.setItem(
        "dank-reader:read:v1",
        JSON.stringify([JSON.stringify(["x.com", "x-media"])]),
      );
    });
    const requests = await mockMedia(page);
    await page.goto("/reader/");

    for (const selector of [".x-attachment video", ".reddit-card video"]) {
      await expectRatio(page.locator(selector), 9 / 16);
    }

    for (const selector of [".x-post-card", ".reddit-card"]) {
      const card = page.locator(selector).first();
      const metrics = card.locator(".social-engagement");
      await metrics.scrollIntoViewIfNeeded();
      const layout = await metrics.evaluate((element) => {
        const parent = element.closest(".post-card");
        if (!parent) throw new Error("Missing post card");
        const card = parent.getBoundingClientRect();
        const items = Array.from(element.children).map((child) =>
          child.getBoundingClientRect(),
        );
        const group = element.getBoundingClientRect();
        const original = parent.querySelector(
          ".social-post-actions .icon-button",
        );
        if (!original) throw new Error("Missing original-source link");
        const originalBox = original.getBoundingClientRect();

        return {
          inset: items[0].left - card.left,
          width: card.width,
          originalRightInset: card.right - originalBox.right,
          fontSize: getComputedStyle(element.children[0]).fontSize,
          iconWidth: element.querySelector("svg")?.getBoundingClientRect()
            .width,
          utilitiesWidth: parent
            .querySelector(".post-utilities")
            ?.getBoundingClientRect().width,
          coverage: group.width / card.width,
          gaps: items
            .slice(1)
            .map((item, index) => item.left - items[index].right),
          labelsClipped: Array.from(
            element.querySelectorAll(".visually-hidden"),
          ).every((label) => getComputedStyle(label).clipPath === "inset(50%)"),
        };
      });
      expect(layout.labelsClipped).toBe(true);

      if (layout.width > 760) {
        expect(layout.inset).toBeLessThan(layout.width / 4);
        expect(layout.originalRightInset).toBeLessThan(2);
        expect(Math.max(...layout.gaps)).toBeLessThanOrEqual(25);
      } else {
        expect(layout.coverage).toBeGreaterThan(0.6);
        expect(layout.inset).toBeLessThan(2);
        expect(layout.originalRightInset).toBeLessThan(2);
        expect(layout.fontSize).toBe("13px");
        expect(layout.iconWidth).toBe(18);

        if (layout.utilitiesWidth !== undefined)
          expect(layout.utilitiesWidth).toBeLessThanOrEqual(64);

        expect(
          Math.max(...layout.gaps) - Math.min(...layout.gaps),
        ).toBeLessThan(2);
      }
    }

    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect(requests()).toBe(0);
    await page.screenshot({
      path: testInfo.outputPath(`social-layout-${width}.png`),
      fullPage: true,
    });
    await page.setViewportSize({ width, height: 600 });

    for (const selector of [".x-attachment video", ".reddit-card video"]) {
      expect(
        await page
          .locator(selector)
          .evaluate((element) => element.getBoundingClientRect().height),
      ).toBeLessThanOrEqual(362);
    }
  });
}

for (const width of [1440, 653, 390, 320]) {
  test(`All and source feeds share footer sizing, source links and read status at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.addInitScript(() => {
      localStorage.setItem(
        "dank-reader:read:v1",
        JSON.stringify(
          [
            ["x.com", "x-media"],
            ["reddit.com", "reddit-image"],
            ["reddit.com", "reddit-video"],
            ["example.com", "blog"],
            ["news.ycombinator.com", "hn"],
          ].map((identity) => JSON.stringify(identity)),
        ),
      );
    });
    await mockMedia(page);

    for (const [view, expected] of [
      ["all", 5],
      ["reddit", 2],
      ["x", 1],
      ["stories", 1],
      ["hacker_news", 1],
    ] as const) {
      await page.goto(`/reader/?view=${view}`);
      const cards = page.locator(".post-card");
      await expect(cards).toHaveCount(expected);

      for (const card of await cards.all()) {
        const footer = card.locator(".post-footer");
        await footer.scrollIntoViewIfNeeded();
        await expect(
          footer.locator(".post-utilities .read-indicator"),
        ).toHaveCount(1);
        await expect(footer.locator(".post-utilities a")).toHaveAttribute(
          "target",
          "_blank",
        );
        const layout = await footer.evaluate((element) => {
          const parent = element.closest(".post-card");
          if (!parent) throw new Error("Missing card");
          const card = parent.getBoundingClientRect();
          const row = element.querySelector(".social-post-actions");
          const source = element.querySelector(".post-utilities a");
          const first = element.querySelector(
            ".social-metric, .post-footer-details",
          );
          if (!row || !source || !first)
            throw new Error("Missing footer contents");
          const content = element.previousElementSibling;
          return {
            width: card.width,
            font: getComputedStyle(first).fontSize,
            icon: source.querySelector("svg")?.getBoundingClientRect().width,
            firstInset: first.getBoundingClientRect().left - card.left,
            rightInset: card.right - source.getBoundingClientRect().right,
            rowHeight: row.getBoundingClientRect().height,
            gap: content
              ? element.getBoundingClientRect().top -
                content.getBoundingClientRect().bottom
              : 0,
          };
        });
        const narrow = layout.width <= 760;
        expect(layout.font).toBe(narrow ? "13px" : "12px");
        expect(layout.icon).toBe(narrow ? 18 : 16);
        expect(layout.firstInset).toBeLessThan(2);
        expect(layout.rightInset).toBeLessThan(2);
        expect(layout.rowHeight).toBeGreaterThanOrEqual(32);
        expect(Math.abs(layout.gap - (narrow ? 8 : 18))).toBeLessThan(2);
      }
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
    }
  });
}
