import { expect, test } from "@playwright/test";

for (const width of [320, 390, 1280]) {
  test(`YouTube channel header keeps captured identity and video totals at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 800 });
    await page.route("**/assets/channel.svg", (route) =>
      route.fulfill({
        contentType: "image/svg+xml",
        body: '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32"><rect width="32" height="32" fill="#582c7e"/></svg>',
      }),
    );
    await page.route("**/api/reader/**", (route) => {
      const path = new URL(route.request().url()).pathname;

      if (path.endsWith("/youtube/sources"))
        return route.fulfill({
          json: {
            feeds: [],
            channels: [
              {
                id: "channel4",
                name: "Channel 4 News",
                url: "https://www.youtube.com/@Channel4News",
                count: 144,
                avatar_url: "/assets/channel.svg",
                reported_video_count: 26000,
                reported_video_count_text: "26K videos",
              },
            ],
            total: 144,
            linked_count: 0,
          },
        });
      if (path.endsWith("/youtube"))
        return route.fulfill({
          json: { items: [], total: 0, next_cursor: null },
        });

      return route.fulfill({ json: { sources: [], total_posts: 0 } });
    });
    await page.goto(
      "/reader/?view=youtube&channel=channel4&format=short&q=test",
    );
    const header = page.locator(".topbar");
    await expect(
      header.getByRole("heading", { name: "Channel 4 News", exact: true }),
    ).toBeVisible();
    await expect(header.locator(".channel-avatar img")).toBeVisible();
    await expect(header.locator(".collection-count-values")).toHaveText(
      "144/≈26Kvideos",
    );
    await expect(header.locator(".collection-count-pill")).toHaveAttribute(
      "title",
      "144 videos collected / 26K videos reported by YouTube at last capture",
    );
    const style = await header.evaluate((element) => {
      const value = getComputedStyle(element);

      return { position: value.position, backdropFilter: value.backdropFilter };
    });
    expect(style.position).toBe("sticky");
    expect(style.backdropFilter).toContain("blur(");
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: testInfo.outputPath(`youtube-header-${width}.png`),
    });
    await header.getByRole("button", { name: "Back to YouTube" }).click();
    await expect(page).toHaveURL(/\/reader\/\?view=youtube$/);
  });
}
