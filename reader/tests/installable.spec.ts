import { expect, test } from "@playwright/test";

test("provides a stable standalone launch and usable home-screen icons", async ({
  page,
  request,
}) => {
  await page.route("**/api/reader/**", (route) =>
    route.fulfill({
      json: {
        sources: [],
        posts: [],
        total_posts: 0,
        next_cursor: null,
        limited: false,
      },
    }),
  );
  await page.goto("/reader/?kind=reddit&q=example");

  const manifestHref = await page
    .locator('link[rel="manifest"]')
    .getAttribute("href");
  expect(manifestHref).toBe("/reader/manifest.webmanifest");

  const manifestResponse = await request.get(manifestHref as string);
  expect(manifestResponse.ok()).toBe(true);
  expect(manifestResponse.headers()["content-type"]).toMatch(
    /application\/(manifest\+json|json)/,
  );
  const manifest = await manifestResponse.json();
  expect(manifest).toMatchObject({
    id: "/reader/",
    name: "DANK",
    short_name: "DANK",
    start_url: "/reader/",
    scope: "/reader/",
    display: "standalone",
    theme_color: "#101113",
    background_color: "#101113",
  });
  expect(manifest.icons).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ sizes: "192x192", purpose: "any" }),
      expect.objectContaining({ sizes: "512x512", purpose: "any" }),
      expect.objectContaining({ sizes: "512x512", purpose: "maskable" }),
    ]),
  );
  const launch = await request.get(manifest.start_url);
  expect(launch.ok()).toBe(true);
  expect(launch.headers()["content-type"]).toContain("text/html");
  expect(await launch.text()).toContain('id="root"');

  const icons = [
    ...manifest.icons,
    {
      src: await page
        .locator('link[rel="apple-touch-icon"]')
        .getAttribute("href"),
      sizes: "180x180",
      type: "image/png",
    },
  ];

  for (const icon of icons) {
    const url = new URL(icon.src, manifestResponse.url());
    expect(url.origin).toBe(new URL(page.url()).origin);
    expect(url.pathname).toMatch(/^\/reader\/icons\//);

    const response = await request.get(url.href);
    expect(response.ok()).toBe(true);
    expect(response.headers()["content-type"]).toContain(icon.type);
    const dimensions = await page.evaluate(async (src) => {
      const image = new Image();
      image.src = src;
      await image.decode();

      return `${image.naturalWidth}x${image.naturalHeight}`;
    }, url.href);
    expect(dimensions).toBe(icon.sizes);
  }
});
