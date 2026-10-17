import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Post } from "../src/lib/types";

const diagram = (background = "") =>
  `<svg xmlns="http://www.w3.org/2000/svg" width="480" height="288">${background}<path d="M40 40V248H440M40 240L140 200L240 160L340 150L440 100" fill="none" stroke="black" stroke-width="2"/><text x="40" y="25" fill="black">Development effort</text></svg>`;
const transparent = diagram();
const opaque = diagram('<rect width="100%" height="100%" fill="white"/>');
const diagrams: Record<string, string> = {
  "transparent.svg": transparent,
  "opaque.svg": opaque,
  "white.svg": transparent.replaceAll("black", "white"),
  "mixed.svg": diagram().replace(
    "</svg>",
    '<path d="M40 100H440M40 140H440" stroke="white" stroke-width="3"/></svg>',
  ),
  "artwork.svg": `<svg xmlns="http://www.w3.org/2000/svg" width="480" height="288">${["#111", "#fff", "#fae660", "#13385e"].map((fill, i) => `<circle cx="${65 + i * 115}" cy="144" r="45" fill="${fill}"/>`).join("")}</svg>`,
  "cutout.svg":
    '<svg xmlns="http://www.w3.org/2000/svg" width="480" height="288"><defs><linearGradient id="shade"><stop stop-color="#111"/><stop offset="1" stop-color="#444"/></linearGradient></defs><ellipse cx="240" cy="144" rx="200" ry="128" fill="url(#shade)"/></svg>',
  "corners.svg": diagram(
    '<rect width="480" height="288" rx="24" fill="#222"/>',
  ),
  "blank.svg":
    '<svg xmlns="http://www.w3.org/2000/svg" width="480" height="288"/>',
};
const defaultHtml =
  '<figure><img src="/test-images/transparent.svg" alt="Development chart"><figcaption>Development over time.</figcaption></figure>';

async function openArticle(page: Page, html = defaultHtml, automatic = false) {
  if (automatic) {
    await page.addInitScript(() => {
      localStorage.setItem(
        "dank-reader:preferences:v1",
        JSON.stringify({ autoImageBackground: true }),
      );
    });
  }
  const post: Post = {
    id: "image-example",
    domain: "example.com",
    title: "Reading figures",
    url: "https://example.com/image-example",
    author: "Example author",
    excerpt: "Charts should be readable on a dark background.",
    html,
    created_at: "2026-10-15T10:00:00Z",
    source: "rss",
    thumbnail: null,
    media: [],
  };
  const pngs = await page.evaluate(() => {
    const canvas = document.createElement("canvas");
    canvas.width = 480;
    canvas.height = 288;
    const context = canvas.getContext("2d");
    if (!context) throw new Error("Canvas is unavailable");
    context.fillRect(40, 40, 400, 208);
    const transparent = canvas.toDataURL();
    context.fillRect(0, 0, 480, 288);
    const opaque = canvas.toDataURL();
    context.clearRect(0, 0, 480, 288);
    context.lineWidth = 3;
    context.beginPath();
    context.moveTo(40, 40);
    context.lineTo(40, 248);
    context.lineTo(440, 248);
    context.moveTo(40, 240);
    context.lineTo(240, 150);
    context.lineTo(440, 80);
    context.stroke();
    return { transparent, opaque, chart: canvas.toDataURL() };
  });
  await page.context().route("**/test-images/**", (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith("broken.svg")) return route.fulfill({ status: 404 });
    if (path.endsWith(".png")) {
      const data = path.endsWith("/opaque.png")
        ? pngs.opaque
        : path.endsWith("/chart.png")
          ? pngs.chart
          : pngs.transparent;
      return route.fulfill({
        contentType: "image/png",
        body: Buffer.from(data.split(",")[1], "base64"),
      });
    }
    return route.fulfill({
      contentType: "image/svg+xml",
      body: diagrams[path.split("/").at(-1) ?? ""] ?? transparent,
    });
  });
  await page.route("**/api/reader/**", (route) => {
    const path = new URL(route.request().url()).pathname;
    return route.fulfill({
      json: path.endsWith("/sources")
        ? {
            sources: [
              { domain: post.domain, name: "Example", tags: [], count: 1 },
            ],
            total_posts: 1,
          }
        : path.endsWith("/post")
          ? post
          : { posts: [post], next_cursor: null, limited: false },
    });
  });
  await page.goto("/reader/?article=image-example&article_source=example.com");
  await expect(page.locator(".article-body img").first()).toBeAttached();
}

test("transparent SVGs can switch backgrounds on hover and keep full-size links", async ({
  page,
  context,
}) => {
  await openArticle(page);
  const image = page.getByRole("img", { name: "Development chart" });
  const toggle = page.getByRole("button", { name: "Light background" });
  await expect(toggle).toHaveCount(1);
  await expect(toggle).toHaveAttribute("aria-pressed", "false");
  await expect(image).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  await page.mouse.move(0, 0);
  await expect(toggle).toHaveCSS("opacity", "0");
  const bounds = await image.boundingBox();
  await image.hover();
  await expect(toggle).toHaveCSS("opacity", "1");
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-pressed", "true");
  await expect(image).toHaveCSS("background-color", "rgb(255, 255, 255)");
  expect(await image.boundingBox()).toEqual(bounds);
  expect(context.pages()).toHaveLength(1);
  await expect(page.locator("figcaption")).toHaveText("Development over time.");
  await expect(page.locator("a button")).toHaveCount(0);
  await page.mouse.move(0, 0);
  await expect(toggle).toHaveCSS("opacity", "0");
  await image.hover();
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-pressed", "false");
  await expect(image).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  const [tab] = await Promise.all([
    context.waitForEvent("page"),
    image.click(),
  ]);
  await tab.waitForURL("**/test-images/transparent.svg");
  await tab.close();
});

test("keyboard users can discover, toggle and undo the background", async ({
  page,
}) => {
  await openArticle(page);
  const toggle = page.getByRole("button", { name: "Light background" });
  await expect(toggle).toHaveCount(1);
  await page.getByRole("link", { name: "Open full-size image:" }).focus();
  await page.keyboard.press("Tab");
  await expect(toggle).toBeFocused();
  await expect(toggle).toHaveCSS("opacity", "1");
  await page.keyboard.press("Space");
  await expect(toggle).toHaveAttribute("aria-pressed", "true");
  await page.keyboard.press("Enter");
  await expect(toggle).toHaveAttribute("aria-pressed", "false");
  const accessibility = await new AxeBuilder({ page }).analyze();
  expect(
    accessibility.violations.filter((item) =>
      ["serious", "critical"].includes(item.impact ?? ""),
    ),
  ).toEqual([]);
});

test("only inspectable transparent images get controls, including PNGs", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await openArticle(
    page,
    [
      '<img src="/test-images/transparent.svg" alt="Transparent SVG">',
      '<img src="/test-images/opaque.svg" alt="Opaque SVG">',
      '<img src="/test-images/transparent.png" alt="Transparent PNG">',
      '<img src="/test-images/opaque.png" alt="Opaque PNG">',
      '<img src="https://images.example.test/test-images/remote.svg" alt="Remote SVG">',
      '<img src="/test-images/broken.svg" alt="Broken image">',
    ].join(""),
  );
  for (const image of await page.locator(".article-body img").all()) {
    await image.scrollIntoViewIfNeeded();
    await expect
      .poll(() => image.evaluate((node: HTMLImageElement) => node.complete))
      .toBe(true);
  }
  const frames = page.locator(".article-image-frame");
  await expect(frames).toHaveCount(2);
  await expect(frames.nth(0).locator("img")).toHaveAttribute(
    "alt",
    "Transparent SVG",
  );
  await expect(frames.nth(1).locator("img")).toHaveAttribute(
    "alt",
    "Transparent PNG",
  );
  await expect(page.getByRole("img", { name: "Remote SVG" })).toHaveJSProperty(
    "naturalWidth",
    480,
  );
  expect(errors).toEqual([]);
});

test("lazy images are enhanced when loaded and existing links stay intact", async ({
  page,
}) => {
  await openArticle(
    page,
    `${"<p>A long article paragraph.</p>".repeat(150)}<a href="https://example.com/source"><img src="/test-images/transparent.svg" width="480" height="288" alt="Linked chart"></a>`,
  );
  const image = page.getByRole("img", { name: "Linked chart" });
  await expect(image).toHaveAttribute("loading", "lazy");
  await expect(image).toHaveJSProperty("complete", false);
  await expect(
    page.getByRole("button", { name: "Light background" }),
  ).toHaveCount(0);
  await image.scrollIntoViewIfNeeded();
  await expect(
    page.getByRole("button", { name: "Light background" }),
  ).toHaveCount(1);
  await expect(page.locator(".article-image-frame > a")).toHaveAttribute(
    "href",
    "https://example.com/source",
  );
  await image.hover();
  await page.getByRole("button", { name: "Light background" }).click();
  await page.getByRole("button", { name: "Larger article text" }).click();
  await expect(image).toHaveCSS("background-color", "rgb(255, 255, 255)");
});

test("rechecks a changed image source and removes controls when it becomes opaque", async ({
  page,
}) => {
  await openArticle(page);
  const image = page.getByRole("img", { name: "Development chart" });
  const toggle = page.getByRole("button", { name: "Light background" });
  await image.hover();
  await toggle.click();
  await image.evaluate((node: HTMLImageElement) => {
    node.src = "/test-images/opaque.svg";
  });
  await expect(toggle).toHaveCount(0);
  await expect(image).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  await expect(
    page.getByRole("link", { name: "Open full-size image:" }),
  ).toBeVisible();
});

test("automatic backgrounds improve dark charts and leave ambiguous or opaque images alone", async ({
  page,
}) => {
  const cases = [
    ["transparent.svg", true, true],
    ["chart.png", true, true],
    ["white.svg", true, false],
    ["mixed.svg", true, false],
    ["artwork.svg", true, false],
    ["cutout.svg", true, false],
    ["corners.svg", true, false],
    ["blank.svg", true, false],
    ["opaque.svg", false, false],
    ["opaque.png", false, false],
  ] as const;
  await openArticle(
    page,
    cases
      .map(
        ([name]) =>
          `<figure><img src="/test-images/${name}" alt="${name}" width="480" height="288"></figure>`,
      )
      .join(""),
    true,
  );
  for (const [name, transparent, light] of cases) {
    await test.step(name, async () => {
      const image = page.getByRole("img", { name, exact: true });
      await image.scrollIntoViewIfNeeded();
      await image.evaluate((node: HTMLImageElement) => node.decode());
      const toggle = page
        .locator("figure")
        .filter({ has: image })
        .getByRole("button", { name: "Light background" });
      await expect(toggle).toHaveCount(transparent ? 1 : 0);
      if (transparent)
        await expect(toggle).toHaveAttribute("aria-pressed", String(light));
      await expect(image).toHaveCSS(
        "background-color",
        light ? "rgb(255, 255, 255)" : "rgba(0, 0, 0, 0)",
      );
    });
  }
});

test("manual overrides in either direction survive text changes and image load events", async ({
  page,
}) => {
  await openArticle(
    page,
    `${defaultHtml}<figure><img src="/test-images/white.svg" alt="White diagram"></figure>`,
    true,
  );
  for (const [name, initial] of [
    ["Development chart", true],
    ["White diagram", false],
  ] as const) {
    const image = page.getByRole("img", { name });
    const toggle = page
      .locator("figure")
      .filter({ has: image })
      .getByRole("button", { name: "Light background" });
    await image.scrollIntoViewIfNeeded();
    await expect(toggle).toHaveAttribute("aria-pressed", String(initial));
    await image.hover();
    await toggle.click();
    await expect(toggle).toHaveAttribute("aria-pressed", String(!initial));
    await page.getByRole("button", { name: "Larger article text" }).click();
    await image.evaluate((node) => node.dispatchEvent(new Event("load")));
    await expect(toggle).toHaveAttribute("aria-pressed", String(!initial));
    await expect(image).toHaveCSS(
      "background-color",
      initial ? "rgba(0, 0, 0, 0)" : "rgb(255, 255, 255)",
    );
  }
});

test("enabling and disabling the saved setting changes subsequently opened articles", async ({
  page,
}) => {
  await openArticle(page);
  const image = page.getByRole("img", { name: "Development chart" });
  await expect(image).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  for (const enabled of [true, false]) {
    await page.getByRole("button", { name: "Settings", exact: true }).click();
    const automatic = page.getByRole("checkbox", {
      name: "Automatically improve image backgrounds",
    });
    await automatic.setChecked(enabled);
    await page.reload();
    if (enabled) await expect(automatic).toBeChecked();
    else await expect(automatic).not.toBeChecked();
    await page.getByRole("button", { name: "All", exact: true }).click();
    await page
      .getByRole("link", { name: "Read Reading figures", exact: true })
      .click();
    await expect(
      page.getByRole("button", { name: "Light background" }),
    ).toHaveAttribute("aria-pressed", String(enabled));
    await expect(image).toHaveCSS(
      "background-color",
      enabled ? "rgb(255, 255, 255)" : "rgba(0, 0, 0, 0)",
    );
  }
});

test.describe("touch screens", () => {
  test.use({
    viewport: { width: 390, height: 844 },
    hasTouch: true,
    isMobile: true,
  });

  test("the toggle is discoverable without hover and does not open the image", async ({
    page,
    context,
  }) => {
    await openArticle(page);
    const image = page.getByRole("img", { name: "Development chart" });
    const toggle = page.getByRole("button", { name: "Light background" });
    await expect(toggle).toBeVisible();
    await expect(toggle).toHaveCSS("opacity", "1");
    const bounds = await toggle.boundingBox();
    expect(bounds?.height).toBeGreaterThanOrEqual(44);
    expect(bounds?.width).toBeGreaterThanOrEqual(44);
    const imageBounds = await image.boundingBox();
    expect(
      bounds && imageBounds && bounds.y >= imageBounds.y + imageBounds.height,
    ).toBe(true);
    await toggle.tap();
    await expect(image).toHaveCSS("background-color", "rgb(255, 255, 255)");
    expect(context.pages()).toHaveLength(1);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await toggle.tap();
    await expect(image).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
    const [tab] = await Promise.all([
      context.waitForEvent("page"),
      image.tap(),
    ]);
    await tab.waitForURL("**/test-images/transparent.svg");
    await tab.close();
  });
});
