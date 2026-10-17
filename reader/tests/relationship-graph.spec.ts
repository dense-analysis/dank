import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import type { Core } from "cytoscape";
import { referenceGraphNodeId } from "../src/lib/youtubeReferenceGraph";

for (const width of [1210, 720, 390, 320]) {
  test(`verified relationship graph is readable and navigable at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 958 });
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(error.message));
    await page.goto("/reader/tests/fixtures/relationship-graph.html");
    const graph = page.locator(".relationship-graph");
    await expect(graph).toHaveAttribute("data-status", "ready");
    await expect(graph.locator("canvas")).toHaveCount(3);
    await expect(
      page.getByText("2 discussions across 2 sources link to this video."),
    ).toBeVisible();
    const selector = page.getByLabel("Explore nodes");
    await selector.selectOption(
      referenceGraphNodeId(
        "comment",
        "reddit.com",
        "reddit-discussion",
        "ph4vac2",
      ),
    );
    const details = page.getByRole("region", { name: "Selected connection" });
    await expect(
      details.getByRole("heading", { name: "James20k" }),
    ).toBeVisible();
    await expect(details).toContainText("seen it happen in real time");
    await details.getByRole("link", { name: "Open comment in reader" }).click();
    await expect(page.getByLabel("Opened reference")).toHaveText(
      "reddit.com:ph4vac2",
    );
    await expect(
      details.getByRole("link", { name: "Original comment" }),
    ).toHaveAttribute("href", /comment\/ph4vac2/);
    await page.getByRole("button", { name: "Zoom in", exact: true }).click();
    await page.getByRole("button", { name: "Zoom out", exact: true }).click();
    await page.getByRole("button", { name: "Fit", exact: true }).click();
    await page.getByRole("button", { name: "Update metadata" }).click();
    await expect(selector).toHaveValue(
      referenceGraphNodeId(
        "comment",
        "reddit.com",
        "reddit-discussion",
        "ph4vac2",
      ),
    );
    await selector.selectOption(
      referenceGraphNodeId("video", "video:NnV_cWeoo5Q"),
    );
    await expect(
      details.getByRole("heading", { name: "Updated YouTube title" }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Reset", exact: true }).click();
    await expect(selector).toHaveValue("");
    const viewport = page.getByRole("application", {
      name: "Interactive relationship graph",
    });
    await viewport.focus();
    await page.keyboard.press("ArrowRight");
    await page.keyboard.press("+");
    await page.keyboard.press("Home");
    await expect(viewport).toBeFocused();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    const audit = await new AxeBuilder({ page })
      .include(".yt-reference-graph")
      .analyze();
    expect(audit.violations).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath(`reference-graph-${width}.png`),
      fullPage: true,
    });
    await page.getByRole("button", { name: "Toggle graph" }).click();
    await expect(graph).toHaveCount(0);
    await page.getByRole("button", { name: "Toggle graph" }).click();
    await expect(graph).toHaveAttribute("data-status", "ready");
    expect(errors).toEqual([]);
  });
}

test("metadata enrichment preserves the panned and zoomed graph", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1210, height: 958 });
  await page.goto("/reader/tests/fixtures/relationship-graph.html");
  await expect(page.locator(".relationship-graph")).toHaveAttribute(
    "data-status",
    "ready",
  );
  await page.evaluate(() => document.fonts.ready);
  const viewport = page.getByRole("application", {
    name: "Interactive relationship graph",
  });
  // Read Cytoscape's public state through its canvas registry; texture caching
  // makes pixel equality an unreliable assertion for viewport preservation.
  const renderer = await page
    .locator(".relationship-graph-canvas")
    .evaluateHandle(
      (element) =>
        (element as HTMLDivElement & { _cyreg: { cy: Core } })._cyreg.cy,
    );
  const position = () =>
    renderer.evaluate((cy) => ({
      zoom: cy.zoom(),
      pan: cy.pan(),
      nodes: cy
        .nodes()
        .map((node) => ({ id: node.id(), position: node.position() })),
    }));
  const initial = await position();
  await viewport.focus();
  await page.keyboard.press("ArrowRight");
  await page.keyboard.press("+");
  const panned = await position();
  expect(panned).not.toEqual(initial);
  await page.getByRole("button", { name: "Update metadata" }).click();
  await expect(
    page
      .getByLabel("Explore nodes")
      .getByRole("option", { name: "Video: Updated YouTube title" }),
  ).toHaveCount(1);
  expect(await position()).toEqual(panned);
  await page.getByRole("button", { name: "Toggle graph" }).click();
  expect(await renderer.evaluate((cy) => cy.destroyed())).toBe(true);
  await renderer.dispose();
});

test("graph loading failures retain keyboard access to the saved evidence", async ({
  page,
}) => {
  await page.route("**/node_modules/.vite/deps/cytoscape.js*", (route) =>
    route.abort(),
  );
  await page.goto("/reader/tests/fixtures/relationship-graph.html");
  await expect(page.locator(".relationship-graph")).toHaveAttribute(
    "data-status",
    "failed",
  );
  await expect(
    page.locator(".relationship-graph").getByRole("status"),
  ).toContainText("Graph could not load");
  await page
    .getByLabel("Explore nodes")
    .selectOption(
      referenceGraphNodeId(
        "comment",
        "reddit.com",
        "reddit-discussion",
        "ph4vac2",
      ),
    );
  await page.getByRole("link", { name: "Open comment in reader" }).click();
  await expect(page.getByLabel("Opened reference")).toHaveText(
    "reddit.com:ph4vac2",
  );
});
