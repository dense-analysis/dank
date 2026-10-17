import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Core } from "cytoscape";
import { connectionNodeId } from "../src/lib/postConnectionsGraph";
import { connectionsFixture } from "./fixtures/post-connections-data";

async function fixture(page: Page, data = connectionsFixture) {
  const requests: string[] = [];
  await page.route("**/api/reader/connections?*", (route) => {
    requests.push(route.request().url());

    return route.fulfill({ json: data });
  });
  await page.goto("/reader/tests/fixtures/post-connections.html");

  return requests;
}

test("loads on demand, separates evidence from exploration and labels unknown destinations", async ({
  page,
}) => {
  const requests = await fixture(page);
  await expect(
    page.getByRole("button", { name: "Connections", exact: true }),
  ).toHaveAttribute("aria-expanded", "false");
  expect(requests).toEqual([]);
  await page.getByRole("button", { name: "Connections", exact: true }).click();
  await expect(
    page.getByText("1 incoming · 3 outgoing references"),
  ).toBeVisible();
  expect(requests).toHaveLength(1);
  const incoming = page.getByRole("region", { name: "Links to this post" });
  await expect(incoming).toContainText(
    "Incoming · Comment mention · KernelReader",
  );
  await expect(incoming).toContainText("kernel security matters");
  const open = incoming.getByRole("link", { name: "Open comment in DANK" });
  await expect(open).toHaveAttribute(
    "href",
    /reference_id=comment-1.*reference_url=.*%23security/,
  );
  await open.click();
  await expect(page.getByLabel("Opened destination")).toHaveText(
    "evidence:reddit.com:discussion:comment-1",
  );
  await incoming.getByRole("link", { name: "Explore this post" }).click();
  await expect(page.getByLabel("Opened destination")).toHaveText(
    "explore:reddit.com:discussion",
  );
  const outgoing = page.getByRole("region", { name: "Links from this post" });
  await outgoing.getByRole("link", { name: "Explore this post" }).click();
  await expect(page.getByLabel("Opened destination")).toHaveText(
    "explore:source.example:research",
  );
  await outgoing.getByRole("link", { name: "Explore video in DANK" }).click();
  await expect(page.getByLabel("Opened destination")).toHaveText(
    "explore:youtube:video:NnV_cWeoo5Q",
  );
  const external = outgoing
    .locator("article")
    .filter({ hasText: "Destination not collected" });
  await expect(
    external.getByRole("link", { name: "Explore this post" }),
  ).toHaveCount(0);
  await expect(
    external.getByRole("link", { name: "Open comment in DANK" }),
  ).toBeVisible();
  await expect(
    external.getByRole("link", { name: "Linked destination" }),
  ).toHaveAttribute("href", /#section$/);
});

for (const width of [1210, 720, 390, 320]) {
  test(`post connection graph supports exact evidence, directions and accessible mobile layout at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 958 });
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(error.message));
    await fixture(page);
    await page.getByRole("button", { name: "Graph view" }).click();
    const graph = page.locator(".relationship-graph");
    await expect(graph).toHaveAttribute("data-status", "ready");
    const selector = page.getByLabel("Explore nodes");
    await selector.selectOption(
      connectionNodeId("comment", "reddit.com", "discussion", "comment-1"),
    );
    const details = page.getByRole("region", { name: "Selected connection" });
    await expect(
      details.getByRole("heading", { name: "KernelReader" }),
    ).toBeVisible();
    await expect(details.locator("article")).toHaveCount(1);
    await details.getByRole("link", { name: "Open comment in DANK" }).click();
    await expect(page.getByLabel("Opened destination")).toHaveText(
      "evidence:reddit.com:discussion:comment-1",
    );
    await page
      .getByRole("button", { name: "Outgoing links", exact: true })
      .click();
    await expect(graph).toHaveAttribute("data-status", "ready");
    await expect(
      selector.getByRole("option", { name: "Comment: KernelReader" }),
    ).toHaveCount(0);
    await selector.selectOption(
      connectionNodeId("post", "source.example", "research"),
    );
    await expect(
      details
        .getByRole("heading", { name: "The original security research" })
        .first(),
    ).toBeVisible();
    await details
      .getByRole("link", { name: "Explore this post" })
      .first()
      .click();
    await expect(page.getByLabel("Opened destination")).toHaveText(
      "explore:source.example:research",
    );
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    const audit = await new AxeBuilder({ page })
      .include(".post-connections")
      .analyze();
    expect(audit.violations).toEqual([]);
    if (width <= 800) {
      for (const control of await page
        .locator(
          ".post-connections-modes button, .post-connections-directions button, .post-connection-actions a",
        )
        .all()) {
        expect((await control.boundingBox())?.height).toBeGreaterThanOrEqual(
          44,
        );
      }
    }
    await page.screenshot({
      path: testInfo.outputPath(`post-connections-${width}.png`),
      fullPage: true,
    });
    expect(errors).toEqual([]);
  });
}

test("returning to a filtered graph retains selected evidence, pan, zoom and moved nodes", async ({
  page,
}) => {
  await fixture(page);
  await page.getByRole("button", { name: "Graph view" }).click();
  await page
    .getByRole("button", { name: "Incoming links", exact: true })
    .click();
  const graph = page.locator(".relationship-graph");
  await expect(graph).toHaveAttribute("data-status", "ready");
  const selected = connectionNodeId(
    "comment",
    "reddit.com",
    "discussion",
    "comment-1",
  );
  await page.getByLabel("Explore nodes").selectOption(selected);
  const renderer = await page
    .locator(".relationship-graph-canvas")
    .evaluateHandle(
      (element) =>
        (element as HTMLDivElement & { _cyreg: { cy: Core } })._cyreg.cy,
    );
  await renderer.evaluate((cy) => {
    cy.nodes()[0].position({ x: -80, y: -90 });
    cy.zoom(1.2);
    cy.pan({ x: 170, y: 240 });
  });
  const snapshot = (cy: Core) => ({
    zoom: cy.zoom(),
    pan: cy.pan(),
    positions: cy
      .nodes()
      .map((node) => ({ id: node.id(), position: node.position() })),
  });
  const before = await renderer.evaluate(snapshot);
  await page.getByRole("button", { name: "Toggle connections" }).click();
  await expect(graph).toHaveCount(0);
  await page.getByRole("button", { name: "Toggle connections" }).click();
  await expect(graph).toHaveAttribute("data-status", "ready");
  await expect(
    page.getByRole("button", { name: "Incoming links", exact: true }),
  ).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByLabel("Explore nodes")).toHaveValue(selected);
  const restored = await page
    .locator(".relationship-graph-canvas")
    .evaluateHandle(
      (element) =>
        (element as HTMLDivElement & { _cyreg: { cy: Core } })._cyreg.cy,
    );
  expect(await restored.evaluate(snapshot)).toEqual(before);
  await renderer.dispose();
  await restored.dispose();
});

test("reports truncated coverage and avoids pretending to have the full graph", async ({
  page,
}) => {
  await fixture(page, {
    ...connectionsFixture,
    incoming_count: 150,
    outgoing_count: 250,
  });
  await page.getByRole("button", { name: "Graph view" }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "Showing 1 of 150" }),
  ).toContainText("3 of 250 outgoing references");
  await expect(
    page.getByText(/missing captures may hide connections/),
  ).toBeVisible();
});

test("failed connection requests show a recoverable error and empty evidence stays explicit", async ({
  page,
}) => {
  let fails = true;
  await page.route("**/api/reader/connections?*", (route) =>
    fails
      ? route.fulfill({
          status: 503,
          json: { detail: "Connection index is unavailable." },
        })
      : route.fulfill({
          json: {
            ...connectionsFixture,
            incoming: [],
            outgoing: [],
            incoming_count: 0,
            outgoing_count: 0,
          },
        }),
  );
  await page.goto("/reader/tests/fixtures/post-connections.html");
  await page.getByRole("button", { name: "Connections", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText(
    "Connection index is unavailable.",
  );
  fails = false;
  await page.getByRole("button", { name: "Try again" }).click();
  await expect(
    page.getByText(
      "No verified links found in the saved evidence for this post.",
    ),
  ).toBeVisible();
  await expect(page.locator(".relationship-graph")).toHaveCount(0);
});

test("unsafe saved URLs remain text and do not create active links", async ({
  page,
}) => {
  const data = structuredClone(connectionsFixture);
  data.incoming[0].evidence.reference_url = "javascript:alert(1)";
  data.incoming[0].evidence.matched_url = "javascript:alert(1)";
  await fixture(page, data);
  await page.getByRole("button", { name: "Connections", exact: true }).click();
  const incoming = page.getByRole("region", { name: "Links to this post" });
  await expect(
    incoming.getByRole("link", { name: "Original comment" }),
  ).toHaveCount(0);
  await expect(
    incoming.getByRole("link", { name: "Linked destination" }),
  ).toHaveCount(0);
  await expect(
    incoming.getByRole("link", { name: "Open comment in DANK" }),
  ).toBeVisible();
});
