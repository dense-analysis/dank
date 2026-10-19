import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, test } from "@playwright/test";
import type { Core } from "cytoscape";
import {
  connectionsFixture,
  outgoingConnection,
  redditConnectionsFixture,
} from "../src/lib/postConnections.fixture";
import { connectionNodeId } from "../src/lib/postConnectionsGraph";

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

test("a crowded mobile graph opens at readable scale and reveals selected destinations without overriding history", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 390, height: 958 });
  const outgoing = Array.from({ length: 8 }, (_, index) => {
    const url = `https://destination-${index}.example/research`;
    const post =
      index < 2
        ? {
            domain: `destination-${index}.example`,
            id: `post-${index}`,
            url,
            title: `Collected research ${index}`,
            source: "rss",
          }
        : null;

    return {
      ...outgoingConnection,
      id: `outgoing-${index}`,
      target: { url, post, youtube_item_id: null },
      evidence: {
        ...outgoingConnection.evidence,
        kind: index < 4 ? ("post" as const) : ("comment" as const),
        reference_id: index < 4 ? "" : `comment-${index}`,
        author: index < 4 ? "" : `Reader ${index}`,
        matched_url: url,
      },
    };
  });
  await fixture(page, {
    ...connectionsFixture,
    incoming: [],
    incoming_count: 0,
    outgoing,
    outgoing_count: outgoing.length,
  });
  await page.getByRole("button", { name: "Graph view" }).click();
  const graph = page.locator(".relationship-graph");
  await expect(graph).toHaveAttribute("data-status", "ready");
  const renderer = () =>
    page
      .locator(".relationship-graph-canvas")
      .evaluateHandle(
        (element) =>
          (element as HTMLDivElement & { _cyreg: { cy: Core } })._cyreg.cy,
      );
  const cy = await renderer();
  const rootId = connectionNodeId(
    "post",
    connectionsFixture.root.domain,
    connectionsFixture.root.id,
  );
  const lastId = connectionNodeId("url", outgoing[7].target.url);
  const bounds = (instance: Core, id: string) => {
    const box = instance.getElementById(id).renderedBoundingBox();

    return {
      zoom: instance.zoom(),
      width: instance.width(),
      height: instance.height(),
      box,
      nodes: instance.nodes().length,
    };
  };
  const initial = await cy.evaluate(bounds, rootId);
  expect(initial.nodes).toBe(16);
  expect(initial.zoom).toBeGreaterThanOrEqual(0.75);
  expect(initial.box.w).toBeGreaterThan(130);
  expect(initial.box.x1).toBeGreaterThan(0);
  expect(initial.box.x2).toBeLessThan(initial.width);
  expect(initial.box.y1).toBeGreaterThan(0);
  expect(initial.box.y2).toBeLessThan(initial.height);
  const offscreen = await cy.evaluate(bounds, lastId);
  expect(
    offscreen.box.x2 > offscreen.width || offscreen.box.y2 > offscreen.height,
  ).toBe(true);

  await page.getByRole("button", { name: "Fit", exact: true }).click();
  expect(await cy.evaluate((instance) => instance.zoom())).toBeLessThan(0.75);
  await page.getByLabel("Explore nodes").selectOption(lastId);
  const selected = await cy.evaluate(bounds, lastId);
  expect(selected.zoom).toBeGreaterThanOrEqual(0.75);
  expect(selected.box.x1).toBeGreaterThan(0);
  expect(selected.box.x2).toBeLessThan(selected.width);
  expect(selected.box.y1).toBeGreaterThan(0);
  expect(selected.box.y2).toBeLessThan(selected.height);

  // Deliberately leave the selected node outside a zoomed-out viewport: returning
  // must restore user navigation rather than refocusing an existing selection.
  await cy.evaluate((instance) => {
    instance.zoom(0.3);
    instance.pan({ x: 37, y: 51 });
  });
  const snapshot = (instance: Core) => ({
    zoom: instance.zoom(),
    pan: instance.pan(),
  });
  const remembered = await cy.evaluate(snapshot);
  await page.getByRole("button", { name: "Toggle connections" }).click();
  await page.getByRole("button", { name: "Toggle connections" }).click();
  await expect(graph).toHaveAttribute("data-status", "ready");
  await expect(page.getByLabel("Explore nodes")).toHaveValue(lastId);
  const restored = await renderer();
  expect(await restored.evaluate(snapshot)).toEqual(remembered);

  await page.getByRole("button", { name: "Reset", exact: true }).click();
  const reset = await restored.evaluate(bounds, rootId);
  expect(reset.zoom).toBeGreaterThanOrEqual(0.75);
  expect(reset.box.x1).toBeGreaterThan(0);
  expect(reset.box.x2).toBeLessThan(reset.width);
  await page.locator(".relationship-graph-viewport").scrollIntoViewIfNeeded();
  await page.screenshot({
    path: testInfo.outputPath("readable-mobile-graph.png"),
  });
  await cy.dispose();
  await restored.dispose();
});

for (const width of [1210, 390]) {
  test(`contextual Reddit graph keeps a publisher-first layout with exact evidence at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 958 });
    const requests: string[] = [];
    await page.route("**/api/reader/connections?*", (route) => {
      requests.push(route.request().url());
      return route.fulfill({ json: redditConnectionsFixture });
    });
    await page.goto("/reader/tests/fixtures/post-connections.html?reddit");
    await page.getByRole("button", { name: "Graph view" }).click();
    const graph = page.locator(".relationship-graph");
    await expect(graph).toHaveAttribute("data-status", "ready");
    expect(requests[0]).toContain("source=reddit&community=kernel");
    await expect(page.getByText(/r\/kernel: saved links/)).toBeVisible();
    await expect(page.getByText(/This layout is not a timeline/)).toBeVisible();
    const selector = page.getByLabel("Explore nodes");
    await expect(
      selector.getByRole("option", { name: "Source: r/kernel" }),
    ).toHaveCount(1);
    const cy = await page
      .locator(".relationship-graph-canvas")
      .evaluateHandle(
        (element) =>
          (element as HTMLDivElement & { _cyreg: { cy: Core } })._cyreg.cy,
      );
    const layout = await cy.evaluate((instance) =>
      instance.nodes().map((node) => ({
        kind: node.data("kind"),
        point: node.position(),
        label: node.data("label"),
      })),
    );
    const axis = width < 760 ? "y" : "x";
    const article = layout.find((node) => node.kind === "article");
    const source = layout.find((node) => node.kind === "source");
    const root = layout.find((node) => node.kind === "root");
    expect(article?.point[axis]).toBeLessThan(source?.point[axis] ?? 0);
    expect(source?.point[axis]).toBeLessThan(root?.point[axis] ?? 0);
    await selector.selectOption(
      connectionNodeId("post", "reddit.com", "second-discussion"),
    );
    await expect(
      page.getByRole("region", { name: "Selected connection" }),
    ).toContainText("Shared article · Post link");
    await page.getByRole("button", { name: "List view" }).click();
    const related = page.getByRole("region", {
      name: "Other posts linking to the same article",
    });
    await expect(related).toContainText("A second saved post");
    await related.getByRole("link", { name: "Explore this post" }).click();
    await expect(page.getByLabel("Opened destination")).toHaveText(
      "explore:reddit.com:second-discussion",
    );
    await page.getByRole("button", { name: "Graph view" }).click();
    await expect(graph).toHaveAttribute("data-status", "ready");
    await page.getByRole("button", { name: "Fit", exact: true }).click();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect(
      (await new AxeBuilder({ page }).include(".post-connections").analyze())
        .violations,
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath(`reddit-context-${width}.png`),
      fullPage: true,
    });
    await cy.dispose();
  });
}

for (const width of [1210, 390]) {
  for (const throughReddit of [false, true]) {
    test(`graph shows bounded ${throughReddit ? "Reddit" : "RSS"} first discovery at ${width}px`, async ({
      page,
    }, testInfo) => {
      await page.setViewportSize({ width, height: 958 });
      await page.route("**/api/reader/connections?*", (route) =>
        route.fulfill({ json: redditConnectionsFixture }),
      );
      await page.goto(
        `/reader/tests/fixtures/post-connections.html?reddit&discovery=${throughReddit ? "reddit" : "rss"}`,
      );
      await page.getByRole("button", { name: "Graph view" }).click();
      await expect(page.locator(".relationship-graph")).toHaveAttribute(
        "data-status",
        "ready",
      );
      const cy = await page
        .locator(".relationship-graph-canvas")
        .evaluateHandle(
          (element) =>
            (element as HTMLDivElement & { _cyreg: { cy: Core } })._cyreg.cy,
        );
      const snapshot = await cy.evaluate((instance) => ({
        nodes: instance.nodes().map((node) => node.data()),
        discoveryEdges: instance.edges('[kind="discovery"]').map((edge) => ({
          style: edge.style("line-style"),
          arrow: edge.style("target-arrow-shape"),
        })),
      }));
      const discovery = snapshot.nodes.find(
        (node) => node.kind === "discovery",
      );
      expect(discovery?.label).toBe(throughReddit ? "r/kernel" : "example.com");
      expect(discovery?.displayLabel).toContain("2026");
      expect(discovery?.displayLabel).toContain(throughReddit ? "17" : "16");
      expect(discovery?.displayLabel).not.toContain("19");
      expect(snapshot.discoveryEdges).toEqual([
        { style: "dashed", arrow: "none" },
      ]);
      expect(
        snapshot.nodes
          .filter((node) => node.kind === "source")
          .map((node) => node.label),
      ).toEqual(["r/kernel"]);
      expect(snapshot.nodes).toHaveLength(5);
      const root = snapshot.nodes.find((node) => node.kind === "root");
      expect(root?.displayLabel).toContain("Published");
      expect(root?.displayLabel).toContain(
        throughReddit
          ? "2h before first discovery"
          : "1d 1h after first discovery",
      );
      await page.getByLabel("Explore nodes").selectOption(root.id);
      await expect(
        page.getByRole("region", { name: "Selected connection" }),
      ).toContainText(
        throughReddit
          ? "2h before first discovery"
          : "1d 1h after first discovery",
      );
      await expect(
        page
          .getByRole("region", { name: "Selected connection" })
          .locator("time")
          .first(),
      ).toHaveAttribute("datetime", "2026-10-17T10:00:00Z");
      await expect(
        page.getByText(/0 incoming · 1 outgoing references/),
      ).toContainText("1 shared-article references");
      await page.getByLabel("Explore nodes").selectOption(discovery.id);
      const details = page.getByRole("region", { name: "Selected connection" });
      await expect(details).toContainText("First observed by DANK");
      await expect(details.locator("time").first()).toHaveAttribute(
        "datetime",
        throughReddit ? "2026-10-17T12:00:00Z" : "2026-10-16T09:00:00Z",
      );
      await expect(
        details.getByText("This post is among the first recorded discoveries."),
      ).toHaveCount(throughReddit ? 1 : 0);
      await expect(details.locator("article")).toHaveCount(0);
      await expect(
        details.getByRole("link", { name: "Explore this post" }),
      ).toHaveCount(0);
      expect(
        (await new AxeBuilder({ page }).include(".post-connections").analyze())
          .violations,
      ).toEqual([]);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
      await page.getByRole("button", { name: "Fit", exact: true }).click();
      await page
        .locator(".relationship-graph-viewport")
        .screenshot({ path: testInfo.outputPath("first-discovery.png") });
      await page
        .getByRole("button", { name: "Incoming links", exact: true })
        .click();
      await expect(
        page
          .getByLabel("Explore nodes")
          .getByRole("option", { name: /First discovered/ }),
      ).toHaveCount(0);
      await cy.dispose();
    });
  }
}

test("graph keeps missing publication and gaps explicit without using collection times", async ({
  page,
}) => {
  await page.route("**/api/reader/connections?*", (route) =>
    route.fulfill({ json: redditConnectionsFixture }),
  );
  await page.goto(
    "/reader/tests/fixtures/post-connections.html?reddit&discovery=unknown",
  );
  await page.getByRole("button", { name: "Graph view" }).click();
  await expect(page.locator(".relationship-graph")).toHaveAttribute(
    "data-status",
    "ready",
  );
  const rootId = connectionNodeId(
    "post",
    redditConnectionsFixture.root.domain,
    redditConnectionsFixture.root.id,
  );
  await page.getByLabel("Explore nodes").selectOption(rootId);
  const details = page.getByRole("region", { name: "Selected connection" });
  await expect(details).toContainText("Publication time unavailable");
  await expect(details).toContainText("Gap unavailable");
  await expect(
    details.locator(".post-connections-publication time"),
  ).toHaveCount(0);
});

test("exploring a publisher keeps its known publication time in discovery details", async ({
  page,
}) => {
  await page.route("**/api/reader/connections?*", (route) =>
    route.fulfill({ json: connectionsFixture }),
  );
  await page.goto("/reader/tests/fixtures/post-connections.html?discovery=rss");
  await page.getByRole("button", { name: "Graph view" }).click();
  await expect(page.locator(".relationship-graph")).toHaveAttribute(
    "data-status",
    "ready",
  );
  const rootId = connectionNodeId(
    "post",
    connectionsFixture.root.domain,
    connectionsFixture.root.id,
  );
  await page
    .getByLabel("Explore nodes")
    .selectOption(connectionNodeId("discovery", rootId));
  const details = page.getByRole("region", { name: "Selected connection" });
  await expect(
    details.locator(".post-connections-publication time"),
  ).toHaveAttribute("datetime", "2026-10-17T10:00:00Z");
});
