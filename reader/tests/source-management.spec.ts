import { expect, type Page, test } from "@playwright/test";
import {
  feedSource,
  refreshJob,
  xAccount,
  xSource,
} from "../src/lib/api.fixture";
import type { SourceDraft } from "../src/lib/sourceSettings";
import type { SavedSource } from "../src/lib/types";
import { apiPath, fulfillEmpty } from "./apiMock";

function saved(id: number, fields: Partial<SourceDraft>): SavedSource {
  return {
    id: `00000000-0000-0000-0000-00000000000${id}`,
    enabled: true,
    domain: "example.com",
    accounts: [],
    feeds: [],
    tags: [],
    adapters: null,
    reddit: null,
    hacker_news: null,
    ...fields,
  };
}

const on = (value: string) => ({ value, enabled: true });

interface Server {
  sources: SavedSource[];
  writes: { method: string; path: string; body: SourceDraft }[];
  refreshes: unknown[];
  /** Answers the next writes with these statuses instead of saving. */
  failing: number[];
  refreshFails: boolean;
  inDatabase: boolean;
}

/** A server that saves sources the way DANK does, normalizing as it goes. */
async function serve(
  page: Page,
  sources: SavedSource[],
  options: Partial<Server> = {},
): Promise<Server> {
  const server: Server = {
    sources,
    writes: [],
    refreshes: [],
    failing: [],
    refreshFails: false,
    inDatabase: true,
    ...options,
  };

  function refuse(status: number, code: string, fields: unknown[] = []) {
    return {
      status,
      json: { error: `Refused: ${code}`, code, fields },
    };
  }

  await page.route("**/api/reader/**", (route) => {
    const path = apiPath(route);
    const method = route.request().method();

    if (path === "/api/reader/sources") {
      return route.fulfill({
        json: {
          sources: [
            xSource({
              count: 3,
              accounts: [
                xAccount({ handle: "alice", name: "Alice", count: 3 }),
              ],
            }),
            feedSource({
              domain: "found.example",
              name: "found.example",
              count: 2,
              configured: false,
            }),
          ],
          total_posts: 5,
          blog_external_count: 0,
          in_database: server.inDatabase,
        },
      });
    }

    if (path === "/api/reader/refresh" && method === "POST") {
      server.refreshes.push(route.request().postDataJSON().scope);

      return server.refreshFails
        ? route.fulfill(refuse(400, "invalid_request"))
        : route.fulfill({ status: 202, json: { job: refreshJob() } });
    }

    if (!path.startsWith("/api/reader/source-settings")) {
      return fulfillEmpty(route);
    }

    if (method === "GET") {
      return route.fulfill({
        json: { sources: server.sources, in_database: server.inDatabase },
      });
    }

    const body: SourceDraft = route.request().postDataJSON();
    server.writes.push({ method, path, body });
    const failure = server.failing.shift();

    if (failure) return route.fulfill(refuse(failure, "unavailable"));

    if (body.domain === "reddit.com" && !body.feeds.length) {
      return route.fulfill(
        refuse(400, "invalid_source", [
          {
            path: "feeds",
            code: "invalid_platform_options",
            message: "Add at least one community.",
          },
        ]),
      );
    }

    const bad = body.feeds.findIndex((feed) => feed.value.includes("!"));

    if (bad >= 0) {
      return route.fulfill(
        refuse(400, "invalid_source", [
          {
            path: `feeds.${bad}.value`,
            code: "invalid_url",
            message: "Use r/community or a Reddit community URL.",
          },
        ]),
      );
    }

    const normal: SourceDraft = {
      ...body,
      feeds: body.feeds.map((feed) => ({
        ...feed,
        value:
          body.domain === "reddit.com" && feed.value.startsWith("r/")
            ? `https://www.reddit.com/${feed.value}/`
            : feed.value,
      })),
    };
    const id = path.split("/")[4];
    const settings = (source: SourceDraft) =>
      JSON.stringify([
        source.domain,
        source.accounts.map((target) => target.value),
        source.feeds.map((target) => target.value),
        source.tags,
      ]);

    if (
      server.sources.some(
        (source) => source.id !== id && settings(source) === settings(normal),
      )
    ) {
      return route.fulfill(refuse(409, "duplicate_source"));
    }

    if (method === "POST") {
      const source = {
        ...normal,
        id: `00000000-0000-0000-0000-0000000000${server.sources.length + 11}`,
      };
      server.sources = [...server.sources, source];

      return route.fulfill({ status: 201, json: source });
    }

    const source = { ...normal, id };
    server.sources = server.sources.map((item) =>
      item.id === id ? source : item,
    );

    return route.fulfill({ json: source });
  });

  return server;
}

const editor = (page: Page) => page.getByRole("form");

test("adds a Reddit source and shows it saved, normalized", async ({
  page,
}) => {
  const server = await serve(page, []);
  await page.goto("/?view=sources&manage=1");

  await expect(
    page.getByRole("heading", { name: "No sources yet" }),
  ).toBeVisible();
  // With nothing saved there is nothing to search or filter.
  await expect(
    page.getByRole("searchbox", { name: "Search saved sources" }),
  ).toHaveCount(0);
  await page.getByRole("button", { name: "More options" }).click();
  await page.getByRole("button", { name: "Reddit" }).click();
  await expect(page).toHaveURL(/source=new&platform=reddit/);
  // The form stands alone: the list's heading and tabs give way to it.
  await expect(
    page.getByRole("heading", { level: 1, name: "Add a Reddit source" }),
  ).toBeVisible();
  await expect(page.locator(".sources-tabs")).toHaveCount(0);
  await expect(page.getByLabel("Collect this source")).toHaveCount(0);

  await page.getByLabel("Add a community").fill("r/LocalLLaMA r/rust");
  await page.getByLabel("Add a community").press("Enter");
  await page.getByLabel("Sorted by").selectOption("hot");
  await page.getByLabel("Tags").fill("AI, news");
  await page.getByRole("button", { name: "Save", exact: true }).click();

  // Saving returns to the list, which names the new source.
  await expect(page.getByRole("status")).toHaveText(
    "Added r/LocalLLaMA, r/rust.",
  );
  await expect(page).not.toHaveURL(/source=/);
  expect(server.writes).toHaveLength(1);
  expect(server.writes[0]).toMatchObject({
    method: "POST",
    body: {
      enabled: true,
      domain: "reddit.com",
      feeds: [on("r/LocalLLaMA"), on("r/rust")],
      tags: ["ai", "news"],
      reddit: { sort: "hot", time_filter: "day" },
    },
  });
  expect(server.writes[0].body).not.toHaveProperty("id");

  // The row is titled by its communities, in the server's spelling.
  await page
    .getByRole("link", { name: /r\/LocalLLaMA, r\/rust.*Reddit · hot/ })
    .click();
  await expect(page).toHaveURL(/source=00000000-0000-0000-0000-000000000011/);
  await expect(
    page.getByRole("checkbox", { name: "r/LocalLLaMA", exact: true }),
  ).toBeChecked();
});

test("puts each refused field's message beside it and keeps the edits", async ({
  page,
}) => {
  const server = await serve(page, []);
  await page.goto("/?view=sources&source=new&platform=reddit");

  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(
    editor(page).getByText("Add at least one community."),
  ).toBeVisible();

  await page.getByLabel("Add a community").fill("r/ok r/bad!");
  await page.getByLabel("Add a community").press("Enter");
  await page.getByRole("button", { name: "Save", exact: true }).click();

  await expect(
    page.getByRole("checkbox", { name: "r/bad!", exact: true }),
  ).toHaveAttribute("aria-invalid", "true");
  await expect(
    editor(page).getByText("Use r/community or a Reddit community URL."),
  ).toBeVisible();
  await expect(
    page.getByRole("checkbox", { name: "r/ok", exact: true }),
  ).toBeChecked();
  expect(server.sources).toHaveLength(0);
});

test("edits a source by sending all of it, and switches an account off", async ({
  page,
}) => {
  const server = await serve(page, [
    saved(1, {
      domain: "x.com",
      accounts: [on("alice"), on("bob")],
      tags: ["ai"],
    }),
  ]);
  await page.goto("/?view=sources&manage=1");
  await page.getByRole("link", { name: /@alice, @bob/ }).click();

  await expect(
    page.getByRole("button", { name: "Save", exact: true }),
  ).toBeDisabled();
  await page.getByRole("checkbox", { name: "@bob", exact: true }).uncheck();
  await page.getByLabel("Add an account").fill("@carol");
  await page.getByRole("button", { name: "Add", exact: true }).click();
  await page.getByRole("button", { name: "Save", exact: true }).click();

  await expect(page.getByRole("status")).toHaveText(
    "Saved @alice, @bob, @carol.",
  );
  expect(server.writes[0]).toMatchObject({
    method: "PUT",
    path: "/api/reader/source-settings/00000000-0000-0000-0000-000000000001",
    body: {
      enabled: true,
      domain: "x.com",
      accounts: [on("alice"), { value: "bob", enabled: false }, on("@carol")],
      feeds: [],
      tags: ["ai"],
      adapters: null,
      reddit: null,
      hacker_news: null,
    },
  });
});

test("keeps unsaved edits when leaving the editor and when another tab saves", async ({
  page,
}) => {
  const server = await serve(page, [
    saved(1, { domain: "x.com", accounts: [on("alice")] }),
  ]);
  await page.clock.install();
  await page.goto("/?view=sources&manage=1");
  await page.getByRole("link", { name: /@alice/ }).click();
  await page.getByLabel("Add an account").fill("mine");
  await page.getByLabel("Add an account").press("Enter");

  await page.getByRole("button", { name: "All sources" }).click();
  await expect(page.getByText("Unsaved changes")).toBeVisible();

  // Another tab adds an account to the same source.
  server.sources = [
    saved(1, { domain: "x.com", accounts: [on("alice"), on("theirs")] }),
  ];
  await page.clock.runFor(5000);
  await expect(
    page.getByRole("link", { name: /@alice, @theirs/ }),
  ).toBeVisible();

  await page.getByRole("link", { name: /@alice/ }).click();
  await expect(
    page.getByRole("checkbox", { name: "@mine", exact: true }),
  ).toBeChecked();
  await expect(page.getByText("saved somewhere else")).toBeVisible();

  await page.getByRole("button", { name: "Discard changes" }).click();
  await expect(
    page.getByRole("checkbox", { name: "@theirs", exact: true }),
  ).toBeChecked();
  await expect(
    page.getByRole("checkbox", { name: "@mine", exact: true }),
  ).toHaveCount(0);
});

test("turns a source off and on from the list", async ({ page }) => {
  // Enough sources that the list offers search and filters.
  const server = await serve(page, [
    saved(1, { domain: "x.com", accounts: [on("alice")] }),
    ...[2, 3, 4, 5, 6].map((id) => saved(id, { domain: `blog${id}.example` })),
  ]);
  await page.goto("/?view=sources&manage=1");
  const toggle = page.getByRole("switch", { name: "Collect @alice" });

  await expect(
    page.getByRole("searchbox", { name: "Search saved sources" }),
  ).toBeVisible();
  // Nothing is off yet, so there is no on-or-off filter to offer.
  await expect(
    page.getByRole("button", { name: "Off", exact: true }),
  ).toHaveCount(0);

  await toggle.click();
  await expect(toggle).not.toBeChecked();
  expect(server.writes[0].body).toMatchObject({
    enabled: false,
    accounts: [on("alice")],
  });

  await page.getByRole("button", { name: "Off", exact: true }).click();
  await expect(page.getByRole("link", { name: /@alice/ })).toBeVisible();
  await expect(page.getByRole("link", { name: /blog\d\.example/ })).toHaveCount(
    0,
  );

  await page.getByRole("button", { name: "Blog or feed" }).click();
  await expect(page.getByText("No saved sources match.")).toBeVisible();
});

test("an unconfirmed save can be tried again, and a repeated add counts as saved", async ({
  page,
}) => {
  const server = await serve(page, [], { failing: [503] });
  await page.goto("/?view=sources&source=new&platform=x");
  await page.getByLabel("Add an account").fill("alice");
  await page.getByLabel("Add an account").press("Enter");
  await page.getByRole("button", { name: "Save", exact: true }).click();

  await expect(page.getByRole("alert")).toContainText("isn't confirmed");
  // The first request did reach the database before its answer was lost.
  server.sources = [saved(7, { domain: "x.com", accounts: [on("alice")] })];
  await page.getByRole("button", { name: "Try again" }).click();

  await expect(page.getByRole("status")).toHaveText("Added @alice.");
  await expect(page.getByRole("link", { name: /@alice/ })).toBeVisible();
  expect(server.sources).toHaveLength(1);
});

test("a duplicate links to the source that already has those settings", async ({
  page,
}) => {
  await serve(page, [
    saved(1, { domain: "x.com", enabled: false, accounts: [on("alice")] }),
  ]);
  await page.goto("/?view=sources&source=new&platform=x");
  await page.getByLabel("Add an account").fill("alice");
  await page.getByLabel("Add an account").press("Enter");
  await page.getByRole("button", { name: "Save", exact: true }).click();

  await expect(page.getByRole("alert")).toContainText(
    "already has exactly these settings",
  );
  await page.getByRole("button", { name: "Open it (it's turned off)" }).click();
  await expect(page).toHaveURL(/source=00000000-0000-0000-0000-000000000001/);
  await expect(page.getByLabel("Collect this source")).not.toBeChecked();
});

test("save and collect keeps the source when collection can't start", async ({
  page,
}) => {
  const server = await serve(
    page,
    [saved(1, { domain: "x.com", accounts: [on("alice")] })],
    { refreshFails: true },
  );
  await page.goto("/?view=sources&source=00000000-0000-0000-0000-000000000001");
  await page.getByLabel("Add an account").fill("bob");
  await page.getByLabel("Add an account").press("Enter");
  await page.getByRole("button", { name: "Save and collect" }).click();

  await expect(
    page.getByRole("alert").filter({
      hasText: "Saved @alice, @bob, but collection couldn't start",
    }),
  ).toBeVisible();
  // Only the account this save added is collected.
  expect(server.refreshes).toEqual([{ kind: "x", account: "bob" }]);
  expect(server.sources[0].accounts).toEqual([on("alice"), on("bob")]);
  await expect(page.getByRole("link", { name: /@alice, @bob/ })).toBeVisible();
});

test("says when the saved sources aren't the ones in use", async ({ page }) => {
  await serve(page, [saved(1, { domain: "blog.example" })], {
    inDatabase: false,
  });
  await page.goto("/?view=sources&manage=1");

  await expect(page.getByRole("note")).toContainText("aren’t in use yet");
  await page.getByRole("link", { name: /blog\.example/ }).click();
  await expect(
    page.getByRole("button", { name: "Save and collect" }),
  ).toHaveCount(0);
});

test("saves the account on screen into the chosen source and collects it", async ({
  page,
}) => {
  const server = await serve(page, [
    saved(1, { domain: "x.com", accounts: [on("bob")], tags: ["ai"] }),
    saved(2, { domain: "x.com", accounts: [on("carol")], tags: ["sport"] }),
  ]);
  await page.goto("/?view=x&account=alice");
  await page.getByRole("button", { name: "Save account" }).click();
  const dialog = page.getByRole("dialog", { name: "Save @alice" });

  // Two X sources differ in their options, so which one is a choice.
  await dialog.getByRole("radio", { name: /@carol/ }).check();
  await dialog.getByRole("button", { name: "Save and collect" }).click();

  await expect(dialog).toHaveCount(0);
  expect(server.writes[0]).toMatchObject({
    method: "PUT",
    path: "/api/reader/source-settings/00000000-0000-0000-0000-000000000002",
    body: { accounts: [on("carol"), on("alice")], tags: ["sport"] },
  });
  expect(server.refreshes).toEqual([{ kind: "x", account: "alice" }]);
  await expect(page.getByRole("button", { name: "Saved" })).toBeVisible();
});

test("offers to turn a saved account back on, and opens a collected one", async ({
  page,
}) => {
  const server = await serve(page, [
    saved(1, {
      domain: "x.com",
      accounts: [{ value: "Alice", enabled: false }],
    }),
  ]);
  await page.goto("/?view=x&account=alice");
  await page.getByRole("button", { name: "Turn on" }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Save", exact: true })
    .click();

  expect(server.writes[0].body.accounts).toEqual([on("Alice")]);
  expect(server.refreshes).toEqual([]);
  await page.getByRole("button", { name: "Saved" }).click();
  await expect(page).toHaveURL(
    /view=sources.*source=00000000-0000-0000-0000-000000000001/,
  );
});

test("saves a discovered site as a new source", async ({ page }) => {
  const server = await serve(page, []);
  await page.goto("/?view=stories&domain=found.example");
  await page.getByRole("button", { name: "Save source" }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Save", exact: true })
    .click();

  expect(server.writes[0]).toMatchObject({
    method: "POST",
    body: { domain: "found.example", enabled: true, feeds: [] },
  });
  await expect(page.getByRole("button", { name: "Saved" })).toBeVisible();
});

test("adds a pasted link by working out what it is, and collects it", async ({
  page,
}) => {
  const server = await serve(page, []);
  await page.goto("/?view=sources&manage=1");
  const box = page.getByRole("textbox", { name: "Add a source" });

  await box.fill("https://www.reddit.com/r/rust/comments/abc/a_post/");
  await expect(page.getByText("Reddit · r/rust")).toBeVisible();
  await box.press("Enter");

  await expect(page.getByRole("status")).toContainText(
    "Added r/rust. Collecting it now.",
  );
  await expect(box).toHaveValue("");
  await expect(box).toBeFocused();
  await expect(page.getByRole("link", { name: /r\/rust/ })).toBeVisible();
  expect(server.writes[0]).toMatchObject({
    method: "POST",
    body: {
      enabled: true,
      domain: "reddit.com",
      feeds: [on("r/rust")],
      reddit: { sort: "top", time_filter: "day" },
    },
  });
  expect(server.refreshes).toEqual([{ kind: "reddit", community: "rust" }]);

  // A link to any page of a site saves the site, without its path.
  await box.fill("https://www.blog.example/2024/a-post/");
  await page.getByRole("button", { name: "Add", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("Added blog.example.");
  expect(server.writes[1].body).toMatchObject({
    domain: "blog.example",
    feeds: [],
  });
  expect(server.refreshes[1]).toEqual({
    kind: "rss",
    domains: ["blog.example"],
  });

  await box.fill("not a source");
  await box.press("Enter");
  await expect(page.getByRole("alert")).toHaveText("Add one source at a time.");
  expect(server.writes).toHaveLength(2);
});

test("adds a handle to the X source that is on, or as a YouTube channel", async ({
  page,
}) => {
  const server = await serve(
    page,
    [saved(1, { domain: "x.com", accounts: [on("alice")], tags: ["ai"] })],
    { refreshFails: true },
  );
  await page.goto("/?view=sources&manage=1");
  const box = page.getByRole("textbox", { name: "Add a source" });
  const said = page.getByRole("form", { name: "Add a source" });

  await box.fill("@Alice");
  await box.press("Enter");
  await expect(page.getByRole("status")).toContainText(
    "DANK already collects @Alice.",
  );
  expect(server.writes).toHaveLength(0);

  await box.fill("@bob");
  await expect(page.getByText("X · @bob")).toBeVisible();
  await box.press("Enter");
  // Added all the same when collection can't start.
  await expect(said.getByRole("alert")).toContainText(
    "Added @bob, but collection couldn't start",
  );
  expect(server.writes[0]).toMatchObject({
    method: "PUT",
    path: "/api/reader/source-settings/00000000-0000-0000-0000-000000000001",
    body: { accounts: [on("alice"), on("bob")], tags: ["ai"] },
  });
  expect(server.refreshes).toEqual([{ kind: "x", account: "bob" }]);

  await box.fill("@carol");
  await said.getByRole("button", { name: "YouTube" }).click();
  await expect(page.getByText("YouTube · @carol")).toBeVisible();
  await box.press("Enter");
  await expect(said.getByRole("alert")).toContainText("Added @carol");
  expect(server.writes[1]).toMatchObject({
    method: "POST",
    body: {
      domain: "youtube.com",
      feeds: [on("https://www.youtube.com/@carol")],
    },
  });
});

test("a pasted link in the site field becomes the site and its feed", async ({
  page,
}) => {
  const server = await serve(page, []);
  await page.goto("/?view=sources&source=new&platform=rss");

  await page.getByLabel("Site").fill("https://www.blog.example/feed.xml");
  await page.getByLabel("Tags").click();
  await expect(page.getByLabel("Site")).toHaveValue("blog.example");
  await expect(
    page.getByRole("checkbox", { name: "blog.example/feed.xml", exact: true }),
  ).toBeChecked();

  // Collecting is the point of a new source, so it is the main action.
  await page.getByLabel("Site").press("Enter");
  await expect(page.getByRole("status")).toHaveText(
    "Added blog.example. Collecting it now.",
  );
  expect(server.writes[0].body).toMatchObject({
    domain: "blog.example",
    feeds: [on("https://www.blog.example/feed.xml")],
  });
  expect(server.refreshes).toEqual([
    { kind: "rss", domains: ["blog.example"] },
  ]);
});

test("the editor fits a phone, with touch-sized controls", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await serve(page, [
    saved(1, {
      domain: "reddit.com",
      feeds: [on("https://www.reddit.com/r/AVeryLongCommunityName/")],
      reddit: { sort: "top", time_filter: "day" },
    }),
  ]);
  await page.goto("/?view=sources&source=00000000-0000-0000-0000-000000000001");
  await expect(editor(page)).toBeVisible();

  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);

  // The save buttons stay on screen however long the form is.
  await expect(
    page.getByRole("button", { name: "Save", exact: true }),
  ).toBeInViewport({ ratio: 1 });

  for (const control of [
    page.getByRole("button", { name: "Save", exact: true }),
    page.getByRole("button", { name: "Remove r/AVeryLongCommunityName" }),
    page.getByLabel("Add a community"),
  ]) {
    const box = await control.boundingBox();
    expect(box?.height).toBeGreaterThanOrEqual(40);
  }
});
