import { expect, type Page, test } from "@playwright/test";
import type { Source } from "../src/lib/types";

const sources: Source[] = [
  {
    domain: "x.com",
    name: "X",
    kind: "x",
    count: 30,
    tags: [],
    accounts: Array.from({ length: 30 }, (_, index) => ({
      handle: `reporter${index + 1}`,
      name: `City Reporter ${index + 1}`,
      count: 1,
      icon_url: null,
    })),
  },
  {
    domain: "reddit.com",
    name: "Reddit",
    kind: "reddit",
    count: 1,
    tags: [],
    communities: [{ name: "LocalLLaMA", count: 1 }],
  },
];

async function mockNavigation(page: Page) {
  await page.route("**/api/reader/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const json = path.endsWith("/youtube/sources")
      ? { feeds: [], channels: [], linked_count: 0 }
      : path.endsWith("/sources")
        ? { sources, total_posts: 31 }
        : { posts: [], next_cursor: null, limited: false };

    await route.fulfill({ json });
  });
}

function openNavigation(page: Page) {
  return page.getByRole("button", { name: "Open navigation", exact: true });
}

for (const width of [320, 390]) {
  test(`mobile feed navigation is compact and respects saved groups at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 700 });
    await mockNavigation(page);
    await page.goto("/reader/");
    await openNavigation(page).click();
    const navigation = page.getByRole("dialog", { name: "Navigation" });
    const xToggle = navigation.getByRole("button", {
      name: "Expand X accounts",
    });
    await expect(xToggle).toBeVisible();
    await expect(page.locator("#x-accounts")).toBeHidden();
    await expect(page.locator("#reddit-communities")).toBeHidden();
    await expect(
      navigation.getByRole("button", { name: "Reddit", exact: true }),
    ).toBeInViewport();
    await expect(
      navigation.getByRole("button", { name: "YouTube", exact: true }),
    ).toBeInViewport();
    await expect(openNavigation(page)).toHaveAttribute("aria-expanded", "true");

    await xToggle.click();
    await navigation
      .getByRole("button", {
        name: "City Reporter 30 (@reporter30), 1 collected posts",
      })
      .click();
    await expect(page).toHaveURL(/account=reporter30/);
    await expect(navigation).toHaveCount(0);
    await expect(openNavigation(page)).toHaveAttribute(
      "aria-expanded",
      "false",
    );
    await expect(page.locator("body")).not.toHaveCSS("overflow", "hidden");

    await openNavigation(page).click();
    await expect(navigation.locator('[aria-current="page"]')).toBeInViewport();
    await navigation
      .getByRole("button", { name: "Collapse X accounts" })
      .click();
    await page.reload();
    await openNavigation(page).click();
    await expect(page.locator("#x-accounts")).toBeHidden();
    await navigation.getByRole("button", { name: "Expand X accounts" }).click();
    await page.keyboard.press("Escape");
    await expect(openNavigation(page)).toBeFocused();
    await openNavigation(page).click();
    await expect(page.locator("#x-accounts")).toBeVisible();
  });
}

test("tapping the drawer backdrop dismisses it and restores the opener", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockNavigation(page);
  await page.goto("/reader/");
  await openNavigation(page).click();
  const navigation = page.getByRole("dialog", { name: "Navigation" });
  await expect(navigation).toBeVisible();
  await navigation.locator(".sidebar").click({ position: { x: 4, y: 4 } });
  await expect(navigation).toBeVisible();
  await page.mouse.click(385, 400);
  await expect(navigation).toHaveCount(0);
  await expect(openNavigation(page)).toBeFocused();
  await expect(page.locator("body")).not.toHaveCSS("overflow", "hidden");
});

test("rotating to a desktop layout closes the mobile modal and unlocks the page", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockNavigation(page);
  await page.goto("/reader/");
  await openNavigation(page).click();
  await expect(page.getByRole("dialog", { name: "Navigation" })).toBeVisible();
  await page.setViewportSize({ width: 844, height: 390 });
  await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(0);
  await expect(page.locator("body")).not.toHaveCSS("overflow", "hidden");
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Blogs", exact: true })
    .click();
  await expect(page).toHaveURL(/view=stories/);
});

test("mobile direct links reveal the active group without changing desktop defaults", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockNavigation(page);
  await page.goto("/reader/?view=x&account=reporter30");
  await openNavigation(page).click();
  await expect(page.locator("#x-accounts")).toBeVisible();
  await expect(page.locator("#reddit-communities")).toBeHidden();
  await expect(
    page
      .getByRole("dialog", { name: "Navigation" })
      .locator('[aria-current="page"]'),
  ).toBeInViewport();

  await page.setViewportSize({ width: 1280, height: 900 });
  await expect(page.locator("#x-accounts")).toBeVisible();
  await expect(page.locator("#reddit-communities")).toBeVisible();
});
