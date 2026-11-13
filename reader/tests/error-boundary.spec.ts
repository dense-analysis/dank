import { expect, test } from "@playwright/test";
import { apiPath, fulfillEmpty } from "./apiMock";

test("data the reader doesn't expect offers a reload, not a blank page", async ({
  page,
}) => {
  // What a page left open across a deploy can meet: an answer in a shape
  // its code wasn't built for.
  await page.route("**/api/reader/**", (route) =>
    apiPath(route) === "/api/reader/youtube/sources"
      ? route.fulfill({ json: {} })
      : fulfillEmpty(route),
  );
  await page.goto("/");

  const alert = page.getByRole("alert").filter({
    hasText: "The reader hit a problem",
  });
  await expect(alert).toBeVisible();
  await expect(alert.getByRole("button", { name: "Reload" })).toBeVisible();
});
