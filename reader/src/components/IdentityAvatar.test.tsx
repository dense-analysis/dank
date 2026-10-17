import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { IdentityAvatar } from "./IdentityAvatar";

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  root = createRoot(container);
});

afterEach(async () => {
  await act(() => root.unmount());
  vi.unstubAllGlobals();
});

it("loads captured X profile images without a referrer and falls back on failure", async () => {
  await act(() =>
    root.render(
      <IdentityAvatar
        name="reporter"
        kind="account"
        iconUrl="https://pbs.twimg.com/profile_images/123/photo.jpg"
      />,
    ),
  );
  const image = container.querySelector("img");
  expect(image?.getAttribute("referrerpolicy")).toBe("no-referrer");
  expect(image?.alt).toBe("");
  await act(() => image?.dispatchEvent(new Event("error")));
  expect(container.querySelector("img")).toBeNull();
  expect(container.textContent).toBe("R");
});

it.each([
  "https://evil.test/photo.jpg",
  "https://pbs.twimg.com.evil.test/profile_images/photo.jpg",
  "http://pbs.twimg.com/profile_images/photo.jpg",
  "https://pbs.twimg.com/media/photo.jpg",
  "https://user@pbs.twimg.com/profile_images/photo.jpg",
  "javascript:alert(1)",
])("rejects an untrusted account image %s", async (iconUrl) => {
  await act(() =>
    root.render(
      <IdentityAvatar name="reporter" kind="account" iconUrl={iconUrl} />,
    ),
  );
  expect(container.querySelector("img")).toBeNull();
  expect(container.textContent).toBe("R");
});
