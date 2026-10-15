import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { CommunityAvatar } from "./CommunityAvatar";

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function render(community: string, iconUrl?: string | null) {
  await act(() =>
    root.render(<CommunityAvatar community={community} iconUrl={iconUrl} />),
  );
}

it("uses decorative cached images and falls back on failure without leaving a broken image", async () => {
  await render("LocalLLaMA", "/assets/community.png");
  const image = container.querySelector("img");
  expect(image?.getAttribute("alt")).toBe("");
  expect(image?.parentElement?.getAttribute("aria-hidden")).toBe("true");
  await act(() => image?.dispatchEvent(new Event("error")));
  expect(container.querySelector("img")).toBeNull();
  expect(container.textContent).toBe("L");

  await render("accelerate", "/assets/accelerate.png");
  expect(container.querySelector("img")?.getAttribute("src")).toContain(
    "/assets/accelerate.png",
  );
});

it.each([
  undefined,
  null,
  "https://reddit.com/assets/icon.png",
  "//reddit.com/assets/icon.png",
  "javascript:alert(1)",
  "/api/reader/posts",
])(
  "uses the community initial without fetching an unavailable or non-asset icon %s",
  async (iconUrl) => {
    await render("r/accelerate", iconUrl);
    expect(container.querySelector("img")).toBeNull();
    expect(container.textContent).toBe("A");
  },
);
