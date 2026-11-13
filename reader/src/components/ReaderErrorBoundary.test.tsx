import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ReaderErrorBoundary } from "./ReaderErrorBoundary";

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function Broken(): never {
  throw new TypeError("Cannot read properties of undefined");
}

it("offers a reload instead of a blank page when rendering fails", async () => {
  // React reports the caught error to the console as well.
  vi.spyOn(console, "error").mockImplementation(() => {});
  const container = document.createElement("div");
  const root = createRoot(container);

  await act(() =>
    root.render(
      <ReaderErrorBoundary>
        <Broken />
      </ReaderErrorBoundary>,
    ),
  );

  expect(container.querySelector('[role="alert"]')?.textContent).toContain(
    "The reader hit a problem",
  );
  expect(container.querySelector("button")?.textContent).toContain("Reload");
  await act(() => root.unmount());
});

it("renders its children while nothing fails", async () => {
  const container = document.createElement("div");
  const root = createRoot(container);

  await act(() =>
    root.render(
      <ReaderErrorBoundary>
        <p>Stories</p>
      </ReaderErrorBoundary>,
    ),
  );

  expect(container.textContent).toBe("Stories");
  await act(() => root.unmount());
});
