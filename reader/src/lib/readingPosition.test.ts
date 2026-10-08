import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { loadReadingPosition, saveReadingPosition } from "./readingPosition";
import { postKey } from "./storage";

let stored: string | null;
beforeEach(() => {
  stored = null;
  vi.stubGlobal("localStorage", {
    getItem: () => stored,
    setItem: (_key: string, value: string) => {
      stored = value;
    },
  });
});
afterEach(() => vi.unstubAllGlobals());

it("retains independent reading positions for the same post id on different sources", () => {
  const a = postKey({ domain: "a.example", id: "same" });
  const b = postKey({ domain: "b.example", id: "same" });
  const position = { scroll: 1200, block: 9, offset: -20 };
  expect(saveReadingPosition(a, position)).toBe(true);
  expect(saveReadingPosition(b, { scroll: 0, block: null, offset: 0 })).toBe(
    true,
  );
  expect(loadReadingPosition(a)).toEqual(position);
  expect(loadReadingPosition(b)?.scroll).toBe(0);
});

it.each([
  "{broken",
  "null",
  "{}",
  '[["key",{"scroll":-1,"block":0,"offset":0}]]',
  '[["key",{"scroll":1e309,"block":0,"offset":0}]]',
  '[["key",{"scroll":40,"block":-1,"offset":0}]]',
])("ignores corrupt or invalid position storage: %s", (value) => {
  stored = value;
  expect(loadReadingPosition("key")).toBeUndefined();
});

it("bounds storage to the most recently updated 200 articles", () => {
  for (let index = 0; index < 205; index++)
    saveReadingPosition(String(index), {
      scroll: index,
      block: null,
      offset: 0,
    });
  expect(loadReadingPosition("0")).toBeUndefined();
  expect(loadReadingPosition("5")?.scroll).toBe(5);
  saveReadingPosition("5", { scroll: 500, block: null, offset: 0 });
  saveReadingPosition("205", { scroll: 205, block: null, offset: 0 });
  expect(loadReadingPosition("6")).toBeUndefined();
  expect(loadReadingPosition("5")?.scroll).toBe(500);
});

it("reports unavailable storage without breaking reading", () => {
  vi.stubGlobal("localStorage", undefined);
  expect(loadReadingPosition("key")).toBeUndefined();
  expect(
    saveReadingPosition("key", { scroll: 300, block: null, offset: 0 }),
  ).toBe(false);
});
