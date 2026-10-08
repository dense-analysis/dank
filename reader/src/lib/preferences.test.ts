import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  defaultPreferences,
  loadPreferences,
  savePreferences,
} from "./preferences";

let persisted: string | null;
beforeEach(() => {
  persisted = null;
  vi.stubGlobal("localStorage", {
    getItem: () => persisted,
    setItem: (_key: string, value: string) => {
      persisted = value;
    },
  });
});
afterEach(() => vi.unstubAllGlobals());

it("persists the selected reading font and bounded text size", () => {
  expect(savePreferences({ font: "newsreader", fontSize: 23 })).toBe(true);
  expect(loadPreferences()).toEqual({ font: "newsreader", fontSize: 23 });
});

it.each([
  "broken",
  "null",
  "[]",
  '{"font":"unknown","fontSize":100}',
  '{"font":"__proto__","fontSize":19.5}',
])("recovers invalid stored preferences: %s", (value) => {
  persisted = value;
  expect(loadPreferences()).toEqual(defaultPreferences);
});

it("preserves a valid font when the stored text size is invalid", () => {
  persisted = '{"font":"georgia","fontSize":-1}';
  expect(loadPreferences()).toEqual({ font: "georgia", fontSize: 19 });
});

it("reports storage failure without throwing or deleting saved preferences", () => {
  savePreferences({ font: "newsreader", fontSize: 22 });
  vi.spyOn(globalThis.localStorage, "setItem").mockImplementation(() => {
    throw new Error("Quota exceeded");
  });
  expect(savePreferences(defaultPreferences)).toBe(false);
  expect(loadPreferences()).toEqual({ font: "newsreader", fontSize: 22 });
  vi.stubGlobal("localStorage", undefined);
  expect(loadPreferences()).toEqual(defaultPreferences);
});
