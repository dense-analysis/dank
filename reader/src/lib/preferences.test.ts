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
  const preferences = {
    ...defaultPreferences,
    font: "newsreader" as const,
    fontSize: 23,
  };
  expect(savePreferences(preferences)).toBe(true);
  expect(loadPreferences()).toEqual(preferences);
});

it("keeps automatic image backgrounds opt-in for existing preferences", () => {
  persisted = '{"font":"newsreader","fontSize":23}';
  expect(loadPreferences()).toEqual({
    ...defaultPreferences,
    font: "newsreader",
    fontSize: 23,
  });
});

it("persists opting in and opting out of automatic image backgrounds", () => {
  for (const autoImageBackground of [true, false]) {
    expect(
      savePreferences({ ...defaultPreferences, autoImageBackground }),
    ).toBe(true);
    expect(loadPreferences().autoImageBackground).toBe(autoImageBackground);
  }
});

it.each(["true", 1, null, {}, []])(
  "does not opt in from an invalid setting: %j",
  (value) => {
    persisted = JSON.stringify({
      ...defaultPreferences,
      autoImageBackground: value,
    });
    expect(loadPreferences().autoImageBackground).toBe(false);
  },
);

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
  expect(loadPreferences()).toEqual({
    ...defaultPreferences,
    font: "georgia",
    fontSize: 19,
  });
});

it("reports storage failure without throwing or deleting saved preferences", () => {
  savePreferences({ ...defaultPreferences, font: "newsreader", fontSize: 22 });
  vi.spyOn(globalThis.localStorage, "setItem").mockImplementation(() => {
    throw new Error("Quota exceeded");
  });
  expect(savePreferences(defaultPreferences)).toBe(false);
  expect(loadPreferences()).toEqual({
    ...defaultPreferences,
    font: "newsreader",
    fontSize: 22,
  });
  vi.stubGlobal("localStorage", undefined);
  expect(loadPreferences()).toEqual(defaultPreferences);
});
