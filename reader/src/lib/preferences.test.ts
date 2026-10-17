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

it("shows comments and remote labels for older or malformed preferences", () => {
  persisted =
    '{"font":"georgia","showRedditComments":"false","showRemoteContentLabels":0}';
  expect(loadPreferences().showRedditComments).toBe(true);
  expect(loadPreferences().showRemoteContentLabels).toBe(true);
});

it("persists opting out of Reddit comment and remote label display", () => {
  savePreferences({
    ...defaultPreferences,
    showRedditComments: false,
    showRemoteContentLabels: false,
  });
  expect(loadPreferences().showRedditComments).toBe(false);
  expect(loadPreferences().showRemoteContentLabels).toBe(false);
});

it("defaults older or malformed media preferences to fitted previews", () => {
  for (const value of [undefined, null, "false", 0, {}, []]) {
    persisted = JSON.stringify({
      fitImagePreviews: value,
      fitVideoPreviews: value,
    });
    expect(loadPreferences().fitImagePreviews).toBe(true);
    expect(loadPreferences().fitVideoPreviews).toBe(true);
  }
});

it("persists independent image and video preview choices", () => {
  for (const fitImagePreviews of [true, false]) {
    for (const fitVideoPreviews of [true, false]) {
      const preferences = {
        ...defaultPreferences,
        fitImagePreviews,
        fitVideoPreviews,
      };
      expect(savePreferences(preferences)).toBe(true);
      expect(loadPreferences()).toEqual(preferences);
    }
  }
});
