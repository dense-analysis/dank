import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { loadReadPosts, postKey, saveReadPosts } from "./storage";

const readKey = "dank-reader:read:v1";
const legacyKey = "dank-reader:library:v1";
let stored: Map<string, string>;

beforeEach(() => {
  stored = new Map();
  vi.stubGlobal("localStorage", {
    getItem: vi.fn((key: string) => stored.get(key) ?? null),
    setItem: vi.fn((key: string, value: string) => {
      stored.set(key, value);
    }),
  });
});
afterEach(() => vi.unstubAllGlobals());

describe("read markers", () => {
  it("persists source-specific identities across reloads", () => {
    const read = [postKey({ domain: "example.com", id: "123" })];
    expect(saveReadPosts(read)).toBe(true);
    expect(loadReadPosts()).toEqual(read);
    expect(globalThis.localStorage.setItem).toHaveBeenCalledWith(
      readKey,
      JSON.stringify(read),
    );
  });

  it("carries over read markers without modifying stored bookmarks or feeds", () => {
    const legacy = JSON.stringify({
      version: 1,
      feeds: [{ id: "old-feed", name: "Old feed" }],
      bookmarks: [{ id: "saved-story" }],
      read: ["read-story"],
    });
    stored.set(legacyKey, legacy);
    expect(loadReadPosts()).toEqual(["read-story"]);
    saveReadPosts([...loadReadPosts(), "another-story"]);
    expect(loadReadPosts()).toEqual(["read-story", "another-story"]);
    expect(stored.get(legacyKey)).toBe(legacy);
    expect(globalThis.localStorage.setItem).not.toHaveBeenCalledWith(
      legacyKey,
      expect.anything(),
    );
  });

  it("uses the new read state once present instead of reloading old markers", () => {
    stored.set(legacyKey, JSON.stringify({ version: 1, read: ["old"] }));
    saveReadPosts([]);
    expect(loadReadPosts()).toEqual([]);
  });

  it.each(["not JSON", "null", "{}", "123"])(
    "recovers from malformed read storage: %s",
    (value) => {
      stored.set(readKey, value);
      expect(loadReadPosts()).toEqual([]);
    },
  );

  it("handles corrupt legacy storage without overwriting it", () => {
    stored.set(legacyKey, "not JSON");
    expect(loadReadPosts()).toEqual([]);
    saveReadPosts(["new"]);
    expect(stored.get(legacyKey)).toBe("not JSON");
  });

  it("filters invalid markers and limits storage to 5,000 unique identities", () => {
    const read = Array.from({ length: 5020 }, (_, i) =>
      postKey({ domain: "example.com", id: String(i) }),
    );
    stored.set(readKey, JSON.stringify([null, 12, ...read, read[5019]]));
    expect(loadReadPosts()).toEqual(read.slice(20));
  });

  it("reports quota failure without losing prior read state", () => {
    saveReadPosts(["previous"]);
    vi.mocked(globalThis.localStorage.setItem).mockImplementation(() => {
      throw new Error("Quota exceeded");
    });
    expect(saveReadPosts(["new"])).toBe(false);
    expect(loadReadPosts()).toEqual(["previous"]);
  });

  it("handles unavailable browser storage", () => {
    vi.stubGlobal("localStorage", undefined);
    expect(loadReadPosts()).toEqual([]);
    expect(saveReadPosts(["new"])).toBe(false);
  });
});

describe("post identity", () => {
  it("distinguishes identical ids across sources and delimiter-like values", () => {
    expect(postKey({ domain: "a", id: "same" })).not.toBe(
      postKey({ domain: "b", id: "same" }),
    );
    expect(postKey({ domain: "a:b", id: "c" })).not.toBe(
      postKey({ domain: "a", id: "b:c" }),
    );
    expect(postKey({ domain: "a/b", id: "c" })).not.toBe(
      postKey({ domain: "a", id: "b/c" }),
    );
  });
});
