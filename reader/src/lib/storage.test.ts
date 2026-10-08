import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Library } from "./storage";
import { defaultFilters, loadLibrary, postKey, saveLibrary } from "./storage";
import type { Post } from "./types";

const post: Post = {
  id: "123",
  domain: "example.com",
  url: "https://example.com/123",
  author: "Author",
  title: "An article",
  excerpt: "The first sentence.",
  html: "<p>The article.</p>",
  created_at: "2026-10-08T10:00:00Z",
  source: "Example",
  thumbnail: null,
  media: [],
};

const empty: Library = { version: 1, feeds: [], bookmarks: [], read: [] };
let persisted: string | null;

beforeEach(() => {
  persisted = null;
  vi.stubGlobal("localStorage", {
    getItem: vi.fn(() => persisted),
    setItem: vi.fn((_key: string, value: string) => {
      persisted = value;
    }),
  });
});

afterEach(() => vi.unstubAllGlobals());

describe("library persistence", () => {
  it("preserves saved definitions, complete bookmarks, and read state across reloads", () => {
    const library: Library = {
      version: 1,
      feeds: [
        {
          id: "politics",
          name: "Politics",
          filters: { ...defaultFilters, tags: ["politics"] },
        },
      ],
      bookmarks: [post],
      read: [postKey(post)],
    };

    expect(saveLibrary(library)).toBe(true);
    expect(loadLibrary()).toEqual(library);
    expect(globalThis.localStorage.setItem).toHaveBeenCalledWith(
      "dank-reader:library:v1",
      expect.any(String),
    );
  });

  it.each(["not JSON", "null", '{"version":2}', '{"version":1,"feeds":[]}'])(
    "recovers safely from corrupt or unsupported storage: %s",
    (value) => {
      persisted = value;
      expect(loadLibrary()).toEqual(empty);
    },
  );

  it("discards malformed entries while retaining valid user data", () => {
    persisted = JSON.stringify({
      version: 1,
      feeds: [
        { id: "good", name: "Politics", filters: defaultFilters },
        {
          id: "bad",
          name: "Broken",
          filters: { ...defaultFilters, tags: [42] },
        },
        {
          id: "bad-date",
          name: "Broken date",
          filters: { ...defaultFilters, after: "2026-02-30" },
        },
      ],
      bookmarks: [post, { ...post, id: "bad", media: [null] }],
      read: [postKey(post), 42, null, postKey(post)],
    });

    expect(loadLibrary()).toEqual({
      version: 1,
      feeds: [{ id: "good", name: "Politics", filters: defaultFilters }],
      bookmarks: [post],
      read: [postKey(post)],
    });
  });

  it("retains the most recent 5,000 unique read identities", () => {
    const read = Array.from({ length: 5_020 }, (_, index) =>
      postKey({ domain: "example.com", id: String(index) }),
    );
    saveLibrary({ ...empty, read });
    expect(loadLibrary().read).toEqual(read.slice(20));
  });

  it("handles storage being unavailable and reports unsuccessful saves", () => {
    vi.stubGlobal("localStorage", undefined);
    expect(loadLibrary()).toEqual(empty);
    expect(saveLibrary(empty)).toBe(false);
  });

  it("reports quota errors without destroying the previous saved library", () => {
    saveLibrary({ ...empty, bookmarks: [post] });
    vi.mocked(globalThis.localStorage.setItem).mockImplementation(() => {
      throw new Error("Quota exceeded");
    });

    expect(saveLibrary(empty)).toBe(false);
    expect(loadLibrary().bookmarks).toEqual([post]);
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
