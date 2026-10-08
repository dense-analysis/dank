import type { Filters, Post, SavedFeed } from "./types";
import { isFilters, isPost, isRecord } from "./validation";

export interface Library {
  version: 1;
  feeds: SavedFeed[];
  bookmarks: Post[];
  read: string[];
}

export const defaultFilters: Filters = {
  q: "",
  mode: "words",
  sort: "newest",
  domains: [],
  tags: [],
  author: "",
  after: "",
  before: "",
};

const STORAGE_KEY = "dank-reader:library:v1";
const MAX_READ = 5_000;

export function postKey(post: Pick<Post, "domain" | "id">): string {
  return JSON.stringify([post.domain, post.id]);
}

function isSavedFeed(value: unknown): value is SavedFeed {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    value.id.trim().length > 0 &&
    typeof value.name === "string" &&
    value.name.trim().length > 0 &&
    isFilters(value.filters)
  );
}

function emptyLibrary(): Library {
  return { version: 1, feeds: [], bookmarks: [], read: [] };
}

function uniqueBy<T>(items: T[], key: (item: T) => string): T[] {
  return [...new Map(items.map((item) => [key(item), item])).values()];
}

function parseLibrary(value: unknown): Library {
  if (
    !isRecord(value) ||
    value.version !== 1 ||
    !Array.isArray(value.feeds) ||
    !Array.isArray(value.bookmarks) ||
    !Array.isArray(value.read)
  ) {
    return emptyLibrary();
  }

  return {
    version: 1,
    feeds: uniqueBy(value.feeds.filter(isSavedFeed), (feed) => feed.id),
    bookmarks: uniqueBy(value.bookmarks.filter(isPost), postKey),
    read: [
      ...new Set(
        value.read.filter((id): id is string => typeof id === "string"),
      ),
    ].slice(-MAX_READ),
  };
}

export function loadLibrary(): Library {
  try {
    const value = globalThis.localStorage.getItem(STORAGE_KEY);
    return value ? parseLibrary(JSON.parse(value)) : emptyLibrary();
  } catch {
    return emptyLibrary();
  }
}

export function saveLibrary(library: Library): boolean {
  try {
    globalThis.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify(parseLibrary(library)),
    );
    return true;
  } catch {
    return false;
  }
}
