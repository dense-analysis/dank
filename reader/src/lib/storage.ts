import type { Filters, Post } from "./types";
import { isRecord } from "./validation";

export const defaultFilters: Filters = {
  q: "",
  sort: "newest",
  domains: [],
  tags: [],
  author: "",
  authorExact: false,
  after: "",
  before: "",
};
const READ_KEY = "dank-reader:read:v1";
const LEGACY_KEY = "dank-reader:library:v1";
const MAX_READ = 5_000;

export function postKey(post: Pick<Post, "domain" | "id">): string {
  return JSON.stringify([post.domain, post.id]);
}

function parseRead(value: unknown): string[] {
  return Array.isArray(value)
    ? [
        ...new Set(value.filter((id): id is string => typeof id === "string")),
      ].slice(-MAX_READ)
    : [];
}

export function loadReadPosts(): string[] {
  try {
    const current = globalThis.localStorage.getItem(READ_KEY);
    if (current !== null) return parseRead(JSON.parse(current));
    // Keep previous bookmarks and feeds untouched while carrying over read markers.
    const legacy: unknown = JSON.parse(
      globalThis.localStorage.getItem(LEGACY_KEY) ?? "null",
    );
    return isRecord(legacy) && legacy.version === 1
      ? parseRead(legacy.read)
      : [];
  } catch {
    return [];
  }
}

export function saveReadPosts(read: string[]): boolean {
  try {
    globalThis.localStorage.setItem(READ_KEY, JSON.stringify(parseRead(read)));
    return true;
  } catch {
    return false;
  }
}
