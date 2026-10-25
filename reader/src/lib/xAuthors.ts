import { postParams, request } from "./api";
import type { Filters, Post } from "./types";
import { isPost, isRecord } from "./validation";

export interface XAuthor {
  handle: string;
  name: string;
  icon_url: string | null;
  count: number;
  posts: Post[];
}

export interface XAuthorPage {
  authors: XAuthor[];
  total: number;
  unattributed_posts: number;
  next_cursor: string | null;
}

function count(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

export async function fetchXAuthors(
  filters: Filters,
  cursor?: string,
  signal?: AbortSignal,
): Promise<XAuthorPage> {
  const params = postParams(filters, "20");
  if (cursor) params.set("cursor", cursor);
  const body = await request(`/x/authors?${params}`, signal);
  if (
    !isRecord(body) ||
    !Array.isArray(body.authors) ||
    !body.authors.every(
      (author) =>
        isRecord(author) &&
        typeof author.handle === "string" &&
        /^[a-z0-9_]{1,15}$/.test(author.handle) &&
        typeof author.name === "string" &&
        (author.icon_url === null || typeof author.icon_url === "string") &&
        count(author.count) &&
        author.count > 0 &&
        Array.isArray(author.posts) &&
        author.posts.length <= 3 &&
        author.posts.every(isPost),
    ) ||
    !count(body.total) ||
    !count(body.unattributed_posts) ||
    (body.next_cursor !== null && typeof body.next_cursor !== "string")
  ) {
    throw new Error("The reader received an unexpected author response.");
  }

  return body as unknown as XAuthorPage;
}
