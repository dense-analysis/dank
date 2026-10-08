import { filterParams } from "./navigation";
import type { Filters, Post, PostPage, Source } from "./types";
import { isPost, isRecord, isSource } from "./validation";

async function request(path: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(`/api/reader${path}`, {
    signal,
    headers: { Accept: "application/json" },
  });

  if (!response.ok) {
    let detail = "";
    try {
      const body: unknown = await response.json();
      if (isRecord(body)) {
        const message = body.detail ?? body.error ?? body.message;
        if (typeof message === "string") detail = message.slice(0, 300);
      }
    } catch {
      // A proxy may return HTML instead of the API's JSON error body.
    }

    throw new Error(
      detail ||
        `The reader could not load this request (${response.status}). Please try again.`,
    );
  }

  try {
    return await response.json();
  } catch {
    throw new Error(
      "The reader received an unreadable response. Please try again.",
    );
  }
}

function invalidResponse(): never {
  throw new Error(
    "The reader received an unexpected response. Please try again.",
  );
}

export async function fetchSources(
  signal?: AbortSignal,
): Promise<{ sources: Source[]; total_posts: number }> {
  const body = await request("/sources", signal);
  if (
    !isRecord(body) ||
    !Array.isArray(body.sources) ||
    !body.sources.every(isSource) ||
    typeof body.total_posts !== "number" ||
    !Number.isSafeInteger(body.total_posts) ||
    body.total_posts < 0
  ) {
    return invalidResponse();
  }

  return { sources: body.sources, total_posts: body.total_posts };
}

export async function fetchPosts(
  filters: Filters,
  cursor?: string | null,
  signal?: AbortSignal,
): Promise<PostPage> {
  const params = filterParams(filters);
  params.set("limit", "30");
  if (cursor) params.set("cursor", cursor);

  const body = await request(`/posts?${params.toString()}`, signal);
  if (
    !isRecord(body) ||
    !Array.isArray(body.posts) ||
    !body.posts.every(isPost) ||
    (body.next_cursor !== null && typeof body.next_cursor !== "string") ||
    typeof body.limited !== "boolean"
  ) {
    return invalidResponse();
  }

  return {
    posts: body.posts,
    next_cursor: body.next_cursor,
    limited: body.limited,
  };
}

export async function fetchPost(
  domain: string,
  id: string,
  signal?: AbortSignal,
): Promise<Post> {
  const params = new URLSearchParams({ domain, id });
  const body = await request(`/post?${params.toString()}`, signal);
  if (!isPost(body)) return invalidResponse();

  return body;
}
