import type { Route } from "@playwright/test";
import type { ApiResponse, PathFor } from "../src/lib/api";

type ListPath = Exclude<
  PathFor<"get">,
  | `${string}{${string}`
  | "/api/reader/post"
  | "/api/reader/connection"
  | "/api/reader/source-icon"
>;

/**
 * What the server answers for each list route when nothing is collected,
 * typed by the schema, so a spec mocks only the routes it is about.
 */
export const emptyResponses: { [P in ListPath]: ApiResponse<P> } = {
  "/api/reader/sources": {
    sources: [],
    total_posts: 0,
    blog_external_count: 0,
    in_database: false,
  },
  "/api/reader/categories": { categories: [] },
  "/api/reader/source-settings": { sources: [], in_database: false },
  "/api/reader/posts": { posts: [], next_cursor: null, limited: false },
  "/api/reader/x/authors": {
    authors: [],
    next_cursor: null,
    total: 0,
    unattributed_posts: 0,
  },
  "/api/reader/connections": {
    root: { domain: "", id: "", url: "", title: "", source: "" },
    incoming: [],
    outgoing: [],
    related: [],
    context: {},
    related_count: 0,
    incoming_count: 0,
    outgoing_count: 0,
    limit: 100,
    coverage: "saved_evidence",
  },
  "/api/reader/refresh": { jobs: [] },
  "/api/reader/youtube": { items: [], next_cursor: null, total: 0 },
  "/api/reader/youtube/sources": {
    feeds: [],
    channels: [],
    total: 0,
    linked_count: 0,
  },
  "/api/reader/youtube/channels": {
    channels: [],
    next_cursor: null,
    total: 0,
    unattributed_items: 0,
  },
  "/api/reader/youtube/playlists": {
    playlists: [],
    total: 0,
    next_cursor: null,
  },
};

export function apiPath(route: Route): string {
  return new URL(route.request().url()).pathname;
}

/**
 * Answers a route a spec doesn't mock as the server would with nothing
 * collected: an empty list, or the error envelope's 404 for one item.
 */
export function fulfillEmpty(route: Route): Promise<void> {
  const path = apiPath(route);

  if (path in emptyResponses)
    return route.fulfill({ json: emptyResponses[path as ListPath] });

  return route.fulfill({
    status: 404,
    json: { error: "Not Found", code: "not_found", fields: [] },
  });
}
