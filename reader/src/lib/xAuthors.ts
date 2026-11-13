import { apiGet, postParams } from "./api";
import type { components } from "./api.gen";
import type { Filters } from "./types";

export type XAuthor = components["schemas"]["XAuthorOut"];
export type XAuthorPage = components["schemas"]["XAuthorsResponse"];

export function fetchXAuthors(
  filters: Filters,
  cursor?: string,
  signal?: AbortSignal,
): Promise<XAuthorPage> {
  const query = postParams(filters, "20");
  if (cursor) query.set("cursor", cursor);

  return apiGet("/api/reader/x/authors", { query, signal });
}
