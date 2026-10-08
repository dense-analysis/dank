import { defaultFilters } from "./storage";
import type { Filters, Post } from "./types";
import { isDateFilter } from "./validation";

export function articleParams(
  params: URLSearchParams,
  post: Pick<Post, "id" | "domain">,
): URLSearchParams {
  const next = new URLSearchParams(params);
  next.set("article", post.id);
  next.set("article_source", post.domain);
  return next;
}

function selections(params: URLSearchParams, name: string): string[] {
  return [
    ...new Set(
      params
        .getAll(name)
        .map((value) => value.trim())
        .filter(Boolean),
    ),
  ];
}

export function parseFilters(params: URLSearchParams): Filters {
  const q = (params.get("q") ?? "").trim();
  const sort = params.get("sort");
  const after = params.get("after") ?? "";
  const before = params.get("before") ?? "";

  return {
    ...defaultFilters,
    q,
    sort:
      sort === "oldest" || sort === "newest"
        ? sort
        : q
          ? "relevance"
          : "newest",
    domains: selections(params, "domain"),
    tags: selections(params, "tag"),
    author: (params.get("author") ?? "").trim(),
    authorExact:
      !!params.get("author")?.trim() && params.get("author_match") === "exact",
    after: isDateFilter(after) ? after : "",
    before: isDateFilter(before) ? before : "",
  };
}

export function filterParams(filters: Filters): URLSearchParams {
  const params = new URLSearchParams();
  const q = filters.q.trim();
  if (q) params.set("q", q);
  const defaultSort = q ? "relevance" : "newest";
  if (filters.sort !== defaultSort && (filters.sort !== "relevance" || q)) {
    params.set("sort", filters.sort);
  }

  for (const domain of new Set(filters.domains)) {
    if (domain.trim()) params.append("domain", domain.trim());
  }

  for (const tag of new Set(filters.tags)) {
    if (tag.trim()) params.append("tag", tag.trim());
  }

  if (filters.author.trim()) {
    params.set("author", filters.author.trim());
    if (filters.authorExact) params.set("author_match", "exact");
  }
  if (filters.after && isDateFilter(filters.after))
    params.set("after", filters.after);
  if (filters.before && isDateFilter(filters.before))
    params.set("before", filters.before);

  return params;
}
