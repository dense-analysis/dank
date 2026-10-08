import { defaultFilters } from "./storage";
import type { Filters } from "./types";
import { isDateFilter } from "./validation";

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
    mode: params.get("mode") === "meaning" ? "meaning" : "words",
    sort: sort === "oldest" || (sort === "relevance" && q) ? sort : "newest",
    domains: selections(params, "domain"),
    tags: selections(params, "tag"),
    author: (params.get("author") ?? "").trim(),
    after: isDateFilter(after) ? after : "",
    before: isDateFilter(before) ? before : "",
  };
}

export function filterParams(filters: Filters): URLSearchParams {
  const params = new URLSearchParams();
  const q = filters.q.trim();
  if (q) params.set("q", q);
  if (filters.mode !== "words") params.set("mode", filters.mode);
  if (filters.sort !== "newest" && (filters.sort !== "relevance" || q)) {
    params.set("sort", filters.sort);
  }

  for (const domain of new Set(filters.domains)) {
    if (domain.trim()) params.append("domain", domain.trim());
  }

  for (const tag of new Set(filters.tags)) {
    if (tag.trim()) params.append("tag", tag.trim());
  }

  if (filters.author.trim()) params.set("author", filters.author.trim());
  if (filters.after && isDateFilter(filters.after))
    params.set("after", filters.after);
  if (filters.before && isDateFilter(filters.before))
    params.set("before", filters.before);

  return params;
}
