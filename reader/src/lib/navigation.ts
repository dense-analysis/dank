import { sectionFromView } from "./collections";
import { parseHackerNewsFilter } from "./hackerNews";
import { defaultFilters } from "./storage";
import type { FeedPlatform, Filters, Post } from "./types";
import { isDateFilter } from "./validation";

export function clearReferenceParams(params: URLSearchParams) {
  for (const key of [
    "reference_kind",
    "reference_id",
    "reference_video",
    "reference_url",
    "reference_saved",
  ])
    params.delete(key);
}

export function clearConnectionParams(params: URLSearchParams) {
  for (const key of [
    "connections",
    "connections_direction",
    "connections_from",
  ])
    params.delete(key);
}

export function articleParams(
  params: URLSearchParams,
  post: Pick<Post, "id" | "domain">,
): URLSearchParams {
  const next = new URLSearchParams(params);
  clearReferenceParams(next);
  clearConnectionParams(next);
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
  const section = sectionFromView(params.get("view"));
  const kind =
    section === "sources" || section === "settings" || section === "youtube"
      ? "all"
      : section;

  return {
    ...defaultFilters,
    kind,
    blogFeed:
      kind === "stories" &&
      ["sources", "external"].includes(params.get("blog_feed") ?? "")
        ? (params.get("blog_feed") as "sources" | "external")
        : "",
    platforms:
      kind === "all" && section === "all"
        ? selections(params, "platform").filter(
            (value): value is FeedPlatform =>
              ["x", "reddit", "hacker_news", "youtube", "stories"].includes(
                value,
              ),
          )
        : [],
    xView:
      kind === "x" && params.get("x_view") === "replies" ? "replies" : "posts",
    hnFilter:
      kind === "hacker_news"
        ? parseHackerNewsFilter(params.get("hn_filter"))
        : "top50",
    community:
      params.get("view") === "reddit"
        ? (params.get("community") ?? "").trim()
        : "",
    account:
      kind === "x"
        ? (params.get("account") ?? "").trim().replace(/^@/, "").toLowerCase()
        : "",
    q,
    sort:
      sort === "oldest" || sort === "newest"
        ? sort
        : q
          ? "relevance"
          : "newest",
    domains: selections(params, "domain"),
    tags: selections(params, "tag"),
    flairs: params.get("view") === "reddit" ? selections(params, "flair") : [],
    author: (params.get("author") ?? "").trim(),
    authorExact:
      !!params.get("author")?.trim() && params.get("author_match") === "exact",
    after: isDateFilter(after) ? after : "",
    before: isDateFilter(before) ? before : "",
  };
}

export function filterParams(filters: Filters): URLSearchParams {
  const params = new URLSearchParams();
  if (filters.kind === "all") {
    for (const platform of new Set(filters.platforms)) {
      params.append("platform", platform);
    }
  }
  if (filters.kind !== "all") params.set("view", filters.kind);
  if (filters.kind === "stories" && filters.blogFeed)
    params.set("blog_feed", filters.blogFeed);
  if (filters.kind === "x" && filters.xView === "replies")
    params.set("x_view", "replies");
  if (filters.kind === "hacker_news") params.set("hn_filter", filters.hnFilter);
  if (filters.kind === "x" && filters.account.trim()) {
    params.set(
      "account",
      filters.account.trim().replace(/^@/, "").toLowerCase(),
    );
  }
  if (filters.kind === "reddit") {
    if (filters.community.trim())
      params.set("community", filters.community.trim());

    for (const flair of new Set(filters.flairs)) {
      if (flair.trim()) params.append("flair", flair.trim());
    }
  }
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
