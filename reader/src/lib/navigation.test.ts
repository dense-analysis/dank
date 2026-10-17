import { describe, expect, it } from "vitest";
import { articleParams, filterParams, parseFilters } from "./navigation";
import { defaultFilters } from "./storage";
import type { Filters } from "./types";

describe("filter URLs", () => {
  it.each(["all", "stories", "x", "reddit"] as const)(
    "round-trips the %s collection",
    (kind) => {
      const filters = { ...defaultFilters, kind };
      expect(parseFilters(filterParams(filters))).toEqual(filters);
    },
  );

  it("keeps account identity independent of author and removes it outside X", () => {
    const filters = parseFilters(
      new URLSearchParams(
        "view=x&account=%40Reporter&author=Ada&author_match=exact&q=local&sort=oldest",
      ),
    );
    expect(filters).toMatchObject({
      kind: "x",
      account: "reporter",
      author: "Ada",
      authorExact: true,
      q: "local",
      sort: "oldest",
    });
    expect(parseFilters(filterParams(filters))).toEqual(filters);
    expect(
      parseFilters(new URLSearchParams("view=reddit&account=reporter")).account,
    ).toBe("");
    expect(
      filterParams({ ...defaultFilters, account: "reporter" }).has("account"),
    ).toBe(false);
  });

  it("preserves Reddit community filters and discards orphaned community selections", () => {
    const filters = {
      ...defaultFilters,
      kind: "reddit" as const,
      community: "LocalLLaMA",
      flairs: ["New Model", "Research & News / 更新"],
      q: "local models",
      sort: "relevance" as const,
    };
    expect(parseFilters(filterParams(filters))).toEqual(filters);
    expect(
      parseFilters(new URLSearchParams("community=LocalLLaMA&flair=New+Model")),
    ).toEqual(defaultFilters);
    expect(
      filterParams({
        ...defaultFilters,
        community: "LocalLLaMA",
        flairs: ["New Model"],
      }).has("community"),
    ).toBe(false);
  });
  it("deduplicates flair selections and excludes them outside Reddit", () => {
    expect(
      parseFilters(
        new URLSearchParams(
          "view=reddit&flair=New+Model&flair=New+Model&flair=+&flair=Discussion",
        ),
      ).flairs,
    ).toEqual(["New Model", "Discussion"]);
    expect(
      filterParams({ ...defaultFilters, flairs: ["New Model"] }).has("flair"),
    ).toBe(false);
  });

  it("round-trips every rule, including repeated selections and punctuation", () => {
    const filters: Filters = {
      ...defaultFilters,
      q: "politics & science + future?",
      sort: "relevance",
      domains: ["example.com", "news.example.org"],
      tags: ["politics", "science & technology"],
      author: "Ada / @writer",
      authorExact: true,
      after: "2026-09-01",
      before: "2026-10-08",
    };

    const query = filterParams(filters).toString();
    expect(parseFilters(new URLSearchParams(query))).toEqual(filters);
    expect(new URLSearchParams(query).getAll("tag")).toEqual(filters.tags);
    expect(new URLSearchParams(query).getAll("domain")).toEqual(
      filters.domains,
    );
    expect(new URLSearchParams(query).get("author_match")).toBe("exact");
  });

  it("keeps typed author filters partial and removes orphaned exact-match flags", () => {
    expect(parseFilters(new URLSearchParams("author=Ada")).authorExact).toBe(
      false,
    );
    expect(
      parseFilters(new URLSearchParams("author_match=exact")).authorExact,
    ).toBe(false);
    expect(
      filterParams({ ...defaultFilters, authorExact: true }).has(
        "author_match",
      ),
    ).toBe(false);
  });

  it("defaults invalid settings and removes empty or duplicate selections", () => {
    expect(
      parseFilters(
        new URLSearchParams(
          "mode=invalid&sort=random&domain=example.com&domain=example.com&domain=+&tag=+&after=2026-02-30&before=tomorrow",
        ),
      ),
    ).toEqual({ ...defaultFilters, domains: ["example.com"] });
  });

  it("defaults searches to relevance and preserves explicit chronological order", () => {
    expect(parseFilters(new URLSearchParams("sort=relevance&q=+")).sort).toBe(
      "newest",
    );
    expect(
      filterParams({ ...defaultFilters, sort: "relevance" }).has("sort"),
    ).toBe(false);
    expect(
      parseFilters(new URLSearchParams("mode=meaning&q=politics&sort=oldest"))
        .sort,
    ).toBe("oldest");
    expect(
      parseFilters(new URLSearchParams("mode=meaning&q=politics")).sort,
    ).toBe("relevance");
    expect(
      parseFilters(new URLSearchParams("q=politics&sort=newest")).sort,
    ).toBe("newest");
    expect(
      filterParams({ ...defaultFilters, q: "politics", sort: "newest" }).get(
        "sort",
      ),
    ).toBe("newest");
  });

  it.each(["words", "meaning"])(
    "treats old %s links as unified search without retaining a hidden mode",
    (mode) => {
      const filters = parseFilters(
        new URLSearchParams(
          `q=politics&mode=${mode}&domain=example.com&sort=oldest`,
        ),
      );
      expect(filters).toEqual({
        ...defaultFilters,
        q: "politics",
        domains: ["example.com"],
        sort: "oldest",
      });
      expect(filterParams(filters).has("mode")).toBe(false);
    },
  );
});

it("normal article navigation clears every reference target without losing graph context", () => {
  const params = new URLSearchParams({
    connections: "graph",
    article: "previous",
    article_source: "source.test",
    reference_kind: "comment",
    reference_id: "123",
    reference_url: "https://target.test/post",
    reference_video: "NnV_cWeoo5Q",
    reference_saved: "2026-10-17T12:00:00Z",
  });
  const next = articleParams(params, { domain: "new.test", id: "next" });
  expect([...next.keys()].some((key) => key.startsWith("reference_"))).toBe(
    false,
  );
  expect(next.get("connections")).toBe("graph");
  expect(params.get("reference_id")).toBe("123");
});
