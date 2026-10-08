import { describe, expect, it } from "vitest";
import { filterParams, parseFilters } from "./navigation";
import { defaultFilters } from "./storage";
import type { Filters } from "./types";

describe("filter URLs", () => {
  it("round-trips every rule, including repeated selections and punctuation", () => {
    const filters: Filters = {
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
