import { describe, expect, it } from "vitest";
import { filterParams, parseFilters } from "./navigation";
import { defaultFilters } from "./storage";
import type { Filters } from "./types";

describe("filter URLs", () => {
  it("round-trips every rule, including repeated selections and punctuation", () => {
    const filters: Filters = {
      q: "politics & science + future?",
      mode: "meaning",
      sort: "relevance",
      domains: ["example.com", "news.example.org"],
      tags: ["politics", "science & technology"],
      author: "Ada / @writer",
      after: "2026-09-01",
      before: "2026-10-08",
    };

    const query = filterParams(filters).toString();
    expect(parseFilters(new URLSearchParams(query))).toEqual(filters);
    expect(new URLSearchParams(query).getAll("tag")).toEqual(filters.tags);
    expect(new URLSearchParams(query).getAll("domain")).toEqual(
      filters.domains,
    );
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

  it("requires a query for relevance but allows chronological meaning search", () => {
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
    ).toBe("newest");
  });
});
