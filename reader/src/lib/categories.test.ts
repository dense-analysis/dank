import { describe, expect, it } from "vitest";
import { postParams } from "./api";
import { filterParams, parseFilters } from "./navigation";

describe("category filters", () => {
  it("preserves exact names alongside platform and search through URLs and API", () => {
    const filters = parseFilters(
      new URLSearchParams(
        "category=Software&category=Science&category=Software&platform=x&q=tools",
      ),
    );
    expect(filters.categories).toEqual(["Software", "Science"]);
    expect(parseFilters(filterParams(filters))).toEqual(filters);
    expect(postParams(filters).getAll("category")).toEqual([
      "Software",
      "Science",
    ]);
    expect(postParams(filters).getAll("platform")).toEqual(["x"]);
  });
});
