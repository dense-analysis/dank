import { describe, expect, it } from "vitest";
import { highlightMatches } from "./highlightMatches";

describe("search term highlighting", () => {
  it("treats punctuation as literal text and keeps the original casing and content", () => {
    const text = "C++ works with [API] and c++. <script> is just text.";
    const parts = highlightMatches(text, "c++ [API]");
    expect(parts.map((part) => part.text).join("")).toBe(text);
    expect(
      parts.filter((part) => part.highlighted).map((part) => part.text),
    ).toEqual(["C++", "[API]", "c++"]);
  });
  it("keeps semantic-only matches and empty searches unmarked", () => {
    for (const query of ["", " \t", "architectural boundaries"]) {
      expect(highlightMatches("Domain modelling", query)).toEqual([
        { text: "Domain modelling", highlighted: false, start: 0 },
      ]);
    }
  });
});
