import { describe, expect, it } from "vitest";
import { availableFlairs } from "./reddit";
import type { Source } from "./types";

const sources: Source[] = [
  {
    domain: "reddit.com",
    name: "Reddit",
    kind: "reddit",
    count: 4,
    tags: [],
    flairs: [
      { name: "Discussion", count: 2 },
      { name: "New Model", count: 1 },
      { name: "Research", count: 1 },
    ],
    communities: [
      {
        name: "LocalLLaMA",
        count: 2,
        flairs: [
          { name: "New Model", count: 1 },
          { name: "Discussion", count: 1 },
        ],
      },
      {
        name: "accelerate",
        count: 2,
        flairs: [
          { name: "Research", count: 1 },
          { name: "Discussion", count: 1 },
        ],
      },
      { name: "Empty", count: 0 },
    ],
  },
];

describe("collected Reddit flairs", () => {
  it("uses global counts for shared labels and isolates community options", () => {
    expect(availableFlairs(sources, "")).toEqual([
      { name: "Discussion", count: 2 },
      { name: "New Model", count: 1 },
      { name: "Research", count: 1 },
    ]);
    expect(availableFlairs(sources, "localllama")).toEqual([
      { name: "Discussion", count: 1 },
      { name: "New Model", count: 1 },
    ]);
    expect(availableFlairs(sources, "accelerate")).toEqual([
      { name: "Discussion", count: 1 },
      { name: "Research", count: 1 },
    ]);
  });

  it("does not leak global flairs into empty, unknown or older communities", () => {
    expect(availableFlairs(sources, "Empty")).toEqual([]);
    expect(availableFlairs(sources, "Unknown")).toEqual([]);
    expect(availableFlairs([{ ...sources[0], flairs: undefined }], "")).toEqual(
      [],
    );
    expect(availableFlairs([{ ...sources[0], kind: "rss" }], "")).toEqual([]);
  });
});
