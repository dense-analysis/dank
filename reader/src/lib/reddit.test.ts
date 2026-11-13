import { describe, expect, it } from "vitest";
import { redditCommunity, redditSource } from "./api.fixture";
import { availableFlairs } from "./reddit";
import type { Source } from "./types";

const sources: Source[] = [
  redditSource({
    name: "Reddit",
    count: 4,
    flairs: [
      { name: "Discussion", count: 2 },
      { name: "New Model", count: 1 },
      { name: "Research", count: 1 },
    ],
    communities: [
      redditCommunity({
        name: "LocalLLaMA",
        count: 2,
        flairs: [
          { name: "New Model", count: 1 },
          { name: "Discussion", count: 1 },
        ],
      }),
      redditCommunity({
        name: "accelerate",
        count: 2,
        flairs: [
          { name: "Research", count: 1 },
          { name: "Discussion", count: 1 },
        ],
      }),
      redditCommunity({ name: "Empty", count: 0 }),
    ],
  }),
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

  it("does not leak global flairs into empty or unknown communities or other sources", () => {
    expect(availableFlairs(sources, "Empty")).toEqual([]);
    expect(availableFlairs(sources, "Unknown")).toEqual([]);
    expect(availableFlairs([{ ...sources[0], kind: "rss" }], "")).toEqual([]);
  });
});
