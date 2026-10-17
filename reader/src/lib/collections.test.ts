import { expect, it } from "vitest";
import { collectedAccounts, sourcesForFeed } from "./collections";
import type { Source } from "./types";

const sources: Source[] = [
  { domain: "example.com", kind: "rss", name: "Blog", tags: [], count: 5 },
  { domain: "reddit.com", kind: "reddit", name: "Reddit", tags: [], count: 2 },
  {
    domain: "x.com",
    kind: "x",
    name: "X",
    tags: [],
    count: 3,
    accounts: [
      {
        handle: "Reporter",
        name: "City Reporter",
        count: 3,
        icon_url: "https://pbs.twimg.com/profile_images/1/a.jpg",
      },
    ],
  },
  {
    domain: "twitter.com",
    name: "Twitter",
    tags: [],
    count: 2,
    accounts: [
      { handle: "reporter", name: "reporter", count: 2, icon_url: null },
    ],
  },
];

it("merges historical X domains into one account with the combined post count", () => {
  expect(collectedAccounts(sources)).toEqual([
    {
      handle: "reporter",
      name: "City Reporter",
      count: 5,
      icon_url: "https://pbs.twimg.com/profile_images/1/a.jpg",
    },
  ]);
  expect(sources[2].accounts?.[0].count).toBe(3);
});

it("keeps source filter choices scoped to each collection, including older source responses", () => {
  expect(sourcesForFeed(sources, "all")).toEqual(sources);
  expect(sourcesForFeed(sources, "stories").map((s) => s.domain)).toEqual([
    "example.com",
  ]);
  expect(sourcesForFeed(sources, "x").map((s) => s.domain)).toEqual([
    "x.com",
    "twitter.com",
  ]);
  expect(sourcesForFeed(sources, "reddit").map((s) => s.domain)).toEqual([
    "reddit.com",
  ]);
});
