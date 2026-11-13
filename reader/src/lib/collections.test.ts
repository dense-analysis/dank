import { expect, it } from "vitest";
import { feedSource, redditSource, xAccount, xSource } from "./api.fixture";
import { collectedAccounts, sourcesForFeed } from "./collections";
import type { Source } from "./types";

const x = xSource({
  name: "X",
  count: 3,
  accounts: [
    xAccount({
      handle: "Reporter",
      name: "City Reporter",
      count: 3,
      total_posts: 10161,
      icon_url: "https://pbs.twimg.com/profile_images/1/a.jpg",
    }),
  ],
});
const sources: Source[] = [
  feedSource({ domain: "example.com", name: "Blog", count: 5 }),
  redditSource({ name: "Reddit", count: 2 }),
  x,
  xSource({
    domain: "twitter.com",
    name: "Twitter",
    count: 2,
    accounts: [
      xAccount({
        handle: "reporter",
        name: "reporter",
        count: 2,
        icon_url: null,
        total_posts: 10000,
      }),
    ],
  }),
];

it("merges historical X domains into one account with the combined post count", () => {
  expect(collectedAccounts(sources)).toEqual([
    {
      handle: "reporter",
      name: "City Reporter",
      count: 5,
      total_posts: 10161,
      icon_url: "https://pbs.twimg.com/profile_images/1/a.jpg",
    },
  ]);
  expect(x.accounts[0].count).toBe(3);
  expect(collectedAccounts([...sources].reverse())[0].total_posts).toBe(10161);
});

it("keeps source filter choices scoped to each collection", () => {
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

it("includes followed and discovered publishers in Blogs", () => {
  const followed: Source = feedSource({
    domain: "followed.test",
    name: "Followed",
    kind: "rss",
    configured: true,
    tags: [],
    count: 2,
  });
  const discovered: Source = feedSource({
    domain: "discovered.test",
    name: "Discovered",
    kind: "rss",
    configured: false,
    tags: [],
    count: 3,
  });
  expect(sourcesForFeed([followed, discovered], "stories")).toEqual([
    followed,
    discovered,
  ]);
  expect(sourcesForFeed([followed, discovered], "all")).toEqual([
    followed,
    discovered,
  ]);
});
